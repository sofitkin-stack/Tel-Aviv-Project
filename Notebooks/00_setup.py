# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup
# MAGIC Shared definitions for the whole pipeline. Every other notebook starts with `%run ./00_setup`.
# MAGIC
# MAGIC Defines: catalog and schema names, `run_id`, `normalize_street`, the compensation rule (`daily_compensation`),
# MAGIC `log_check`, and the write helpers `append_rows` (Bronze), `latest_per_key`, `upsert_rows`, `last_merge_metrics` (Silver).
# MAGIC The functions have no side effects except the writes they name, so `tests/` runs them on a local Spark.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# ---- names ----
CATALOG = "tlv"          # if creating a catalog is not allowed in your workspace, change to "workspace"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# ---- source urls ----
CLOSURES_CSV_URL = "https://storage.googleapis.com/test_onedatai/rechov_sagur.csv"
BUSINESSES_API_URL = "https://gisn.tel-aviv.gov.il/arcgis/rest/services/IView2/MapServer/925/query"
ADDRESSES_API_URL = "https://gisn.tel-aviv.gov.il/arcgis/rest/services/IView2/MapServer/527/query"   # city addresses: street code + name + house number + x/y

# ---- one id per pipeline run (all notebooks in the same run share it) ----
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# COMMAND ----------

# ---- create catalog and schemas (safe to run again) ----
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

# COMMAND ----------

def normalize_street(col):
    """
    Turn a street name column into a matching key.
    Same function is used on closures and on businesses, so both sides match.
    Steps: remove quote marks, drop a leading 'רח'/'רחוב', collapse spaces, trim.
    """
    c = F.regexp_replace(col, "[\"'״׳`]", "")          # remove all kinds of quote marks
    c = F.regexp_replace(c, "^\\s*(רחוב|רח)\\s+", "")   # drop a leading 'רחוב ' or 'רח '
    c = F.regexp_replace(c, "\\s+", " ")                 # many spaces -> one space
    c = F.trim(c)
    return c

# COMMAND ----------

# ---- compensation rule (assignment): 100 ILS per area unit per closure day, at most 10,000 ILS per business per day ----
RATE_PER_AREA = 100
DAILY_CAP = 10000


def daily_compensation(area_col):
    """Compensation of one business for one closure day: area * RATE_PER_AREA, capped at DAILY_CAP. SQL: LEAST(area * 100, 10000)."""
    return F.least(area_col * RATE_PER_AREA, F.lit(DAILY_CAP))

# COMMAND ----------

def log_check(check_name, value, passed, layer):
    """
    Append one row to gold.dq_results: which check, what value, did it pass.
    Zero or 'passed = True' means good.
    """
    row = spark.createDataFrame(
        [(run_id, layer, check_name, float(value), bool(passed), datetime.now())],
        "run_id string, layer string, check_name string, value double, passed boolean, checked_at timestamp",
    )
    row.write.mode("append").saveAsTable(f"{GOLD}.dq_results")

# COMMAND ----------

def last_merge_metrics(table_name):
    """Row counts of the last write to a Delta table: inserted, updated, deleted."""
    m = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]["operationMetrics"]
    return {
        "inserted": int(m.get("numTargetRowsInserted", m.get("numOutputRows", 0))),
        "updated": int(m.get("numTargetRowsUpdated", 0)),
        "deleted": int(m.get("numTargetRowsDeleted", 0)),
    }


METADATA_COLS = ("run_id", "ingested_at", "source", "bronze_ingested_at", "silver_run_id", "processed_at")


def append_rows(df, table_name):
    """Bronze write: append the DataFrame as a new snapshot. Returns the number of rows appended."""
    df.write.mode("append").saveAsTable(table_name)
    return df.count()


def latest_per_key(df, key_cols, order_col="ingested_at"):
    """One row per key: the one with the newest order_col (Bronze keeps every snapshot, Silver uses the latest)."""
    newest_first = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(newest_first)).filter("_rn = 1").drop("_rn")


def key_condition(key_cols):
    """MERGE match: every key column equal. Example: t.`id_rechov` = s.`id_rechov` AND t.`side` = s.`side`"""
    return " AND ".join([f"t.`{k}` = s.`{k}`" for k in key_cols])


def changed_condition(df, key_cols):
    """True when at least one data column differs (null-safe). Key and metadata columns are not compared."""
    data_cols = [c for c in df.columns if c not in METADATA_COLS and c not in key_cols]
    return " OR ".join([f"NOT (t.`{c}` <=> s.`{c}`)" for c in data_cols])


def upsert_rows(df, table_name, key_cols):
    """
    Incremental load with Delta MERGE by key. Returns (inserted, updated).
      key not in the table          -> insert
      key exists, data changed      -> update
      key exists, data identical    -> nothing
    """
    if not spark.catalog.tableExists(table_name):      # first run: the table does not exist yet
        df.write.saveAsTable(table_name)
        return df.count(), 0
    if df.isEmpty():                                   # nothing new to merge
        return 0, 0

    (DeltaTable.forName(spark, table_name).alias("t")
        .merge(df.alias("s"), key_condition(key_cols))
        .withSchemaEvolution()                          # a new column in df is added to the table
        .whenMatchedUpdateAll(condition=changed_condition(df, key_cols))
        .whenNotMatchedInsertAll()
        .execute())
    m = last_merge_metrics(table_name)
    return m["inserted"], m["updated"]

# COMMAND ----------

# ---- self test (runs only when this notebook is run directly) ----
test = spark.createDataFrame(
    [(" רח' של״ג ",), ("רחוב  דיזנגוף",), ('המלך ג\'ורג\'',), ("קרן היסוד",)],
    "street string",
)
display(test.withColumn("street_key", normalize_street(F.col("street"))))
print("run_id:", run_id)
print("schemas:", [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()])

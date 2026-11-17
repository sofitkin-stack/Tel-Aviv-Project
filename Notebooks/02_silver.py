# Databricks notebook source
# MAGIC %md
# MAGIC # 02_silver
# MAGIC Clean, typed data: one validated record per source record.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC 1. Reads from Bronze only the rows that landed since the last Silver run (watermark).
# MAGIC 2. Keeps the latest snapshot of every record, cleans it and converts the types.
# MAGIC 3. Merges the result into the Silver table by key: new key inserted, changed record updated, identical record left untouched.
# MAGIC 4. Rebuilds the business-to-street lookup.
# MAGIC 5. Writes every check to `gold.dq_results`.
# MAGIC
# MAGIC ## The watermark
# MAGIC Every Silver row keeps `bronze_ingested_at`, the time its Bronze row landed.
# MAGIC A run reads only Bronze rows whose `ingested_at` is later than the highest `bronze_ingested_at` already in Silver.
# MAGIC On the first run Silver is empty, so everything is read.
# MAGIC
# MAGIC ## The tables
# MAGIC
# MAGIC | table | one row is | key | notes |
# MAGIC |---|---|---|---|
# MAGIC | `silver.closures` | one side (from / to) of one closure event | `id_rechov`, `side` | `reject_reason` says why a row is not used (NULL = valid) |
# MAGIC | `silver.businesses` | one business, its latest record | `business_id` | latest `date_import` wins, `objectid` breaks ties; `reject_reason` as above |
# MAGIC | `silver.addresses` | one city address (layer 527) | `address_id` | street code, street name, house number, x/y |
# MAGIC | `silver.business_street` | one business and its street code | `business_id` | rebuilt every run from businesses and addresses; `match_method` = by_name / by_point / unmatched |
# MAGIC
# MAGIC `id_rechov` identifies the closure event (one closed segment and date range), not the street.
# MAGIC The CSV holds a few events twice under different `ID`s; keying Silver by event keeps each once, and the count goes to `dq_results`.
# MAGIC
# MAGIC ## The balance check
# MAGIC Closure rows in Silver = 2 x distinct closure events in Bronze. If the difference is not 0, a side was lost.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared definitions
# MAGIC `00_setup` is loaded first because this notebook uses what it defines:
# MAGIC - the schema names `BRONZE` (read), `SILVER` (write) and `GOLD` (for `dq_results`),
# MAGIC - `run_id`, written as `silver_run_id` on every Silver row,
# MAGIC - `latest_per_key` (newest Bronze snapshot per key), `upsert_rows` (the Delta MERGE by key) and `last_merge_metrics` (inserted / updated counts),
# MAGIC - `normalize_street`, the one function that turns a street name into a matching key on both the business and the address side,
# MAGIC - `log_check`, which writes every check to `gold.dq_results`,
# MAGIC - the imports `F`, `DeltaTable`, `Window`, `datetime`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------


def bronze_rows_to_process(bronze_table, silver_table):
    """
    Bronze rows not yet processed: ingested_at later than the highest bronze_ingested_at in the Silver table.
    First run (Silver table does not exist yet): all of Bronze.
    """
    bronze = spark.table(bronze_table)
    if not spark.catalog.tableExists(silver_table):
        print(f"{bronze_table}: first run, processing all rows")
        return bronze
    watermark = spark.table(silver_table).agg(F.max("bronze_ingested_at")).collect()[0][0]
    print(f"{bronze_table}: processing rows ingested after {watermark}")
    return bronze.filter(F.col("ingested_at") > F.lit(watermark))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Closures
# MAGIC Goal: turn each CSV line into two typed rows, one per closed street (from / to), with real dates, a street code, the pedestrian flag and a reject reason, and merge them into `silver.closures`.
# MAGIC
# MAGIC Transformation steps, in the order the code runs them:
# MAGIC 1. **Read**: Bronze rows newer than the watermark; if a CSV line arrived in several snapshots, keep its newest copy (`latest_per_key` on `ID`).
# MAGIC 2. **Type conversion**: `tr_from` / `tr_to` text -> `start_date` / `end_date`.
# MAGIC 3. **Fix values**: missing end date -> start date; end date before start date -> start date (counted).
# MAGIC 4. **Reshape**: one line -> two rows, `side = from` (me_* columns) and `side = to` (ad_* columns); `street_code` cast to a number.
# MAGIC 5. **Computed columns**: `is_pedestrian_street` (the word "מדרחוב" in `shaot` / `sw_laila`), `silver_run_id`, `processed_at`.
# MAGIC 6. **Mark invalid rows**: `reject_reason` = `start_date_unparseable` or `missing_street_code` (code 0 / null); the rows stay, Gold skips them.
# MAGIC 7. **Remove duplicates**: the same event (`id_rechov`, `side`) under two different `ID`s -> keep the lowest `ID` (counted).
# MAGIC 8. **Merge** into `silver.closures` by (`id_rechov`, `side`): insert new, update changed, leave identical.
# MAGIC 9. **Checks** to `dq_results`: balance (Silver rows = 2 x Bronze events), no duplicate key, counts of rejected / fixed / pedestrian rows.

# COMMAND ----------

closures_table = f"{SILVER}.closures"
bronze_closures = latest_per_key(bronze_rows_to_process(f"{BRONZE}.closures", closures_table), ["ID"])
closures_to_process = bronze_closures.count()

# dates: text "dd/MM/yyyy H:mm" -> date. Missing end date -> start date.
closures = (
    bronze_closures
    .withColumn("start_date", F.to_date(F.try_to_timestamp(F.col("tr_from"), F.lit("d/M/yyyy H:mm"))))
    .withColumn("end_date", F.to_date(F.try_to_timestamp(F.col("tr_to"), F.lit("d/M/yyyy H:mm"))))
    .withColumn("end_date", F.coalesce(F.col("end_date"), F.col("start_date")))
)

# end before start: use start date, and count how many rows were fixed
end_before_start = closures.filter(F.col("end_date") < F.col("start_date")).count()
closures = closures.withColumn(
    "end_date", F.when(F.col("end_date") < F.col("start_date"), F.col("start_date")).otherwise(F.col("end_date"))
)

# COMMAND ----------

# one row per street: the "from" street (me_*) and the "to" street (ad_*)
common_cols = ["ID", "id_rechov", "k_sug", "t_sug", "k_sgira", "shaot", "sw_laila", "start_date", "end_date",
               F.col("ingested_at").alias("bronze_ingested_at")]

from_side = closures.select(
    *common_cols,
    F.lit("from").alias("side"),
    F.col("me_k_rechov").alias("street_code"),
    F.col("me_shem_rechov").alias("street_name"),
)
to_side = closures.select(
    *common_cols,
    F.lit("to").alias("side"),
    F.col("ad_k_rechov").alias("street_code"),
    F.col("ad_shem_rechov").alias("street_name"),
)

closures_split = (
    from_side.unionByName(to_side)
    .withColumnRenamed("ID", "closure_id")
    .withColumn("silver_run_id", F.lit(run_id))
    .withColumn("processed_at", F.current_timestamp())
)

# street_code as a number; in this file a missing street appears as code 0 (and name "0")
closures_split = closures_split.withColumn("street_code", F.col("street_code").cast("long"))

# a permanent pedestrian street ("מדרחוב" in the free-text columns shaot / sw_laila) is flagged; Gold excludes it from compensation
closures_split = closures_split.withColumn(
    "is_pedestrian_street",
    F.concat_ws(" ", F.col("shaot"), F.col("sw_laila")).contains("מדרחוב"),
)

# reject reasons. Matching to businesses is by street code, so a row without a code cannot be used.
closures_split = closures_split.withColumn(
    "reject_reason",
    F.when(F.col("start_date").isNull(), "start_date_unparseable")
     .when(F.col("street_code").isNull() | (F.col("street_code") == 0), "missing_street_code")
)
# the same closure event can appear twice in the file under different IDs: keep one row per (id_rechov, side), lowest ID
one_per_event = Window.partitionBy("id_rechov", "side").orderBy(F.col("closure_id").cast("long"))
closures_split = closures_split.withColumn("rn", F.row_number().over(one_per_event))
duplicate_events_removed = closures_split.filter("rn > 1").count()
closures_split = closures_split.filter("rn = 1").drop("rn")

# COMMAND ----------

# merge by key (closure event x side). Valid and rejected rows live in the same table; reject_reason tells them apart.
closure_key = ["id_rechov", "side"]
clo_ins, clo_upd = upsert_rows(closures_split, closures_table, closure_key)

# COMMAND ----------

bronze_events_total = spark.table(f"{BRONZE}.closures").select("id_rechov").distinct().count()
silver_closures = spark.table(f"{SILVER}.closures")
silver_closures_total = silver_closures.count()
closures_rejected = silver_closures.filter("reject_reason IS NOT NULL").count()
closure_dups = silver_closures.groupBy(*closure_key).count().filter("count > 1").count()

log_check("closures_bronze_rows_processed", closures_to_process, True, "silver")
log_check("closures_silver_inserted", clo_ins, True, "silver")
log_check("closures_silver_updated", clo_upd, True, "silver")
log_check("closures_rejected_total", closures_rejected, True, "silver")
log_check("closures_end_before_start_fixed", end_before_start, True, "silver")
log_check("closures_duplicate_events_removed", duplicate_events_removed, True, "silver")
log_check("closures_pedestrian_street_rows", closures_split.filter("is_pedestrian_street").count(), True, "silver")
log_check("closures_balance", silver_closures_total - 2 * bronze_events_total, silver_closures_total == 2 * bronze_events_total, "silver")
log_check("closures_duplicates_on_key", closure_dups, closure_dups == 0, "silver")

print(f"closures: processed {closures_to_process} bronze rows -> silver +{clo_ins} ~{clo_upd}")
print(f"closures totals: bronze events {bronze_events_total}, silver rows {silver_closures_total} (rejected {closures_rejected}), duplicate events removed in batch {duplicate_events_removed}")
display(silver_closures.groupBy("reject_reason").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Businesses
# MAGIC Goal: flatten the JSON feature into columns, convert area and dates to numbers and timestamps, keep the latest record per business id, mark invalid rows, and merge into `silver.businesses`.
# MAGIC
# MAGIC Transformation steps, in the order the code runs them:
# MAGIC 1. **Read**: Bronze rows newer than the watermark; if a feature arrived in several snapshots, keep its newest copy (`latest_per_key` on `objectid`).
# MAGIC 2. **Flatten**: the JSON text -> one column per field we use (`business_id`, `street_name`, `house_number`, `owner_name`, area, dates, `x`, `y`).
# MAGIC 3. **Type conversion**: `shetach` text -> `area` (double); `tr_hakama` milliseconds -> `founded_at` (timestamp); `date_import` text -> timestamp.
# MAGIC 4. **Computed columns**: `street_key` (normalized street name, used by section 4), `silver_run_id`, `processed_at`.
# MAGIC 5. **Drop rows without a key**: a row with no `business_id` cannot be merged; counted in `dq_results`, then removed.
# MAGIC 6. **Mark invalid rows**: `reject_reason` = `invalid_area` (null or negative); the row stays, Gold skips it.
# MAGIC 7. **Remove duplicates**: one row per `business_id` inside the batch, the latest `date_import` wins, `objectid` breaks ties (counted).
# MAGIC 8. **Merge** into `silver.businesses` by `business_id`: insert new, update changed, leave identical.
# MAGIC 9. **Checks** to `dq_results`: no duplicate id, rejected count, rows without a street name.

# COMMAND ----------

business_table = f"{SILVER}.businesses"
bronze_businesses = latest_per_key(bronze_rows_to_process(f"{BRONZE}.businesses_raw", business_table), ["objectid"])
businesses_to_process = bronze_businesses.count()

# the JSON fields we use (other fields in the feature are ignored)
feature_schema = """
    attributes struct<
        OBJECTID long, id_esek long, shimush string, shem_rechov string, ms_bayit string,
        tr_hakama long, sw_taun_rishui int, shetach string,
        shem_machzik_rashi string, ms_mezahe_machzik_rashi string, date_import string
    >,
    geometry struct<x double, y double>
"""

businesses = (
    bronze_businesses
    .withColumn("f", F.from_json(F.col("feature_json"), feature_schema))
    .select(
        F.col("f.attributes.OBJECTID").alias("objectid"),
        F.col("f.attributes.id_esek").alias("business_id"),
        F.col("f.attributes.shimush").alias("business_type"),
        F.col("f.attributes.shem_rechov").alias("street_name"),
        F.col("f.attributes.ms_bayit").alias("house_number"),
        F.col("f.attributes.shem_machzik_rashi").alias("owner_name"),
        F.col("f.attributes.ms_mezahe_machzik_rashi").alias("owner_id"),
        F.col("f.attributes.sw_taun_rishui").alias("requires_license"),
        F.col("f.attributes.shetach").alias("area_raw"),
        F.col("f.attributes.tr_hakama").alias("founded_ms"),
        F.col("f.attributes.date_import").alias("date_import_raw"),
        F.col("f.geometry.x").alias("x"),
        F.col("f.geometry.y").alias("y"),
        F.col("ingested_at").alias("bronze_ingested_at"),
    )
    # types: area -> double, founded (epoch milliseconds) -> timestamp, date_import (text) -> timestamp
    .withColumn("area", F.col("area_raw").cast("double"))
    .withColumn("founded_at", F.timestamp_seconds(F.col("founded_ms") / 1000))
    .withColumn("date_import", F.try_to_timestamp(F.col("date_import_raw"), F.lit("dd/MM/yyyy HH:mm:ss")))
    .withColumn("street_key", normalize_street(F.col("street_name")))
    .withColumn("silver_run_id", F.lit(run_id))
    .withColumn("processed_at", F.current_timestamp())
)

# a row without a business id has no key and cannot be merged: counted and dropped
no_id_rows = businesses.filter(F.col("business_id").isNull()).count()
businesses = businesses.filter(F.col("business_id").isNotNull())

# reject reason: the row stays in Silver, Gold skips it
businesses = businesses.withColumn(
    "reject_reason",
    F.when(F.col("area").isNull() | (F.col("area") < 0), "invalid_area")
)
businesses = businesses.drop("area_raw", "founded_ms", "date_import_raw")

# COMMAND ----------

# one row per business inside this batch: latest date_import wins, objectid breaks ties
latest_first = Window.partitionBy("business_id").orderBy(F.col("date_import").desc_nulls_last(), F.col("objectid").desc())
businesses = businesses.withColumn("rn", F.row_number().over(latest_first))
dedup_removed = businesses.filter("rn > 1").count()
businesses = businesses.filter("rn = 1").drop("rn")

# merge into silver.businesses by business_id. Valid and rejected rows live in the same table; reject_reason tells them apart.
biz_ins, biz_upd = upsert_rows(businesses, business_table, ["business_id"])

# COMMAND ----------

silver_businesses = spark.table(business_table)
silver_businesses_total = silver_businesses.count()
businesses_rejected = silver_businesses.filter("reject_reason IS NOT NULL").count()
without_street = silver_businesses.filter(F.col("street_key").isNull() | (F.col("street_key") == "")).count()
business_dups = silver_businesses.groupBy("business_id").count().filter("count > 1").count()

log_check("businesses_bronze_rows_processed", businesses_to_process, True, "silver")
log_check("businesses_without_id_dropped", no_id_rows, True, "silver")
log_check("businesses_batch_dedup_removed", dedup_removed, True, "silver")
log_check("businesses_silver_inserted", biz_ins, True, "silver")
log_check("businesses_silver_updated", biz_upd, True, "silver")
log_check("businesses_rejected_total", businesses_rejected, True, "silver")
log_check("businesses_silver_rows", silver_businesses_total, silver_businesses_total > 0, "silver")
log_check("businesses_duplicates_on_id", business_dups, business_dups == 0, "silver")
log_check("businesses_without_street", without_street, True, "silver")

print(f"businesses: processed {businesses_to_process} bronze rows -> silver +{biz_ins} ~{biz_upd}, rejected {businesses_rejected}, dedup in batch {dedup_removed}, no id {no_id_rows}")
display(silver_businesses.groupBy("reject_reason").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. City addresses: street code + street name + house number + x/y
# MAGIC Goal: flatten the address features into a typed lookup of street code, name, house number and coordinates, merged into `silver.addresses`.
# MAGIC
# MAGIC Transformation steps, in the order the code runs them:
# MAGIC 1. **Read**: Bronze rows newer than the watermark, newest copy per `objectid`.
# MAGIC 2. **Flatten + types**: JSON text -> `address_id`, `street_code` (long), `street_name`, `house_number`, `entrance`, `x`, `y` (doubles; geometry first, attribute columns as fallback).
# MAGIC 3. **Computed columns**: `street_key` (normalized name, same function as for businesses), `silver_run_id`, `processed_at`.
# MAGIC 4. **Filter invalid rows**: an address without a street code or without coordinates is useless as a lookup and is dropped (no reject reason here: this table is a helper, not a source record).
# MAGIC 5. **Merge** into `silver.addresses` by `address_id`.
# MAGIC 6. **Checks** to `dq_results`: processed / inserted / updated / total rows.

# COMMAND ----------

address_table = f"{SILVER}.addresses"
bronze_addresses = latest_per_key(bronze_rows_to_process(f"{BRONZE}.addresses_raw", address_table), ["objectid"])
addresses_to_process = bronze_addresses.count()

address_schema = """
    attributes struct<
        oid_ktovet long, k_rechov long, t_rechov string, ms_bayit long, knisa string, x double, y double
    >,
    geometry struct<x double, y double>
"""

addresses = (
    bronze_addresses
    .withColumn("f", F.from_json(F.col("feature_json"), address_schema))
    .select(
        F.col("objectid").alias("address_id"),
        F.col("f.attributes.k_rechov").alias("street_code"),
        F.col("f.attributes.t_rechov").alias("street_name"),
        F.col("f.attributes.ms_bayit").alias("house_number"),
        F.trim(F.col("f.attributes.knisa")).alias("entrance"),
        F.coalesce(F.col("f.geometry.x"), F.col("f.attributes.x")).alias("x"),
        F.coalesce(F.col("f.geometry.y"), F.col("f.attributes.y")).alias("y"),
        F.col("ingested_at").alias("bronze_ingested_at"),
    )
    .withColumn("street_key", normalize_street(F.col("street_name")))
    .withColumn("silver_run_id", F.lit(run_id))
    .withColumn("processed_at", F.current_timestamp())
    .filter(F.col("street_code").isNotNull() & F.col("x").isNotNull() & F.col("y").isNotNull())
)

addr_ins, addr_upd = upsert_rows(addresses, address_table, ["address_id"])
addresses_total = spark.table(address_table).count()
log_check("addresses_bronze_rows_processed", addresses_to_process, True, "silver")
log_check("addresses_silver_inserted", addr_ins, True, "silver")
log_check("addresses_silver_updated", addr_upd, True, "silver")
log_check("addresses_silver_rows", addresses_total, addresses_total > 0, "silver")
print(f"addresses: processed {addresses_to_process} bronze rows -> silver +{addr_ins} ~{addr_upd}, total {addresses_total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Business -> street code (`silver.business_street`)
# MAGIC Goal: give every business the street code it is missing, so Gold can join businesses to closures by code instead of by name.
# MAGIC
# MAGIC Transformation steps, in the order the code runs them (the first step that finds a code wins):
# MAGIC 1. **Input**: valid businesses only (`reject_reason IS NULL`) with their `street_key`, `x`, `y`; all addresses.
# MAGIC 2. **Name -> code table**: from the addresses, one code per `street_key`; if a name maps to several codes, the most common one.
# MAGIC 3. **Match by name** (`by_name`): left join businesses to that table on `street_key`.
# MAGIC 4. **Match by point** (`by_point`), only for businesses still without a code: the address that sits on the same point as the business
# MAGIC    (coordinates rounded to 1 m). The city snaps business points onto its address points, so an equality join is enough.
# MAGIC 5. **Combine**: `street_code` = code by name, else code by point, else NULL; `match_method` records which one.
# MAGIC 6. **Write**: the whole table is rebuilt and overwritten every run (it is derived from two Silver tables, so it must follow them).
# MAGIC 7. **Checks** to `dq_results`: count per `match_method`, including `unmatched`.

# COMMAND ----------

all_addresses = spark.table(address_table)
all_businesses = spark.table(business_table).filter("reject_reason IS NULL").select("business_id", "street_key", "x", "y")

# step 1: street name -> street code (one code per name; if a name maps to several codes, keep the most common one)
name_to_code = (
    all_addresses.groupBy("street_key", "street_code").count()
    .withColumn("rn", F.row_number().over(Window.partitionBy("street_key").orderBy(F.col("count").desc(), F.col("street_code"))))
    .filter("rn = 1")
    .select("street_key", F.col("street_code").alias("code_by_name"))
)
by_name = all_businesses.join(name_to_code, "street_key", "left")

# step 2, only for businesses with no code by name: the address on the same point (coordinates rounded to 1 m).
# If several addresses share a point, the lowest street code is taken.
same_point = (
    all_addresses
    .select(F.round("x").alias("rx"), F.round("y").alias("ry"), "street_code")
    .groupBy("rx", "ry").agg(F.min("street_code").alias("code_by_point"))
)
by_point = (
    by_name.filter(F.col("code_by_name").isNull())
    .withColumn("rx", F.round("x")).withColumn("ry", F.round("y"))
    .join(same_point, ["rx", "ry"])
    .select("business_id", "code_by_point")
)

business_street = (
    by_name.join(by_point, "business_id", "left")
    .withColumn("street_code", F.coalesce(F.col("code_by_name"), F.col("code_by_point")))
    .withColumn("match_method",
                F.when(F.col("code_by_name").isNotNull(), "by_name")
                 .when(F.col("code_by_point").isNotNull(), "by_point")
                 .otherwise("unmatched"))
    .select("business_id", "street_code", "match_method")
    .withColumn("silver_run_id", F.lit(run_id))
    .withColumn("processed_at", F.current_timestamp())
)
business_street.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.business_street")

# COMMAND ----------

method_counts = {r["match_method"]: r["count"] for r in spark.table(f"{SILVER}.business_street").groupBy("match_method").count().collect()}
log_check("business_street_by_name", method_counts.get("by_name", 0), True, "silver")
log_check("business_street_by_point", method_counts.get("by_point", 0), True, "silver")
log_check("business_street_unmatched", method_counts.get("unmatched", 0), True, "silver")
print("business -> street code:", method_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Look at the result
# MAGIC Goal: show a few rows of every Silver table and all the checks this run wrote, so the run can be judged at a glance.

# COMMAND ----------

display(spark.table(f"{SILVER}.closures").limit(5))
display(spark.table(business_table).limit(5))
display(spark.table(address_table).limit(5))
display(spark.table(f"{SILVER}.business_street").filter("match_method = 'by_point'").limit(5))
display(spark.table(f"{GOLD}.dq_results").filter(F.col("run_id") == run_id).orderBy("check_name"))

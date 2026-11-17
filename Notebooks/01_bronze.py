# Databricks notebook source
# MAGIC %md
# MAGIC # 01_bronze
# MAGIC Land the two sources exactly as they are (raw, all strings), with run metadata.
# MAGIC
# MAGIC ## Load strategy: append-only, download only when the source changed
# MAGIC Before downloading, each source is asked for its current version stamp (see section 0).
# MAGIC If the stamp equals the one saved at the last download, the source is skipped and `dq_results` records it.
# MAGIC Otherwise the full source is downloaded and appended to Bronze as a new snapshot, tagged with `run_id` and `ingested_at`.
# MAGIC Nothing in Bronze is ever updated or deleted, so Bronze keeps the history of every snapshot.
# MAGIC Deduplication by key (latest snapshot wins) happens in Silver.
# MAGIC
# MAGIC Writes:
# MAGIC - `tlv.bronze.closures`         one row per CSV line per download (`ID` identifies the line), every column as string
# MAGIC - `tlv.bronze.businesses_raw`   one row per API feature per download (`objectid`), the feature as a JSON string
# MAGIC - `tlv.bronze.addresses_raw`    one row per city address per download (`objectid`), the feature as a JSON string
# MAGIC - `tlv.bronze.source_versions`  one row per source per download: the version stamp that was downloaded

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared definitions
# MAGIC `00_setup` is loaded first because this notebook uses what it defines:
# MAGIC - the table names (`BRONZE`, and `GOLD` for `dq_results`) and the three source URLs (`CLOSURES_CSV_URL`, `BUSINESSES_API_URL`, `ADDRESSES_API_URL`),
# MAGIC - `run_id`, written on every Bronze row and on every row of `source_versions`, so a snapshot can be traced to its run,
# MAGIC - `append_rows`, the Bronze write (append a snapshot, return the row count),
# MAGIC - `log_check`, which records row counts and skipped sources in `gold.dq_results`,
# MAGIC - the Spark imports (`F`, `datetime`) and the `CREATE SCHEMA IF NOT EXISTS` statements that make the first run work on an empty catalog.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

import csv
import io
import json
import time
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Has the source changed since the last download?
# MAGIC Each source carries a "version stamp", a value that changes only when the publisher replaces the data:
# MAGIC - CSV file: the `Last-Modified` header of the file. One HEAD request returns it without downloading the file.
# MAGIC - ArcGIS layer: the `date_import` column, the date the city loaded the layer. The city always reloads the whole layer,
# MAGIC   so every row carries the same value. One `SELECT DISTINCT`-style query (`returnDistinctValues=true`) returns that single value, not the rows.
# MAGIC
# MAGIC After every download, the stamp that was downloaded is written to `bronze.source_versions` (source, stamp, run_id, time).
# MAGIC On the next run, `source_changed` compares the source's current stamp with the last saved one:
# MAGIC equal means nothing new was published, so the download is skipped; different means a new version exists, so it is downloaded and the new stamp is saved.

# COMMAND ----------

VERSIONS_TABLE = f"{BRONZE}.source_versions"    # one row per download: which source, which version stamp, in which run, when


def csv_version(url):
    """
    Reads the current version stamp of the CSV file from the web server.
    Sends one HEAD request (headers only, the file itself is not downloaded) and returns the Last-Modified header,
    for example 'Tue, 03 Feb 2026 08:32:28 GMT'. The value changes only when the file is replaced on the server.
    Nothing is written.
    """
    response = requests.head(url, timeout=60)
    response.raise_for_status()
    return response.headers.get("Last-Modified")


def arcgis_version(url):
    """
    Reads the current version stamp of an ArcGIS layer from the city's API.
    The stamp is the date_import column (the date the city loaded the layer). The city reloads the whole layer at once,
    so every row has the same value, and one distinct-values query returns that single value instead of 37,000 rows.
    Returns it as text, for example '17/11/2026 01:51:44'. Nothing is written.
    """
    params = {"where": "1=1", "outFields": "date_import", "returnDistinctValues": "true", "returnGeometry": "false", "f": "json"}
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    body = response.json()
    if "error" in body:
        raise Exception(body["error"])
    values = sorted(f["attributes"]["date_import"] for f in body["features"])
    return ",".join(values)          # normally exactly one value; joined only in case the layer ever carries more than one


def saved_version(source_name):
    """
    Reads from bronze.source_versions the version stamp of the LAST download of this source
    (the row with the newest downloaded_at for this source_name).
    Returns None when the table does not exist yet (very first run) or has no row for this source (never downloaded).
    Nothing is written.
    """
    if not spark.catalog.tableExists(VERSIONS_TABLE):
        return None
    rows = (spark.table(VERSIONS_TABLE).filter(F.col("source_name") == source_name)
            .orderBy(F.col("downloaded_at").desc()).limit(1).collect())
    return rows[0]["version"] if rows else None


def save_version(source_name, version):
    """
    Writes one row to bronze.source_versions: (source_name, version, run_id, now).
    Called only after a download was appended to Bronze, so the table records what Bronze actually holds;
    if a download fails halfway, no row is written and the next run downloads again.
    The table is created on the first call.
    """
    row = spark.createDataFrame(
        [(source_name, version, run_id, datetime.now())],
        "source_name string, version string, run_id string, downloaded_at timestamp",
    )
    row.write.mode("append").saveAsTable(VERSIONS_TABLE)


def source_changed(source_name, version):
    """
    Decides whether the source must be downloaded in this run.
    Compares `version` (the stamp the source reports now) with saved_version(source_name) (the stamp of the last download).
    Different, or nothing saved yet -> returns True: the caller downloads, appends to Bronze and calls save_version.
    Equal -> returns False: nothing new was published, the download is skipped, and one row is written to
    gold.dq_results (check bronze_<source>_skipped_unchanged = 1) so the skip is visible in the run log.
    """
    last = saved_version(source_name)
    print(f"{source_name}: current version {version!r}, saved version {last!r}")
    if version == last:
        log_check(f"bronze_{source_name}_skipped_unchanged", 1, True, "bronze")
        print(f"{source_name}: unchanged, download skipped")
    return version != last

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Closures CSV
# MAGIC Goal: land the street-closures file as it is, one Bronze row per CSV line, every column as text.

# COMMAND ----------

def download_csv_rows(url):
    """
    Download the CSV and return a list of dicts (one per line), all values as text.
    utf-8-sig removes the BOM character that this file starts with.
    """
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    text = response.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


closures_version = csv_version(CLOSURES_CSV_URL)
if source_changed("closures", closures_version):
    closure_rows = download_csv_rows(CLOSURES_CSV_URL)
    print("csv rows downloaded:", len(closure_rows))
    print("csv columns:", list(closure_rows[0].keys()))

    # keep the CSV column order, every column as string
    csv_columns = list(closure_rows[0].keys())
    closures_schema = ", ".join([f"`{c}` string" for c in csv_columns])
    closures_df = (
        spark.createDataFrame(closure_rows, closures_schema)
        .withColumn("run_id", F.lit(run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source", F.lit(CLOSURES_CSV_URL))
    )
    appended = append_rows(closures_df, f"{BRONZE}.closures")
    save_version("closures", closures_version)
    print("closures: downloaded", len(closure_rows), "appended", appended)
    log_check("bronze_closures_downloaded", len(closure_rows), len(closure_rows) > 0, "bronze")
    log_check("bronze_closures_appended", appended, True, "bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Businesses API (ArcGIS, paged)
# MAGIC Goal: land every business in the layer, one Bronze row per API feature, the feature kept as JSON text.

# COMMAND ----------

def fetch_page(url, offset, page_size, out_fields):
    """One API call: return the response body with the features starting at `offset`."""
    params = {
        "where": "1=1",
        "outFields": out_fields,
        "f": "json",
        "resultOffset": offset,
        "resultRecordCount": page_size,
    }
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    body = response.json()
    if "error" in body:                       # ArcGIS returns errors with HTTP 200
        raise Exception(body["error"])
    return body


def fetch_all_features(url, page_size=2000, out_fields="*"):
    """
    Loop over pages with resultOffset until a page comes back empty.
    One retry per page with a short wait, in case of a transient failure.
    """
    all_features = []
    offset = 0
    while True:
        try:
            body = fetch_page(url, offset, page_size, out_fields)
        except Exception as e:
            print("page at offset", offset, "failed once:", e, "-> retrying")
            time.sleep(2)
            body = fetch_page(url, offset, page_size, out_fields)
        page = body.get("features", [])
        if len(page) == 0:
            break
        all_features.extend(page)
        offset = offset + len(page)
        print("fetched", len(all_features), "features so far")
    return all_features


def features_to_df(features, oid_field):
    """
    One row per feature: the object id as a column, the whole feature kept as a JSON string (Silver will flatten it).
    oid_field is the layer's object id field: OBJECTID for businesses, oid_ktovet for addresses.
    """
    rows = []
    for f in features:
        rows.append((f["attributes"][oid_field], json.dumps(f, ensure_ascii=False)))
    return spark.createDataFrame(rows, "objectid long, feature_json string")

# COMMAND ----------

businesses_version = arcgis_version(BUSINESSES_API_URL)
if source_changed("businesses", businesses_version):
    features = fetch_all_features(BUSINESSES_API_URL)
    print("features downloaded:", len(features))
    print("first feature:", json.dumps(features[0], ensure_ascii=False)[:300])

    businesses_df = (
        features_to_df(features, "OBJECTID")
        .withColumn("run_id", F.lit(run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source", F.lit(BUSINESSES_API_URL))
    )
    appended = append_rows(businesses_df, f"{BRONZE}.businesses_raw")
    save_version("businesses", businesses_version)
    print("businesses: downloaded", len(features), "appended", appended)
    log_check("bronze_businesses_downloaded", len(features), len(features) > 0, "bronze")
    log_check("bronze_businesses_appended", appended, True, "bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. City addresses (ArcGIS layer 527): street code + street name + house number + x/y
# MAGIC Goal: land the city address points, which later give each business its street code (the businesses API has none).

# COMMAND ----------

# only the fields Silver needs, to keep the download small
ADDRESS_FIELDS = "oid_ktovet,k_rechov,t_rechov,ms_bayit,knisa,x,y,date_import"

addresses_version = arcgis_version(ADDRESSES_API_URL)
if source_changed("addresses", addresses_version):
    addresses = fetch_all_features(ADDRESSES_API_URL, out_fields=ADDRESS_FIELDS)
    print("addresses downloaded:", len(addresses))
    print("first address:", json.dumps(addresses[0], ensure_ascii=False)[:300])

    addresses_df = (
        features_to_df(addresses, "oid_ktovet")
        .withColumn("run_id", F.lit(run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source", F.lit(ADDRESSES_API_URL))
    )
    appended = append_rows(addresses_df, f"{BRONZE}.addresses_raw")
    save_version("addresses", addresses_version)
    print("addresses: downloaded", len(addresses), "appended", appended)
    log_check("bronze_addresses_downloaded", len(addresses), len(addresses) > 0, "bronze")
    log_check("bronze_addresses_appended", appended, True, "bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. State of Bronze after this run
# MAGIC Goal: show what this run added: rows per table, the saved version stamps, and a few sample rows.
# MAGIC A downloaded source adds a full snapshot (total rows grow by the snapshot size); a skipped source adds nothing.

# COMMAND ----------

print("this run_id:", run_id)
for table in ["closures", "businesses_raw", "addresses_raw"]:
    total = spark.table(f"{BRONZE}.{table}").count()
    this_run = spark.table(f"{BRONZE}.{table}").filter(F.col("run_id") == run_id).count()
    print(f"{table}: {total} rows in Bronze, {this_run} appended in this run")

display(spark.table(VERSIONS_TABLE).orderBy("source_name", "downloaded_at"))
display(spark.sql(f"SELECT run_id, COUNT(*) AS rows FROM {BRONZE}.closures GROUP BY run_id ORDER BY run_id"))
display(spark.sql(f"SELECT run_id, COUNT(*) AS rows FROM {BRONZE}.businesses_raw GROUP BY run_id ORDER BY run_id"))
display(spark.sql(f"SELECT run_id, COUNT(*) AS rows FROM {BRONZE}.addresses_raw GROUP BY run_id ORDER BY run_id"))
display(spark.table(f"{BRONZE}.closures").limit(5))
display(spark.table(f"{BRONZE}.businesses_raw").limit(3))
display(spark.table(f"{BRONZE}.addresses_raw").limit(3))

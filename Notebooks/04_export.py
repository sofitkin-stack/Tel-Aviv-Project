# Databricks notebook source
# MAGIC %md
# MAGIC # 04_export
# MAGIC Writes the cleaned Silver tables and the final Gold outputs as CSV files, one folder per table, to the volume
# MAGIC `tlv.gold.export` (Catalog -> tlv -> gold -> Volumes -> export). From there the files can be downloaded or read by any tool.
# MAGIC
# MAGIC Goal: deliver the datasets the assignment asks for (cleaned data and final business outputs) as plain files, rewritten on every run.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Create the volume if it does not exist.
# MAGIC 2. For every table in the list: read it, write it as one CSV file with a header, overwriting the previous export.
# MAGIC 3. Log the number of exported tables to `gold.dq_results` and list the export folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared definitions
# MAGIC `00_setup` is loaded first because this notebook uses what it defines:
# MAGIC - the names `CATALOG`, `SILVER` and `GOLD`, to find the tables and to place the volume,
# MAGIC - `log_check`, which writes one row to `gold.dq_results`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {GOLD}.export")
EXPORT_PATH = f"/Volumes/{CATALOG}/gold/export"

# table -> folder name under EXPORT_PATH
TABLES_TO_EXPORT = {
    f"{SILVER}.closures": "silver_closures",                      # cleaned closure sides (valid rows and rejected rows with reject_reason)
    f"{SILVER}.businesses": "silver_businesses",                  # cleaned businesses, latest record per business
    f"{SILVER}.addresses": "silver_addresses",                    # city addresses with street codes
    f"{SILVER}.business_street": "silver_business_street",        # business -> street code, with match_method
    f"{GOLD}.fact_business_day_2023": "gold_fact_business_day_2023",
    f"{GOLD}.comp_by_business_2023": "gold_comp_by_business_2023",  # deliverable 1
    f"{GOLD}.comp_by_street_2023": "gold_comp_by_street_2023",      # deliverable 2
    f"{GOLD}.metrics_2023": "gold_metrics_2023",
}

for table, folder in TABLES_TO_EXPORT.items():
    df = spark.table(table)
    (df.coalesce(1)                                # one CSV file per table
       .write.mode("overwrite")
       .option("header", "true")
       .csv(f"{EXPORT_PATH}/{folder}"))
    print(f"{table}: {df.count()} rows -> {EXPORT_PATH}/{folder}")

log_check("export_tables_written", len(TABLES_TO_EXPORT), True, "export")
display(dbutils.fs.ls(EXPORT_PATH))

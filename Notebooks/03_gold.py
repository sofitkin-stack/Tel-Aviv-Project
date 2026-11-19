# Databricks notebook source
# MAGIC %md
# MAGIC # 03_gold
# MAGIC Compensation for 2023: one row per business per compensated day, and the totals per business and per street.
# MAGIC
# MAGIC ## Load strategy
# MAGIC 1. **Rebuild only when Silver changed.** Section 0 compares the newest `processed_at` in Silver with the stamp of the last build (`gold.build_log`). Equal: the notebook stops, nothing is read or written.
# MAGIC 2. **When it rebuilds, it rebuilds everything** (overwrite). One Silver change fans out to many fact rows (a shortened closure removes days for every business on the street),
# MAGIC    so recomputing from Silver is simpler and safer than updating rows in place, and a rerun always gives the same result. At 655K fact rows it takes seconds.
# MAGIC
# MAGIC ## Business rules
# MAGIC 1. A business is eligible on a day when a closure touches its street (match by street code; both streets of a closure count).
# MAGIC 2. One compensation per business per day, whatever the number of closures that day.
# MAGIC 3. 100 ILS per unit of area (`shetach`) per day, capped at 10,000 ILS per business per day.
# MAGIC 4. Days in 2023 only. A business founded after 2023 gets nothing; one founded during 2023 is eligible from its founding date.
# MAGIC
# MAGIC ## Writes
# MAGIC `gold.closure_days_2023`, `gold.dim_business`, `gold.fact_business_day_2023`, `gold.comp_by_business_2023`, `gold.comp_by_street_2023`, `gold.metrics_2023`, `gold.build_log`; checks go to `gold.dq_results`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared definitions
# MAGIC `00_setup` is loaded first because this notebook uses what it defines:
# MAGIC - the schema names `SILVER` (what Gold reads) and `GOLD` (what Gold writes),
# MAGIC - `run_id`, written on `metrics_2023` and on every check row,
# MAGIC - the compensation rule: `daily_compensation`, `RATE_PER_AREA`, `DAILY_CAP` (section 4),
# MAGIC - `log_check`, which writes the checks of section 7 to `gold.dq_results`,
# MAGIC - the imports `F`, `Window`, `datetime`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

YEAR_START = "2023-01-01"
YEAR_END = "2023-12-31"


def save_gold(df, name):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.{name}")
    return spark.table(f"{GOLD}.{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Has Silver changed since the last Gold build?
# MAGIC Goal: skip the whole rebuild on a run where nothing in Silver changed, so a daily run without new data costs three small queries and no writes.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Silver stamp: the newest `processed_at` across `silver.closures`, `silver.businesses` and `silver.addresses`.
# MAGIC    The MERGE in Silver touches `processed_at` only on rows whose content changed, so this value moves only when something really changed.
# MAGIC    `silver.business_street` is derived from businesses and addresses and rebuilt every run, so it is not part of the stamp.
# MAGIC 2. Compare with the stamp saved in `gold.build_log` by the last build.
# MAGIC 3. Equal: write `gold_skipped_unchanged = 1` to `dq_results` and stop the notebook here.
# MAGIC    Different, or no build yet: continue; section 7 saves the new stamp after a successful build.

# COMMAND ----------

BUILD_LOG = f"{GOLD}.build_log"    # one row per Gold build: run_id, the Silver stamp it was built from, when


def silver_stamp():
    """The newest processed_at in the three Silver source tables: when Silver last changed."""
    stamps = [
        spark.table(f"{SILVER}.{t}").agg(F.max("processed_at")).collect()[0][0]
        for t in ("closures", "businesses", "addresses")
    ]
    return max(s for s in stamps if s is not None)


def last_build_stamp():
    """The Silver stamp the last Gold build used, or None when Gold was never built."""
    if not spark.catalog.tableExists(BUILD_LOG):
        return None
    rows = spark.table(BUILD_LOG).orderBy(F.col("built_at").desc()).limit(1).collect()
    return rows[0]["silver_stamp"] if rows else None


current_stamp = silver_stamp()
previous_stamp = last_build_stamp()
print(f"silver last changed {current_stamp}; last gold build used {previous_stamp}")
if current_stamp == previous_stamp:
    log_check("gold_skipped_unchanged", 1, True, "gold")
    dbutils.notebook.exit("gold not rebuilt: Silver has not changed since the last build")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Street names (one display name per street code)
# MAGIC Goal: give every street code one official name for the reports (the city address names, closures as fallback).
# MAGIC
# MAGIC Steps:
# MAGIC 1. Union the (code, name) pairs of all addresses and of all valid closure rows.
# MAGIC 2. Count how often each pair appears; per code keep the most frequent name (ties: alphabetical).
# MAGIC 3. Result: `street_names`, one row per code, used by sections 3 and 5 (not written as a table).

# COMMAND ----------

# the city addresses carry the official name; closures are the fallback for codes with no address
street_names = (
    spark.table(f"{SILVER}.addresses").select("street_code", "street_name")
    .unionByName(spark.table(f"{SILVER}.closures").filter("reject_reason IS NULL").select("street_code", "street_name"))
    .groupBy("street_code", "street_name").count()
    .withColumn("rn", F.row_number().over(Window.partitionBy("street_code").orderBy(F.col("count").desc(), "street_name")))
    .filter("rn = 1")
    .select("street_code", "street_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Closure days: one row per (street, day) closed in 2023
# MAGIC Goal: expand every valid, non-pedestrian closure into its calendar days, clipped to 2023, one row per street per day (a street closed twice on a day appears once).
# MAGIC
# MAGIC Steps:
# MAGIC 1. Read `silver.closures` with `reject_reason IS NULL`; drop rows flagged `is_pedestrian_street` (their event count is kept for the metrics).
# MAGIC 2. Clip each closure to 2023: `from_day = max(start_date, 2023-01-01)`, `to_day = min(end_date, 2023-12-31)`; a closure entirely outside 2023 disappears here.
# MAGIC 3. Expand to one row per day with `explode(sequence(from_day, to_day))`.
# MAGIC 4. Group by (street_code, day), counting distinct events, so a street closed twice on a day is one row.
# MAGIC 5. Write `gold.closure_days_2023`.

# COMMAND ----------

closures_all = spark.table(f"{SILVER}.closures").filter("reject_reason IS NULL").select("id_rechov", "side", "street_code", "start_date", "end_date", "is_pedestrian_street")
# permanent pedestrian streets are not compensated
closures = closures_all.filter(~F.col("is_pedestrian_street")).drop("is_pedestrian_street")
pedestrian_events_excluded = closures_all.filter("is_pedestrian_street").select("id_rechov").distinct().count()

closure_days = (
    closures
    # cut each closure to the 2023 window before expanding it to days
    .withColumn("from_day", F.greatest(F.col("start_date"), F.lit(YEAR_START).cast("date")))
    .withColumn("to_day", F.least(F.col("end_date"), F.lit(YEAR_END).cast("date")))
    .filter(F.col("from_day") <= F.col("to_day"))
    .withColumn("day", F.explode(F.sequence(F.col("from_day"), F.col("to_day"))))   # SQL: explode(sequence(from_day, to_day))
    .select("street_code", "day", "id_rechov")
)
closure_days_2023 = save_gold(
    closure_days.groupBy("street_code", "day").agg(F.countDistinct("id_rechov").alias("closures_that_day")),
    "closure_days_2023",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Businesses with a street code (dim_business)
# MAGIC Goal: one row per valid business with its area, founding date, street code and how the code was matched; the business side of the join.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Read `silver.businesses` with `reject_reason IS NULL`.
# MAGIC 2. Left join `silver.business_street` to get `street_code` and `match_method` (a business without a code keeps NULL and is counted later).
# MAGIC 3. Left join `street_names` to get the official street name; the source name is kept beside it.
# MAGIC 4. Convert `founded_at` to a date (`founded_date`).
# MAGIC 5. Write `gold.dim_business`.

# COMMAND ----------

businesses = spark.table(f"{SILVER}.businesses").filter("reject_reason IS NULL")
business_street = spark.table(f"{SILVER}.business_street").select("business_id", "street_code", "match_method")

dim_business = save_gold(
    businesses.join(business_street, "business_id", "left")
    .join(street_names.withColumnRenamed("street_name", "street_name_official"), "street_code", "left")
    .select(
        "business_id", "business_type", "owner_name",
        F.col("street_name").alias("street_name_in_source"),
        F.coalesce(F.col("street_name_official"), F.col("street_name")).alias("street_name"),
        "house_number", "street_code", "match_method", "area",
        F.to_date("founded_at").alias("founded_date"), "x", "y",
    ),
    "dim_business",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. fact_business_day_2023: one row per business per compensated day
# MAGIC Goal: join businesses to closure days on street code, keep days on or after the founding date, and compute the daily compensation: area x 100, capped at 10,000.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Eligible businesses: a street code is known, and the business was not founded after 2023.
# MAGIC 2. Inner join to `closure_days_2023` on `street_code`: one row per business per closed day of its street (rule 1).
# MAGIC 3. Keep only days on or after the founding date (a business founded in mid-2023 earns from that day).
# MAGIC 4. Rule 2 holds by construction: a business has one street and closure days are unique per street and day, so (business_id, day) is unique; section 7 checks it.
# MAGIC 5. Computed columns: `daily_comp_uncapped = area * 100`, `daily_comp = least(that, 10000)` (rules 3 and 4), `hit_cap`.
# MAGIC 6. Write `gold.fact_business_day_2023`.

# COMMAND ----------

eligible_businesses = (
    dim_business
    .filter(F.col("street_code").isNotNull())
    .filter(F.col("founded_date").isNull() | (F.col("founded_date") <= F.lit(YEAR_END).cast("date")))   # founded after 2023: out
    .select("business_id", "street_code", "area", "founded_date")
)

fact = (
    eligible_businesses.join(closure_days_2023, "street_code")
    .filter(F.col("founded_date").isNull() | (F.col("day") >= F.col("founded_date")))                     # eligible from founding date
    # rule 2: once per business per day (a business has one street, so (business_id, day) is already unique after the join)
    .select("business_id", "street_code", "day", "area", "closures_that_day")
    .withColumn("daily_comp_uncapped", F.col("area") * RATE_PER_AREA)
    .withColumn("daily_comp", daily_compensation(F.col("area")))                                           # LEAST(area*100, 10000), defined in 00_setup
    .withColumn("hit_cap", F.col("daily_comp_uncapped") > DAILY_CAP)
)
fact_business_day = save_gold(fact, "fact_business_day_2023")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Outputs: totals per business and per street
# MAGIC Goal: the two deliverables of the assignment: annual compensation per business and per street, with days, cap hits and uncapped amounts.
# MAGIC
# MAGIC Steps:
# MAGIC 1. `comp_by_business_2023`: group the fact by `business_id` (days, total, uncapped total, days at cap), join the business attributes from `dim_business`, add the average per day.
# MAGIC 2. `comp_by_street_2023`: group the fact by `street_code` (businesses, closure days, business-days, total), join the street name, add the total per closure day.
# MAGIC 3. Both written sorted by total, largest first.

# COMMAND ----------

comp_by_business = save_gold(
    fact_business_day.groupBy("business_id")
    .agg(
        F.count("*").alias("compensated_days"),
        F.sum("daily_comp").alias("total_comp_ils"),
        F.sum("daily_comp_uncapped").alias("total_comp_uncapped_ils"),
        F.sum(F.col("hit_cap").cast("int")).alias("days_at_cap"),
    )
    .join(dim_business.select("business_id", "business_type", "owner_name", "street_name", "house_number", "street_code", "area"), "business_id")
    .withColumn("avg_comp_per_day_ils", F.round(F.col("total_comp_ils") / F.col("compensated_days"), 2))
    .select("business_id", "business_type", "owner_name", "street_name", "house_number", "street_code", "area",
            "compensated_days", "total_comp_ils", "avg_comp_per_day_ils", "days_at_cap", "total_comp_uncapped_ils")
    .orderBy(F.col("total_comp_ils").desc()),
    "comp_by_business_2023",
)

comp_by_street = save_gold(
    fact_business_day.groupBy("street_code")
    .agg(
        F.countDistinct("business_id").alias("businesses"),
        F.countDistinct("day").alias("closure_days"),
        F.count("*").alias("business_days"),
        F.sum("daily_comp").alias("total_comp_ils"),
    )
    .join(street_names, "street_code", "left")
    .withColumn("comp_per_closure_day_ils", F.round(F.col("total_comp_ils") / F.col("closure_days"), 2))
    .select("street_code", "street_name", "businesses", "closure_days", "business_days", "total_comp_ils", "comp_per_closure_day_ils")
    .orderBy(F.col("total_comp_ils").desc()),
    "comp_by_street_2023",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Metrics: cost per area and more
# MAGIC Goal: one small table of headline numbers (total, businesses, days, cost per area unit, share of days at the cap) for the README and the dashboard.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Aggregate the fact once: total, uncapped total, distinct businesses, business-days, days at cap.
# MAGIC 2. Add the counts that do not come from the fact: total area of compensated businesses, streets closed in 2023, businesses without a street code, pedestrian events excluded.
# MAGIC 3. Build one row per metric (name, value, description) and write `gold.metrics_2023` with the `run_id`.

# COMMAND ----------

totals = fact_business_day.agg(
    F.sum("daily_comp").alias("total_comp"),
    F.sum("daily_comp_uncapped").alias("total_comp_uncapped"),
    F.countDistinct("business_id").alias("businesses"),
    F.count("*").alias("business_days"),
    F.sum(F.col("hit_cap").cast("int")).alias("days_at_cap"),
).collect()[0]
area_total = comp_by_business.agg(F.sum("area")).collect()[0][0]
streets_closed = closure_days_2023.select("street_code").distinct().count()
unmatched_businesses = dim_business.filter(F.col("street_code").isNull()).count()

metrics_rows = [
    ("total_compensation_ils", float(totals["total_comp"]), "Sum of all daily compensations in 2023"),
    ("compensated_businesses", float(totals["businesses"]), "Businesses with at least one compensated day"),
    ("compensated_business_days", float(totals["business_days"]), "Rows in fact_business_day_2023"),
    ("cost_per_area_unit_ils", round(float(totals["total_comp"]) / float(area_total), 2), "total_compensation / total area of compensated businesses"),
    ("avg_comp_per_business_ils", round(float(totals["total_comp"]) / float(totals["businesses"]), 2), "total_compensation / compensated businesses"),
    ("share_of_days_at_cap", round(float(totals["days_at_cap"]) / float(totals["business_days"]), 4), "Business-days where area*100 exceeded the 10,000 cap"),
    ("cap_savings_ils", float(totals["total_comp_uncapped"] - totals["total_comp"]), "What the cap saved versus paying area*100 with no cap"),
    ("streets_with_closures_2023", float(streets_closed), "Distinct street codes closed on at least one day in 2023"),
    ("businesses_without_street_code", float(unmatched_businesses), "Businesses that could not be matched to a street, so could not be compensated"),
    ("pedestrian_street_events_excluded", float(pedestrian_events_excluded), "Closure events marked as a permanent pedestrian street, excluded from compensation"),
]
metrics_2023 = save_gold(
    spark.createDataFrame(metrics_rows, "metric string, value double, description string")
    .withColumn("run_id", F.lit(run_id)).withColumn("computed_at", F.current_timestamp()),
    "metrics_2023",
)
display(metrics_2023)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Checks
# MAGIC Goal: prove the fact table is sound: no duplicate business-day, no day over the cap or outside 2023 or before founding, and both output tables sum to the same total; every check goes to `gold.dq_results`.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Count violations of each rule directly on the written tables (not on the DataFrames in memory).
# MAGIC 2. Write one `dq_results` row per check with the value and pass/fail.
# MAGIC 3. Stop with an error if any Gold check of this run failed, so the job turns red and `build_log` is not written (the next run rebuilds).
# MAGIC 4. Save the Silver stamp this build used to `gold.build_log` (section 0 reads it on the next run).
# MAGIC 5. Show the top 10 businesses and streets and this run's Gold checks.

# COMMAND ----------

fact_dups = fact_business_day.groupBy("business_id", "day").count().filter("count > 1").count()
over_cap = fact_business_day.filter(F.col("daily_comp") > DAILY_CAP).count()
outside_2023 = fact_business_day.filter((F.col("day") < YEAR_START) | (F.col("day") > YEAR_END)).count()
before_founding = fact_business_day.join(dim_business.select("business_id", "founded_date"), "business_id") \
    .filter(F.col("day") < F.col("founded_date")).count()
sum_business = comp_by_business.agg(F.sum("total_comp_ils")).collect()[0][0] or 0
sum_street = comp_by_street.agg(F.sum("total_comp_ils")).collect()[0][0] or 0

log_check("gold_fact_rows", fact_business_day.count(), True, "gold")
log_check("gold_fact_duplicates_business_day", fact_dups, fact_dups == 0, "gold")
log_check("gold_daily_comp_over_cap", over_cap, over_cap == 0, "gold")
log_check("gold_days_outside_2023", outside_2023, outside_2023 == 0, "gold")
log_check("gold_days_before_founding", before_founding, before_founding == 0, "gold")
log_check("gold_business_vs_street_totals_diff", sum_business - sum_street, abs(sum_business - sum_street) < 0.01, "gold")
log_check("gold_businesses_without_street_code", unmatched_businesses, True, "gold")

# data quality gate: a failed check stops the run here (the tables are written, but the build is not recorded)
failed_checks = (spark.table(f"{GOLD}.dq_results")
    .filter((F.col("run_id") == run_id) & (F.col("layer") == "gold") & (F.col("passed") == False))
    .select("check_name", "value").collect())
if failed_checks:
    raise ValueError(f"gold checks failed: {[(r['check_name'], r['value']) for r in failed_checks]}")

# remember which Silver state this build reflects
spark.createDataFrame(
    [(run_id, current_stamp, datetime.now())], "run_id string, silver_stamp timestamp, built_at timestamp"
).write.mode("append").saveAsTable(BUILD_LOG)

print("fact rows:", fact_business_day.count(), "| total compensation 2023:", totals["total_comp"])
display(comp_by_business.limit(10))
display(comp_by_street.limit(10))
display(spark.table(f"{GOLD}.dq_results").filter((F.col("run_id") == run_id) & (F.col("layer") == "gold")).orderBy("check_name"))

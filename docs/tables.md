# Tables

All tables live in the catalog `tlv` (schemas `bronze`, `silver`, `gold`). Back to the [README](../README.md).

**Bronze** (`tlv.bronze`, append-only, every column as text or JSON)

| table | one row is | key |
|---|---|---|
| `closures` | one CSV line per download | `ID` + `run_id` |
| `businesses_raw` | one API feature (JSON) per download | `objectid` + `run_id` |
| `addresses_raw` | one city address feature (JSON) per download | `objectid` + `run_id` |
| `source_versions` | one download: source name, version stamp, run, time | |

**Silver** (`tlv.silver`, typed, merged by key, `reject_reason` NULL = valid)

| table | one row is | key | notes |
|---|---|---|---|
| `closures` | one side (from / to) of one closure event | `id_rechov`, `side` | dates, `street_code`, `is_pedestrian_street`, `reject_reason` |
| `businesses` | one business, latest record | `business_id` | `area`, `founded_at`, `x`, `y`, `reject_reason` |
| `addresses` | one city address | `address_id` | `street_code`, `street_name`, `house_number`, `x`, `y` |
| `business_street` | one business and its street code | `business_id` | `match_method` = by_name / by_point / unmatched; rebuilt every run |

**Gold** (`tlv.gold`, rebuilt when Silver changed)

| table | one row is | main columns |
|---|---|---|
| `closure_days_2023` | one street and one day it was closed in 2023 | `street_code`, `day`, `closures_that_day` |
| `dim_business` | one valid business with its street code | `business_id`, `street_code`, `match_method`, `area`, `founded_date` |
| `fact_business_day_2023` | one business and one compensated day | `daily_comp`, `daily_comp_uncapped`, `hit_cap` |
| `comp_by_business_2023` | annual total per business (deliverable 1) | `compensated_days`, `total_comp_ils`, `days_at_cap` |
| `comp_by_street_2023` | annual total per street (deliverable 2) | `businesses`, `closure_days`, `total_comp_ils` |
| `metrics_2023` | one metric per row | `total_compensation_ils`, `cost_per_area_unit_ils`, `share_of_days_at_cap`, ... |
| `dq_results` | one check of one run, every layer | `run_id`, `layer`, `check_name`, `value`, `passed` |
| `build_log` | one Gold build and the Silver stamp it used | `run_id`, `silver_stamp`, `built_at` |

# Output files

CSV exports of the run of 19.11.2026, written by `Notebooks/04_export.py` from the tables in `tlv.silver` and `tlv.gold`. Every file has a header row. Back to the [README](../README.md); column-level detail is in [docs/tables.md](../docs/tables.md).

| file | source table | what it is | grain (one row is) | rows | use |
|---|---|---|---|---|---|
| `silver_closures.csv` | `silver.closures` | the street-closures CSV after cleaning: parsed dates, missing end date filled with the start date, each closure split into its two streets, street code attached, duplicates removed | one side (from / to) of one closure event; key `id_rechov` + `side` | 3,994 (334 of them with a `reject_reason`, no street code; 20 flagged `is_pedestrian_street`) | the cleaned closures deliverable; the input Gold expands into closure days |
| `silver_businesses.csv` | `silver.businesses` | the businesses API after cleaning: JSON flattened, `area` numeric, dates typed, one record per business id (latest `date_import`) | one business; key `business_id` | 37,390 | the cleaned businesses deliverable; the input Gold joins to closure days by street code |
| `comp_by_business_2023.csv` | `gold.comp_by_business_2023` | annual compensation per business for 2023, with its attributes, compensated days, days at the cap and the uncapped amount | one compensated business; key `business_id` | 7,484 | deliverable 1 (totals per business); dashboard tables and filter |
| `comp_by_street_2023.csv` | `gold.comp_by_street_2023` | annual compensation per street for 2023, with businesses, closure days and business-days | one street with at least one compensated business; key `street_code` | 84 | deliverable 2 (totals per street); dashboard top-10 chart |
| `metrics_2023.csv` | `gold.metrics_2023` | the headline numbers of the run: total, businesses, business-days, cost per area unit, share of days at the cap, cap savings, streets closed, unmatched businesses, pedestrian events excluded | one metric; key `metric` | 10 | the figures quoted in the README and in `docs/results.md`; dashboard counters |

Not exported here: `gold.fact_business_day_2023` (one row per business per compensated day, 654,671 rows, about 40 MB) and the Silver helper tables `addresses` and `business_street`. They are written to the volume `tlv.gold.export` by `04_export`.

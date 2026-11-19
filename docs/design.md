# Design: assumptions, decisions, limitations

Back to the [README](../README.md).

## Assumptions and decisions

1. Compensation is calculated only for days in 2023.
2. `shetach` is taken from the API as is (an integer), 100 ILS per unit per day, capped at 10,000 ILS per business per day.
3. A business is matched to a closure by street code. The businesses API carries only a street name, so the code comes from the city addresses layer (ArcGIS layer 527: 52,165 addresses with street code, street name, house number and coordinates).
   - First by normalized street name.
   - When no address carries that name, by the address that sits on the same point as the business (coordinates rounded to 1 m; the city snaps business points onto its address points).
   - Businesses that match neither way are counted in `dq_results` and receive no compensation.
   - Both sources use the same street codes (checked on 7 codes from the closures file).
   - Validation (`sql/street_match_comparison.sql`, 17.11.2026): resolving the code by name and by nearest address independently gives the same code for 98.7% of businesses (36,867). The average distance from a business point to its nearest address point is 0.1 m, so business coordinates are snapped to the city address points. 156 businesses (0.4%) get different codes from the two methods, 125 have a name but no address within 50 m, 196 have an address but an unknown name, and 26 have neither.
4. Each closure row names two streets; both are treated as affected. Streets located between the two named streets are not included: the assignment does not define them and the data gives no way to identify them. `id_rechov` is used only as the row identifier.
5. Every day in a closure range is an eligible day.
6. Businesses founded after 2023 are excluded (in Gold). A business founded during 2023 is eligible only from its founding date (`tr_hakama`) onward.
7. Bronze is append-only and downloads a source only when a new version was published.
   - Each source has a version stamp that changes only when the publisher replaces the data. For the CSV it is the file's `Last-Modified` header (one HEAD request, no download). For an ArcGIS layer it is the `date_import` column, the date the city loaded the layer; it is identical on every row because the city always reloads the whole layer, so one distinct-values query returns that single value.
   - After each download the stamp is saved in `bronze.source_versions`. On the next run the current stamp is compared with the saved one: equal means skip (recorded in `dq_results` as `bronze_<source>_skipped_unchanged`), different means download.
   - A changed source is downloaded in full and appended as a new snapshot tagged with `run_id` and `ingested_at`; nothing in Bronze is updated or deleted. The sources offer no per-record change date, so record-level incremental extraction is not possible. The city reloads both layers often: both carried a 17.11.2026 stamp the morning after a 16.11 run.
8. Closures: `id_rechov` identifies the closure event (one value per closed segment and date range), not a street.
   - 9 events appear twice in the file under different `ID`s and are kept once in Silver (lowest `ID`).
   - The 334 sides whose street code is 0 (336 before the duplicate events are removed) carry no street information and are kept with `reject_reason = missing_street_code`, so Gold ignores them (203 events: 70 with one valid side that still counts, 133 with none; only 6 of the 203 touch 2023).
   - Some of these rows name streets in the free-text remark column (for example "חסימה מרחוב פרישמן עד לרחוב אבן גבירול"); recovering a street code from that text against the city street table is a possible next step, not done here.
9. Silver is loaded incrementally: each run processes only Bronze rows ingested after the highest `bronze_ingested_at` already in Silver, keeps the latest snapshot per key, and merges by key with Delta MERGE (closure event x side; business id, where the latest `date_import` in the batch wins). New keys are inserted, changed records updated, identical ones left untouched. Rows that fail validation stay in the same Silver table with a `reject_reason` (NULL = valid); Gold reads only valid rows, and a record that is fixed at the source gets its `reject_reason` cleared by the next MERGE. The pipeline keeps the same cost when the sources grow.
10. Gold is rebuilt in full (overwrite), but only when Silver changed.
    - Full rebuild: Gold covers one finished year whose data is corrected rather than grown (late closures, fixed dates, changed business attributes or rules). The fact is bounded by businesses on closed streets x 8,408 street-days (about 655K rows), and a rebuild applies inserts, updates and deletes from Silver in one idempotent step.
    - Only when Silver changed: Gold compares the newest `processed_at` across the three Silver source tables with the stamp saved in `gold.build_log` by the last build. On an unchanged Silver it records `gold_skipped_unchanged` in `dq_results` and stops.
    - Compensation is computed on `fact_business_day_2023`, one row per business per compensated day, with `daily_comp = LEAST(area * 100, 10000)`.
11. Permanent pedestrian streets are not compensated. 9 closure events carry the text "מדרחוב" (pedestrian street) in the free-text hours/remark columns (`shaot`, `sw_laila`); the five largest (`id_rechov` 2994, 2995, 2997, 2998, 2999: Herzl, Rothschild, Florentin, HaCarmel, HaYarkon and their paired streets) start in June 2020 and cover most of 2023. They describe a permanent change of the street, not a closure that harms a business temporarily. Silver flags them with `is_pedestrian_street`, Gold excludes them (model B, used for all outputs), and `metrics_2023` reports how many events were excluded. For comparison, Gold also computes the literal rule with these streets included (model A) and writes both totals and their difference to `metrics_2023`. Before this exclusion these events produced about 2,450 of the 8,408 street-days of 2023 and most of the total compensation (6.2 billion ILS).

## Design decisions in one place

| decision | why | where |
|---|---|---|
| Bronze downloads only when the publisher's version stamp changed | the sources have no per-record change date; the stamp is the only change signal they offer, and one request answers it | `01_bronze` section 0, assumption 7 |
| `date_import` compared as text, with equality | the layer stores it as a string; the question is "same load or a new one", not "newer", so equality is enough and never misreads a date format | `01_bronze` section 0 |
| Silver keeps invalid rows with a `reject_reason` instead of a quarantine table | one table per entity to maintain; a record fixed at the source clears its own reason on the next MERGE | `02_silver`, assumption 9 |
| Street code by name, then by point | the businesses layer has no street code; the city snaps business points onto address points, so an exact point match is a safe second step | `02_silver` section 4, assumption 3 |
| Gold is rebuilt in full, but only when Silver changed | one finished year whose data is corrected rather than grown; a full rebuild applies inserts, updates and deletes in one idempotent step, and the stamp check makes the daily run free when nothing changed | `03_gold` section 0, assumption 10 |
| The compensation rule is one function in `00_setup` | `daily_compensation` is used by Gold and tested in `tests/`; the rate and cap are defined once | `00_setup` |
| A failed Gold check stops the run and the build is not recorded | the job turns red instead of publishing wrong numbers; the next run rebuilds | `03_gold` section 7 |

## Known limitations

1. Bronze downloads a changed source in full (about 37K businesses, 52K addresses); the sources offer no way to fetch only changed records.
2. The 334 closure sides without a street code (assumption 8) are not compensated; some name a street in free text that is not parsed.
3. Businesses whose street name is unknown to the city address layer and whose point sits on no address (26 in the 16.11 run) receive no compensation and are only counted.
4. Each notebook run has its own `run_id`, so in a job the four tasks write four ids per day to `dq_results`.
5. Street-closure rows name two streets; streets between them are not identified (assumption 4).

## Next steps for production

1. Alerting on the job (failure email or Slack) and on `dq_results` rows with `passed = false`.
2. A CI step that runs `pytest` on every pull request.
3. Expected-schema checks on the API response before Bronze appends it, so a renamed field fails loudly.
4. Retention for Bronze snapshots (for example keep 90 days) once the history is no longer needed.

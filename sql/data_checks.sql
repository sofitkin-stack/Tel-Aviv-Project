-- Data sanity checks for the 2023 compensation result. Run each block in a SQL cell.
-- "Expected" says what a clean result looks like.

-- 1. Independent recomputation of the total, without the Gold tables.
--    Expected: equals total_compensation_ils in tlv.gold.metrics_2023 (4,034,540,500).
WITH days AS (
  SELECT DISTINCT street_code, explode(sequence(greatest(start_date, DATE'2023-01-01'),
                                            least(end_date,   DATE'2023-12-31'))) AS day
  FROM tlv.silver.closures
  WHERE reject_reason IS NULL AND NOT is_pedestrian_street
    AND end_date >= '2023-01-01' AND start_date <= '2023-12-31'
)
SELECT SUM(LEAST(b.area * 100, 10000)) AS total_recomputed,
       COUNT(*)                        AS business_days,
       COUNT(DISTINCT b.business_id)   AS businesses
FROM days d
JOIN tlv.silver.business_street s ON s.street_code = d.street_code
JOIN tlv.silver.businesses b      ON b.business_id = s.business_id AND b.reject_reason IS NULL
WHERE b.founded_at IS NULL OR DATE(b.founded_at) <= d.day;

-- 2. Duplicates. Expected: every count is 0.
SELECT 'fact business-day'        AS what, COUNT(*) - COUNT(DISTINCT business_id, day) AS dups FROM tlv.gold.fact_business_day_2023
UNION ALL SELECT 'silver.businesses by id',  COUNT(*) - COUNT(DISTINCT business_id) FROM tlv.silver.businesses
UNION ALL SELECT 'business_street by id',    COUNT(*) - COUNT(DISTINCT business_id) FROM tlv.silver.business_street
UNION ALL SELECT 'closure_days by street-day', COUNT(*) - COUNT(DISTINCT street_code, day) FROM tlv.gold.closure_days_2023
UNION ALL SELECT 'silver.closures by event-side', COUNT(*) - COUNT(DISTINCT id_rechov, side) FROM tlv.silver.closures
UNION ALL SELECT 'rejected businesses (info)', COUNT(*) FROM tlv.silver.businesses WHERE reject_reason IS NOT NULL;

-- 3. Same physical business under several ids (same owner id + same address).
--    Expected: a few. Many means the source double-counts businesses.
SELECT COUNT(*) AS groups_with_more_than_one_id, SUM(n) - COUNT(*) AS extra_business_ids
FROM (SELECT owner_id, street_name, house_number, COUNT(DISTINCT business_id) AS n
      FROM tlv.silver.businesses
      GROUP BY owner_id, street_name, house_number
      HAVING n > 1);

-- 4. Area ("shetach") distribution. Expected: no zeros, no absurd values (a shop of 50,000 units is a data error).
SELECT COUNT(*) AS businesses,
       SUM(CASE WHEN area <= 0 THEN 1 END)      AS area_zero_or_negative,
       SUM(CASE WHEN area >= 100 THEN 1 END)    AS area_at_cap_or_above,
       SUM(CASE WHEN area >= 5000 THEN 1 END)   AS area_5000_plus,
       MIN(area), percentile(area, 0.5) AS median, percentile(area, 0.9) AS p90, MAX(area)
FROM tlv.silver.businesses;

-- 4b. The 10 largest areas, to see what they are.
SELECT business_id, business_type, street_name, house_number, owner_name, area
FROM tlv.silver.businesses ORDER BY area DESC LIMIT 10;

-- 5. Founding dates. Expected: no future dates, few nulls.
SELECT SUM(CASE WHEN founded_at IS NULL THEN 1 END)                 AS founded_null,
       SUM(CASE WHEN founded_at > current_timestamp() THEN 1 END)   AS founded_in_future,
       MIN(founded_at), MAX(founded_at)
FROM tlv.silver.businesses;

-- 6. Street matching quality. Expected: most by_name; by_point a minority; a few unmatched.
SELECT match_method, COUNT(*) AS businesses
FROM tlv.silver.business_street GROUP BY match_method;

-- 6b. Do the businesses on the biggest street really say "אבן גבירול" in the source? Expected: yes.
SELECT b.street_name AS source_street_name, s.match_method, COUNT(*) AS businesses
FROM tlv.gold.comp_by_business_2023 c
JOIN tlv.silver.businesses b      ON b.business_id = c.business_id
JOIN tlv.silver.business_street s ON s.business_id = c.business_id
WHERE c.street_name = 'אבן גבירול'
GROUP BY 1, 2 ORDER BY businesses DESC;

-- 7. Closures: days per event inside 2023. Expected: max 365; the long ones are light rail / infrastructure.
SELECT id_rechov, street_name, t_sug, start_date, end_date,
       datediff(least(end_date, DATE'2023-12-31'), greatest(start_date, DATE'2023-01-01')) + 1 AS days_in_2023
FROM tlv.silver.closures
WHERE reject_reason IS NULL AND NOT is_pedestrian_street AND end_date >= '2023-01-01' AND start_date <= '2023-12-31'
ORDER BY days_in_2023 DESC LIMIT 15;

-- 8. Pipeline balance. Expected: silver rows = distinct business ids in bronze (37,370).
SELECT (SELECT COUNT(*) FROM tlv.silver.businesses WHERE reject_reason IS NULL)     AS silver_valid,
       (SELECT COUNT(*) FROM tlv.silver.businesses WHERE reject_reason IS NOT NULL) AS silver_rejected,
       (SELECT COUNT(DISTINCT get_json_object(feature_json, '$.attributes.id_esek')) FROM tlv.bronze.businesses_raw) AS bronze_distinct_ids;

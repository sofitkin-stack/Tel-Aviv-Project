-- Dashboard queries over tlv.gold (2023 compensation). One query per tile.
-- Build in Databricks: Dashboards > Create dashboard > Data tab > paste each query as a dataset,
-- then add a widget per dataset on the canvas.

-- 1. KPI counters (3 counters: total_compensation_ils, compensated_businesses, share_of_days_at_cap)
SELECT metric, value
FROM tlv.gold.metrics_2023
WHERE metric IN ('total_compensation_ils', 'compensated_businesses', 'share_of_days_at_cap');

-- 2. Top 10 streets by compensation (bar chart: x = street_name, y = total_comp_ils)
SELECT street_name, total_comp_ils, businesses, closure_days
FROM tlv.gold.comp_by_street_2023
ORDER BY total_comp_ils DESC
LIMIT 10;

-- 3. Top 10 businesses by compensation (table)
SELECT business_id, business_type, street_name, house_number, area, compensated_days, total_comp_ils
FROM tlv.gold.comp_by_business_2023
ORDER BY total_comp_ils DESC
LIMIT 10;

-- 4. Compensation per month (line or bar: x = month, y = total_comp_ils)
SELECT date_trunc('month', day) AS month,
       SUM(daily_comp)          AS total_comp_ils,
       COUNT(DISTINCT business_id) AS businesses
FROM tlv.gold.fact_business_day_2023
GROUP BY 1
ORDER BY 1;

-- 5. Streets closed per day (line: x = day, y = streets_closed)
SELECT day, COUNT(DISTINCT street_code) AS streets_closed
FROM tlv.gold.closure_days_2023
GROUP BY day
ORDER BY day;

-- 6. Share of business-days at the daily cap, by business type (bar)
SELECT d.business_type,
       COUNT(*)                                   AS business_days,
       ROUND(AVG(CAST(f.hit_cap AS INT)), 3)      AS share_at_cap
FROM tlv.gold.fact_business_day_2023 f
JOIN tlv.gold.dim_business d USING (business_id)
GROUP BY d.business_type
ORDER BY business_days DESC
LIMIT 15;

-- all_businesses (table with the business filter)
SELECT business_id, owner_name, business_type, street_name, house_number, area, compensated_days, total_comp_ils
FROM tlv.gold.comp_by_business_2023 ORDER BY total_comp_ils DESC;

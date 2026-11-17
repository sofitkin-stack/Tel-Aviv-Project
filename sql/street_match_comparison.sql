-- Compare the two ways of resolving a business's street code: by street name vs. by nearest address (x/y, 50 m).
-- Run as one SQL cell. Expected: most businesses agree; "differ" should be a few percent at most.
WITH name_to_code AS (                      -- most common street code per normalized street name in the addresses layer
  SELECT street_key, street_code AS code_by_name
  FROM (SELECT street_key, street_code, COUNT(*) AS n,
               ROW_NUMBER() OVER (PARTITION BY street_key ORDER BY COUNT(*) DESC, street_code) AS rn
        FROM tlv.silver.addresses GROUP BY street_key, street_code)
  WHERE rn = 1
),
biz AS (
  SELECT business_id, street_key, x, y,
         CAST(FLOOR(x / 100) AS INT) AS gx, CAST(FLOOR(y / 100) AS INT) AS gy
  FROM tlv.silver.businesses WHERE reject_reason IS NULL AND x IS NOT NULL AND y IS NOT NULL
),
addr AS (
  SELECT street_code, x AS ax, y AS ay,
         CAST(FLOOR(x / 100) AS INT) AS gx, CAST(FLOOR(y / 100) AS INT) AS gy
  FROM tlv.silver.addresses WHERE x IS NOT NULL AND y IS NOT NULL
),
nearest AS (                                -- nearest address within 50 m, searching the 3x3 grid cells around the business
  SELECT business_id, street_code AS code_by_location, distance_m
  FROM (SELECT b.business_id, a.street_code,
               SQRT(POW(b.x - a.ax, 2) + POW(b.y - a.ay, 2)) AS distance_m,
               ROW_NUMBER() OVER (PARTITION BY b.business_id ORDER BY SQRT(POW(b.x - a.ax, 2) + POW(b.y - a.ay, 2))) AS rn
        FROM biz b
        JOIN addr a ON a.gx BETWEEN b.gx - 1 AND b.gx + 1 AND a.gy BETWEEN b.gy - 1 AND b.gy + 1)
  WHERE rn = 1 AND distance_m <= 50
),
both AS (
  SELECT b.business_id, n.code_by_name, l.code_by_location, l.distance_m
  FROM biz b
  LEFT JOIN name_to_code n ON n.street_key = b.street_key
  LEFT JOIN nearest l ON l.business_id = b.business_id
)
SELECT CASE
         WHEN code_by_name IS NULL AND code_by_location IS NULL THEN '4. neither'
         WHEN code_by_name IS NULL THEN '3. location only'
         WHEN code_by_location IS NULL THEN '2. name only'
         WHEN code_by_name = code_by_location THEN '1. agree'
         ELSE '5. differ'
       END AS result,
       COUNT(*) AS businesses,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
       ROUND(AVG(distance_m), 1) AS avg_distance_m
FROM both
GROUP BY 1 ORDER BY 1;

-- Second query (optional): 10 examples where the two methods differ, with both street names, to see which one is right.
-- Replace the final SELECT above with this one (keep the WITH part):
-- SELECT b.business_id, bz.street_name AS business_street, sn.street_name AS by_name_street,
--        sl.street_name AS by_location_street, ROUND(b.distance_m, 1) AS distance_m
-- FROM both b
-- JOIN tlv.silver.businesses bz ON bz.business_id = b.business_id
-- LEFT JOIN (SELECT street_code, MAX(street_name) AS street_name FROM tlv.silver.addresses GROUP BY street_code) sn ON sn.street_code = b.code_by_name
-- LEFT JOIN (SELECT street_code, MAX(street_name) AS street_name FROM tlv.silver.addresses GROUP BY street_code) sl ON sl.street_code = b.code_by_location
-- WHERE b.code_by_name <> b.code_by_location
-- LIMIT 10;

-- ============================================================
-- 03_analytics.sql — "Which routes are chronically slow?"
-- We proxy "delay" as trip duration / speed vs. the norm for
-- the same route + hour. All queries use window functions.
-- ============================================================

-- ------------------------------------------------------------
-- Q1. Average duration & speed by pickup hour (citywide rhythm)
--     + each hour's deviation from the daily average (window fn)
-- ------------------------------------------------------------
WITH hourly AS (
    SELECT
        EXTRACT(HOUR FROM pickup_ts) AS pickup_hour,
        COUNT(*)                     AS trips,
        ROUND(AVG(duration_min), 2)  AS avg_duration_min,
        ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph
    FROM clean.trips
    GROUP BY 1
)
SELECT
    pickup_hour,
    trips,
    avg_duration_min,
    avg_speed_mph,
    ROUND(avg_duration_min - AVG(avg_duration_min) OVER (), 2) AS delta_vs_daily_avg
FROM hourly
ORDER BY pickup_hour;

-- ------------------------------------------------------------
-- Q2. Chronically slow zones: rank pickup zones by median speed
--     within each hour bucket (PERCENTILE_CONT + RANK window)
-- ------------------------------------------------------------
WITH zone_hour AS (
    SELECT
        z.borough,
        z.zone,
        EXTRACT(HOUR FROM t.pickup_ts) AS pickup_hour,
        COUNT(*) AS trips,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.avg_speed_mph) AS median_speed
    FROM clean.trips t
    JOIN clean.zones z ON z.location_id = t.pu_location_id
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 100          -- statistical floor
)
SELECT *
FROM (
    SELECT
        pickup_hour,
        borough,
        zone,
        trips,
        ROUND(median_speed::NUMERIC, 2) AS median_speed_mph,
        RANK() OVER (PARTITION BY pickup_hour ORDER BY median_speed ASC) AS slowness_rank
    FROM zone_hour
) ranked
WHERE slowness_rank <= 5            -- 5 slowest zones per hour
ORDER BY pickup_hour, slowness_rank;

-- ------------------------------------------------------------
-- Q3. Route-level chronic lateness: for each OD pair, compare a
--     trip's duration to the route's typical (median) duration.
--     A route is "chronically late" if a large share of trips run
--     >25% over its own median.
-- ------------------------------------------------------------
-- Pattern: compute the median per route first, then join back
-- (PERCENTILE_CONT is an ordered-set aggregate, not a window function)
WITH route_median AS (
    SELECT
        pu_location_id,
        do_location_id,
        COUNT(*) AS trips,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_min) AS median_duration
    FROM clean.trips
    GROUP BY 1, 2
    HAVING COUNT(*) >= 200
),
flagged AS (
    SELECT
        t.pu_location_id,
        t.do_location_id,
        rm.trips,
        rm.median_duration,
        AVG(CASE WHEN t.duration_min > rm.median_duration * 1.25 THEN 1.0 ELSE 0.0 END)
            AS pct_over_125
    FROM clean.trips t
    JOIN route_median rm
      ON rm.pu_location_id = t.pu_location_id
     AND rm.do_location_id = t.do_location_id
    GROUP BY 1, 2, 3, 4
)
SELECT
    pz.zone AS pickup_zone,
    dz.zone AS dropoff_zone,
    f.trips,
    ROUND(f.median_duration::NUMERIC, 1) AS median_min,
    ROUND(100 * f.pct_over_125, 1)       AS pct_trips_25pct_over_median,
    DENSE_RANK() OVER (ORDER BY f.pct_over_125 DESC) AS chronic_rank
FROM flagged f
JOIN clean.zones pz ON pz.location_id = f.pu_location_id
JOIN clean.zones dz ON dz.location_id = f.do_location_id
ORDER BY chronic_rank
LIMIT 20;

-- ------------------------------------------------------------
-- Q4. Seasonal pattern: month-over-month avg duration with a
--     3-month moving average (frame-based window function)
-- ------------------------------------------------------------
SELECT
    DATE_TRUNC('month', pickup_ts)::DATE AS month,
    COUNT(*)                             AS trips,
    ROUND(AVG(duration_min), 2)          AS avg_duration_min,
    ROUND(AVG(AVG(duration_min)) OVER (
        ORDER BY DATE_TRUNC('month', pickup_ts)::DATE
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS moving_avg_3mo,
    ROUND(AVG(duration_min) - LAG(AVG(duration_min)) OVER (
        ORDER BY DATE_TRUNC('month', pickup_ts)::DATE
    ), 2) AS mom_change
FROM clean.trips
GROUP BY 1
ORDER BY 1;

-- metric_id: ads_retention_nd
-- 口径: cohort_日新增(first_seen)在 day+N 仍有行为的玩家 / cohort 规模; N in (1,3,7)
WITH cohorts AS (
    SELECT player_id, server_id, first_dt AS cohort_dt
    FROM dws.player_first_seen
),
activity AS (
    SELECT DISTINCT player_id, server_id, dt FROM dws.player_behavior_di
),
exploded AS (
    SELECT c.cohort_dt, c.server_id, c.player_id, n.n_days
    FROM cohorts c
    CROSS JOIN (SELECT 1 AS n_days UNION ALL SELECT 3 UNION ALL SELECT 7) n
)
SELECT
    e.cohort_dt,
    e.server_id,
    e.n_days,
    COUNT(DISTINCT e.player_id) AS cohort_size,
    COUNT(DISTINCT CASE WHEN a.player_id IS NOT NULL THEN e.player_id END) AS retained_cnt,
    COUNT(DISTINCT CASE WHEN a.player_id IS NOT NULL THEN e.player_id END) * 1.0
        / NULLIF(COUNT(DISTINCT e.player_id), 0) AS retention_rate,
    'ads_retention_nd' AS metric_id
FROM exploded e
LEFT JOIN activity a
  ON e.player_id = a.player_id AND e.server_id = a.server_id
 AND a.dt = e.cohort_dt + e.n_days
GROUP BY e.cohort_dt, e.server_id, e.n_days;

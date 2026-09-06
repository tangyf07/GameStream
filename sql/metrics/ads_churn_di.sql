-- metric_id: ads_churn_di
-- 口径: 过去7日有行为且过去3日无行为的玩家 / 过去7日有行为玩家
WITH days AS (
    SELECT DISTINCT dt, server_id FROM dws.player_behavior_di
),
active_7 AS (
    SELECT d.dt, d.server_id, p.player_id
    FROM days d
    JOIN dws.player_behavior_di p
      ON p.server_id = d.server_id AND p.dt BETWEEN d.dt - 6 AND d.dt
    GROUP BY d.dt, d.server_id, p.player_id
),
active_3 AS (
    SELECT d.dt, d.server_id, p.player_id
    FROM days d
    JOIN dws.player_behavior_di p
      ON p.server_id = d.server_id AND p.dt BETWEEN d.dt - 2 AND d.dt
    GROUP BY d.dt, d.server_id, p.player_id
)
SELECT
    a7.dt,
    a7.server_id,
    COUNT(DISTINCT a7.player_id) AS active_7d_users,
    COUNT(DISTINCT CASE WHEN a3.player_id IS NULL THEN a7.player_id END) AS churn_risk_users,
    COUNT(DISTINCT CASE WHEN a3.player_id IS NULL THEN a7.player_id END) * 1.0
        / NULLIF(COUNT(DISTINCT a7.player_id), 0) AS churn_risk_rate,
    'ads_churn_di' AS metric_id
FROM active_7 a7
LEFT JOIN active_3 a3
  ON a7.dt = a3.dt AND a7.server_id = a3.server_id AND a7.player_id = a3.player_id
GROUP BY a7.dt, a7.server_id;

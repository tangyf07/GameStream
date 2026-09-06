-- metric_id: ads_dau_di
-- 口径: 当日有任意有效行为事件的去重 player_id
SELECT
    dt,
    server_id,
    COUNT(DISTINCT player_id) AS dau,
    'ads_dau_di' AS metric_id
FROM dws.player_behavior_di
GROUP BY dt, server_id;

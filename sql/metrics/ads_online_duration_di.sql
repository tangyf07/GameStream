-- metric_id: ads_online_duration_di
-- 口径: SUM(logout.online_sec) / DAU；session 时长由模拟器 logout.payload.online_sec 提供
SELECT
    dt,
    server_id,
    SUM(online_sec_sum) AS total_online_sec,
    COUNT(DISTINCT player_id) AS players,
    SUM(online_sec_sum) * 1.0 / NULLIF(COUNT(DISTINCT player_id), 0) AS avg_online_sec,
    'ads_online_duration_di' AS metric_id
FROM dws.player_behavior_di
GROUP BY dt, server_id;

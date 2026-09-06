-- metric_id: ads_arpu_di
-- 口径: SUM(amount_fen)/100 / DAU ，单位 CNY
SELECT
    dt,
    server_id,
    COUNT(DISTINCT player_id) AS dau,
    SUM(recharge_fen) / 100.0 AS revenue_cny,
    (SUM(recharge_fen) / 100.0) / NULLIF(COUNT(DISTINCT player_id), 0) AS arpu_cny,
    'ads_arpu_di' AS metric_id
FROM dws.player_behavior_di
GROUP BY dt, server_id;

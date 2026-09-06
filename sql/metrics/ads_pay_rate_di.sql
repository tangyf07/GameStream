-- metric_id: ads_pay_rate_di
-- 口径: 当日有 recharge 的去重玩家 / DAU
SELECT
    dt,
    server_id,
    COUNT(DISTINCT player_id) AS dau,
    COUNT(DISTINCT CASE WHEN recharge_cnt > 0 THEN player_id END) AS pay_users,
    COUNT(DISTINCT CASE WHEN recharge_cnt > 0 THEN player_id END) * 1.0
        / NULLIF(COUNT(DISTINCT player_id), 0) AS pay_rate,
    'ads_pay_rate_di' AS metric_id
FROM dws.player_behavior_di
GROUP BY dt, server_id;

-- metric_id: ads_dungeon_clear_rate_di
-- 口径: clear_dungeon 次数 / enter_dungeon 次数
SELECT
    dt,
    server_id,
    dungeon_id,
    enter_cnt,
    clear_cnt,
    clear_cnt * 1.0 / NULLIF(enter_cnt, 0) AS clear_rate,
    'ads_dungeon_clear_rate_di' AS metric_id
FROM dws.dungeon_behavior_di;

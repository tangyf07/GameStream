-- DWS: player-day / dungeon-day aggregates

CREATE SCHEMA IF NOT EXISTS dws;

CREATE TABLE IF NOT EXISTS dws.player_behavior_di (
    dt                  DATE,
    server_id           INTEGER,
    player_id           BIGINT,
    event_cnt           BIGINT,
    session_cnt         BIGINT,
    login_cnt           BIGINT,
    logout_cnt          BIGINT,
    enter_dungeon_cnt   BIGINT,
    clear_dungeon_cnt   BIGINT,
    death_cnt           BIGINT,
    recharge_cnt        BIGINT,
    recharge_fen        BIGINT,
    online_sec_sum      BIGINT,
    first_event_time    TIMESTAMP,
    last_event_time     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dws.dungeon_behavior_di (
    dt          DATE,
    server_id   INTEGER,
    dungeon_id  INTEGER,
    enter_cnt   BIGINT,
    clear_cnt   BIGINT,
    death_cnt   BIGINT
);

CREATE TABLE IF NOT EXISTS dws.player_first_seen (
    player_id         BIGINT,
    server_id         INTEGER,
    first_dt          DATE,
    first_event_time  TIMESTAMP
);

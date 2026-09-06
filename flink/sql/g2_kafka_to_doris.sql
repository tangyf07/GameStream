-- =============================================================================
-- GameStream G2: Kafka ODS → Flink (batch, bounded) → Doris ADS (JDBC)
-- Sink choice: JDBC to doris-fe:9030 (MySQL protocol). Flink does the transform;
-- Doris UNIQUE KEY REPLACE handles re-runs. Avoids heavier flink-doris-connector.
-- Run AFTER events are produced. Bounded Kafka source stops at latest offsets.
-- =============================================================================

SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';

CREATE TABLE kafka_ods_player_events (
    event_id      STRING,
    event_type    STRING,
    event_time    TIMESTAMP(3),
    player_id     BIGINT,
    role_id       BIGINT,
    server_id     INT,
    session_id    STRING,
    payload       ROW<
        dungeon_id INT,
        amount_fen BIGINT,
        online_sec INT,
        client_version STRING,
        device_os STRING,
        channel STRING
    >
) WITH (
    'connector' = 'kafka',
    'topic' = 'gamestream.ods.player_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'gamestream-g2-ads',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'latest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

CREATE VIEW v_ods_clean AS
SELECT
    event_id,
    event_type,
    event_time,
    CAST(event_time AS DATE) AS dt,
    player_id,
    role_id,
    server_id,
    session_id,
    payload.dungeon_id AS dungeon_id,
    payload.amount_fen AS amount_fen
FROM kafka_ods_player_events
WHERE event_id IS NOT NULL
  AND player_id IS NOT NULL
  AND player_id > 0
  AND event_type IN (
      'login','create_role','enter_dungeon','clear_dungeon','death',
      'equip','enhance','recharge','gacha','friend','logout'
  );

-- Doris JDBC sinks (append in batch mode; UNIQUE KEY REPLACE on re-insert)
CREATE TABLE ads_dau_di_jdbc (
    dt         DATE,
    server_id  INT,
    dau        BIGINT,
    metric_id  STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://doris-fe:9030/ads?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'ads_dau_di',
    'username' = 'root',
    'password' = '',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '5'
);

CREATE TABLE ads_pay_rate_di_jdbc (
    dt         DATE,
    server_id  INT,
    dau        BIGINT,
    pay_users  BIGINT,
    pay_rate   DOUBLE,
    metric_id  STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://doris-fe:9030/ads?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'ads_pay_rate_di',
    'username' = 'root',
    'password' = '',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '5'
);

BEGIN STATEMENT SET;

INSERT INTO ads_dau_di_jdbc
SELECT
    dt,
    server_id,
    COUNT(DISTINCT player_id) AS dau,
    CAST('ads_dau_di' AS STRING) AS metric_id
FROM v_ods_clean
GROUP BY dt, server_id;

INSERT INTO ads_pay_rate_di_jdbc
SELECT
    dt,
    server_id,
    COUNT(DISTINCT player_id) AS dau,
    COUNT(DISTINCT CASE WHEN event_type = 'recharge' THEN player_id END) AS pay_users,
    CAST(COUNT(DISTINCT CASE WHEN event_type = 'recharge' THEN player_id END) AS DOUBLE)
        / NULLIF(COUNT(DISTINCT player_id), 0) AS pay_rate,
    CAST('ads_pay_rate_di' AS STRING) AS metric_id
FROM v_ods_clean
GROUP BY dt, server_id;

END;

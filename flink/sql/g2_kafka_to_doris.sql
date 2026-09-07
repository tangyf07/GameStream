-- GameStream G2: Kafka ODS → Flink batch bounded → Doris ADS (JDBC)
-- Fixes: event_time as STRING (avoid ISO-Z ROW parse drops); payload as STRING;
--        TABLEAU mode; explicit INSERT jobs (not only STATEMENT SET).

SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE TABLE kafka_ods_player_events (
    event_id      STRING,
    event_type    STRING,
    event_time    STRING,
    player_id     BIGINT,
    role_id       BIGINT,
    server_id     INT,
    session_id    STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'gamestream.ods.player_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'gamestream-g2-ads-v2',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE VIEW v_ods_clean AS
SELECT
    event_id,
    event_type,
    CAST(TO_TIMESTAMP(REPLACE(REPLACE(event_time, 'T', ' '), 'Z', '')) AS DATE) AS dt,
    player_id,
    server_id
FROM kafka_ods_player_events
WHERE event_id IS NOT NULL
  AND player_id IS NOT NULL
  AND player_id > 0
  AND event_time IS NOT NULL
  AND event_type IN (
      'login','create_role','enter_dungeon','clear_dungeon','death',
      'equip','enhance','recharge','gacha','friend','logout'
  );

CREATE TABLE ads_dau_di_jdbc (
    dt         DATE,
    server_id  INT,
    dau        BIGINT,
    metric_id  STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://doris-fe:9030/ads?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&connectTimeout=10000&socketTimeout=60000',
    'table-name' = 'ads_dau_di',
    'username' = 'root',
    'password' = '',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '50',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '8'
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
    'url' = 'jdbc:mysql://doris-fe:9030/ads?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&connectTimeout=10000&socketTimeout=60000',
    'table-name' = 'ads_pay_rate_di',
    'username' = 'root',
    'password' = '',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '50',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '8'
);

-- Debug count (visible in sql-client -f with TABLEAU)
SELECT COUNT(*) AS kafka_cnt FROM kafka_ods_player_events;

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

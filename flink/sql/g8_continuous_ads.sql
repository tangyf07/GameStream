-- =============================================================================
-- GameStream G8: Continuous Doris ADS mainline (streaming)
-- Unifies G3 watermark/dedup + G4 checkpoint + G5 restart into ONE job.
--
-- Sink honesty (important):
--   Flink JDBC MySQL upsert dialect emits `INSERT ... ON DUPLICATE KEY UPDATE`,
--   which Doris FE rejects. Continuous Flink sink is therefore upsert-kafka
--   (ALS + PK). Doris UNIQUE KEY visibility is provided by the resident
--   materializer (pipeline/doris_ads_materializer.py). Script-phase
--   materialize_doris remains fallback/dev only. NOT end-to-end EO-2PC.
--
-- Pipeline:
--   Kafka ODS → clean → Rank(event_id) → unbounded daily GROUP BY
--     → upsert-kafka gamestream.g8.ads_dau / gamestream.g8.ads_pay_rate
--   resident materializer → ads.ads_dau_di / ads.ads_pay_rate_di
-- =============================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'pipeline.name' = 'g8-continuous-ads';

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause-between-checkpoints' = '5s';
SET 'execution.checkpointing.timeout' = '2min';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.num-retained' = '3';
SET 'state.backend.type' = 'hashmap';
SET 'state.checkpoints.dir' = 'file:///checkpoints';

SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '10';
SET 'restart-strategy.fixed-delay.delay' = '5s';

SET 'table.exec.source.idle-timeout' = '5 s';
SET 'table.exec.state.ttl' = '1 d';

CREATE TABLE kafka_g8_ods (
    event_id      STRING,
    event_type    STRING,
    event_time    TIMESTAMP(3),
    player_id     BIGINT,
    server_id     INT,
    tag           STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'gamestream.g8.ods.events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'gamestream-g8-ads-v1',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.timestamp-format.standard' = 'SQL'
);

CREATE VIEW v_g8_clean_dedup AS
SELECT event_id, event_type, event_time, player_id, server_id, tag
FROM (
    SELECT
        e.*,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time ASC) AS rn
    FROM kafka_g8_ods e
    WHERE event_id IS NOT NULL
      AND player_id IS NOT NULL
      AND player_id > 0
      AND event_time IS NOT NULL
      AND event_type IN (
          'login','create_role','enter_dungeon','clear_dungeon','death',
          'equip','enhance','recharge','gacha','friend','logout'
      )
) t
WHERE rn = 1;

-- Continuous ADS stream (upsert by dt,server_id)
CREATE TABLE kafka_g8_ads_dau (
    dt         DATE,
    server_id  INT,
    dau        BIGINT,
    metric_id  STRING,
    PRIMARY KEY (dt, server_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.g8.ads_dau',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE kafka_g8_ads_pay_rate (
    dt         DATE,
    server_id  INT,
    dau        BIGINT,
    pay_users  BIGINT,
    pay_rate   DOUBLE,
    metric_id  STRING,
    PRIMARY KEY (dt, server_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.g8.ads_pay_rate',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.json.timestamp-format.standard' = 'SQL'
);

BEGIN STATEMENT SET;

INSERT INTO kafka_g8_ads_dau
SELECT
    CAST(event_time AS DATE) AS dt,
    server_id,
    CAST(COUNT(DISTINCT player_id) AS BIGINT) AS dau,
    CAST('ads_dau_di' AS STRING) AS metric_id
FROM v_g8_clean_dedup
GROUP BY CAST(event_time AS DATE), server_id;

INSERT INTO kafka_g8_ads_pay_rate
SELECT
    CAST(event_time AS DATE) AS dt,
    server_id,
    CAST(COUNT(DISTINCT player_id) AS BIGINT) AS dau,
    CAST(COUNT(DISTINCT CASE WHEN event_type = 'recharge' THEN player_id END) AS BIGINT) AS pay_users,
    CAST(COUNT(DISTINCT CASE WHEN event_type = 'recharge' THEN player_id END) AS DOUBLE)
        / NULLIF(COUNT(DISTINCT player_id), 0) AS pay_rate,
    CAST('ads_pay_rate_di' AS STRING) AS metric_id
FROM v_g8_clean_dedup
GROUP BY CAST(event_time AS DATE), server_id;

END;

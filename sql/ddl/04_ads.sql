-- ADS: application metrics for dashboard / DataPilot

CREATE SCHEMA IF NOT EXISTS ads;

CREATE TABLE IF NOT EXISTS ads.ads_dau_di (
    dt          DATE,
    server_id   INTEGER,
    dau         BIGINT,
    metric_id   VARCHAR
);

CREATE TABLE IF NOT EXISTS ads.ads_retention_nd (
    cohort_dt       DATE,
    server_id       INTEGER,
    n_days          INTEGER,
    cohort_size     BIGINT,
    retained_cnt    BIGINT,
    retention_rate  DOUBLE,
    metric_id       VARCHAR
);

CREATE TABLE IF NOT EXISTS ads.ads_online_duration_di (
    dt                DATE,
    server_id         INTEGER,
    total_online_sec  BIGINT,
    players           BIGINT,
    avg_online_sec    DOUBLE,
    metric_id         VARCHAR
);

CREATE TABLE IF NOT EXISTS ads.ads_pay_rate_di (
    dt          DATE,
    server_id   INTEGER,
    dau         BIGINT,
    pay_users   BIGINT,
    pay_rate    DOUBLE,
    metric_id   VARCHAR
);

CREATE TABLE IF NOT EXISTS ads.ads_arpu_di (
    dt           DATE,
    server_id    INTEGER,
    dau          BIGINT,
    revenue_cny  DOUBLE,
    arpu_cny     DOUBLE,
    metric_id    VARCHAR
);

CREATE TABLE IF NOT EXISTS ads.ads_dungeon_clear_rate_di (
    dt          DATE,
    server_id   INTEGER,
    dungeon_id  INTEGER,
    enter_cnt   BIGINT,
    clear_cnt   BIGINT,
    clear_rate  DOUBLE,
    metric_id   VARCHAR
);

CREATE TABLE IF NOT EXISTS ads.ads_churn_di (
    dt                DATE,
    server_id         INTEGER,
    active_7d_users   BIGINT,
    churn_risk_users  BIGINT,
    churn_risk_rate   DOUBLE,
    metric_id         VARCHAR
);

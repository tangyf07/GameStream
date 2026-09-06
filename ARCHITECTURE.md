# GameStream Architecture (short)

## Streams

- **ODS topic**: `gamestream.ods.player_events`
- **DWD topic**: `gamestream.dwd.player_events_clean`
- **DWS topic**: `gamestream.dws.player_behavior_di`
- **ADS stream**: `gamestream.ads.metrics_stream`

Lite: same names as folders under `data/topics/` via `pipeline/kafka_io.FileTopic`.

## Processing

| Stage | Prod | Lite |
|-------|------|------|
| Ingest | Kafka | JSONL + file topic |
| Clean | Flink SQL `01_ods_clean.sql` | DuckDB `step_ods_dwd` |
| Agg | Flink day tumble + Spark backfill | DuckDB `step_dws` / `step_ads` |
| Serve | Doris | DuckDB + Parquet |

## Why DuckDB locally

No Docker on target Windows laptop; DuckDB embeds OLAP SQL close enough to demonstrate ODS→ADS and the same metric SQL text under `sql/metrics/`.

## Deep dive

Kafka partitions/CG/semantics, Flink watermark/state/checkpoint, skew, late data → `docs/flink-kafka-deep-dive.md`.  
Doris ADS DDL → `sql/ddl/doris_ads.sql`. Iceberg → `sql/ddl/iceberg_notes.md`.

# Spark Batch Jobs

| Script | Role |
|--------|------|
| `dws_ads_batch.py` | Read DWD parquet → player-day DWS + DAU ADS only |

```bash
# after local_runner produced data/dwd/*.parquet
python spark/jobs/dws_ads_batch.py
# or
spark-submit spark/jobs/dws_ads_batch.py --dwd data/dwd/player_events_clean.parquet
```

If PySpark is not installed, the script exits 0 with a soft skip message —
DuckDB lite path already materializes the same tables.

**Not in this job:** retention / churn (see `sql/metrics/ads_retention_nd.sql`,
`sql/metrics/ads_churn_di.sql`, and DuckDB `pipeline/local_runner.py`).

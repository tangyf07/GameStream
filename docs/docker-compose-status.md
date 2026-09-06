# docker compose 状态（如实）

目标命令：`docker compose --profile full up`（见根目录 `docker-compose.yml`：Redpanda + Flink + 参考 OLAP）。

## 尝试记录

| 环境 | 日期 (UTC+8) | Docker | 结果 |
|------|--------------|--------|------|
| 构建机（Cursor box） | 2026-09-06 | **未安装**（`docker: command not found`） | **未执行** compose |
| 用户机 tangyf | 2026-09-06 | **未安装**（PowerShell：`docker: not found`） | **未执行** compose |

因此：**不能声称 Kafka/Flink/Doris 集群已在本地跑通**。生产参考以 `flink/`、`sql/ddl/doris_ads.sql`、`docker-compose.yml` 代码为准；可演示路径是 DuckDB lite。

有 Docker 后建议：

```bash
docker compose --profile full up -d
# 将实际日志摘录追加到本文件「成功/失败」节；失败也写错误原文，勿改口称为已上线
```

Kafka Lag / Flink Checkpoint 耗时 / 端到端 P95：**仅在有真实集群实测后再写入** `bench/results/`，禁止编造。

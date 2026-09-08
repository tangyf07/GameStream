# Golden Path 演示（6 步）

面向 WSL + Docker 现场演示。只证明一条链路、一个指标：

**事件 → Kafka → Flink → `ads_dau_di` → Doris**

技术骨干是已验证的 G2（`scripts/e2e_g2.sh` · `docs/e2e-g2.md`）。不引入新 pipeline / 新 metric。

前置：在 WSL 里 `cd` 到仓库（例：`/mnt/c/Users/tangy/source/repos/GameStream`）。脚本若从 `/mnt/c` 直接跑出怪错，先拷到 `/tmp` 并去 CRLF（见文末坑）。

---

## Step 1 — 起栈

```bash
docker compose up -d
```

看：`docker compose ps` 里 kafka / flink JM+TM / doris-fe / doris-be 为 Up（或 healthy）。

成功信号：无持续 Restarting；`docker compose ps` 核心服务在跑。

---

## Step 2 — 确认端口与 BE

```bash
# Kafka EXTERNAL
ss -lntp | grep 19092 || true

# Flink UI
curl -sf http://127.0.0.1:8081/overview

# Doris MySQL 协议 + BE Alive
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G"
```

看：Flink overview JSON；`SHOW BACKENDS` 里 `Alive` = `true`。

成功信号：`:19092` 在听；`:8081` 可 curl；`:9030` 可连；**BE Alive**（仅 FE 健康不够）。

---

## Step 3 — 灌事件（或整段 e2e）

推荐一键（含 clear ADS + produce + 提交 Flink + 查 Doris）：

```bash
cp scripts/e2e_g2.sh /tmp/e2e_g2.sh && sed -i 's/\r$//' /tmp/e2e_g2.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream bash /tmp/e2e_g2.sh
# 可选：PLAYERS=500 EVENTS=3000 DAYS=1
```

薄封装（同上）：`bash scripts/demo_golden_path.sh`（内部仍调 `e2e_g2.sh`）。

脚本内：先 **TRUNCATE/DELETE** demo ADS，再删/建 topic `gamestream.ods.player_events` → `simulator/generate_events.py` → `kafka-console-producer`。**证明来自本轮**，不依赖旧行。

看：日志 `[g2] publishing N lines to gamestream.ods.player_events`；`[g2] demo ADS tables cleared`。

成功信号：N ≈ `EVENTS`（默认 3000）；**先 produce，再 submit**（下一步）。

---

## Step 4 — Flink 作业 `g2_kafka_to_doris.sql`

`e2e_g2.sh` **已经**用 sql-client `-f /opt/flink/sql/g2_kafka_to_doris.sql` 提交（batch + bounded Kafka → JDBC Doris）。现场若只演示 submit：

```bash
docker exec gs-flink-jm /opt/flink/bin/sql-client.sh \
  -j /opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar \
  -j /opt/flink/usrlib/flink-connector-jdbc-3.1.2-1.17.jar \
  -j /opt/flink/usrlib/mysql-connector-j-8.0.33.jar \
  -f /opt/flink/sql/g2_kafka_to_doris.sql
```

链路：Kafka ODS → `v_ods_clean`（清洗）→ 聚合写入 `ads.ads_dau_di`（作业里还有 `ads_pay_rate_di`，本演示只验收 DAU）。

看：Flink UI `http://127.0.0.1:8081`；sql-client 退出码；日志 `[g2] sql-client exit=0`。

成功信号：作业跑完（bounded）；**exit=0**（`e2e_g2.sh` 在 RC≠0 时 **立即非零退出**，不假 PASS）；无 jar/classpath 报错（`docker exec` 必须带 `-j`）。

---

## Step 5 — Doris 查一个指标

```bash
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
  "SELECT dt, server_id, dau, metric_id FROM ads.ads_dau_di WHERE metric_id='ads_dau_di' AND dau>0 ORDER BY dt, server_id;"
```

看：每行 `metric_id` 与 `dau`。

成功信号：至少 1 行；**`metric_id=ads_dau_di` 且 `dau>0`**（不要只靠无过滤的 `COUNT(*)>0`，避免旧数据假绿）。

---

## Step 6 — 对照已提交结果

对照文件：[`docs/e2e-g2-query-result.txt`](e2e-g2-query-result.txt)

```text
dt          server_id  dau  metric_id
...                     ... ads_dau_di
```

看：结构同结果文件；`metric_id` 全为 `ads_dau_di`；`dau>0`。行数/具体 dau 随 `PLAYERS/EVENTS/DAYS/日期` 会变，不要求字节级一致。

成功信号：本轮查询非空且 metric 名正确；与结果文件同形态即可。

---

## 常见坑（摘自 e2e-g2.md）

1. **CRLF / `/mnt/c`**：脚本先 `cp` 到 `/tmp` 再 `sed -i 's/\r$//'`；Windows 侧用 `wsl -e bash ...`。
2. **`replication_num=1`**：单 BE 必须用 G2 DDL（`sql/ddl/doris_ads_g2.sql`）；默认 3 副本会建表失败。
3. **先 produce 再 submit**：bounded `latest-offset` 在作业启动时截断；晚灌的事件本轮看不见。
4. **BE Alive**：`SHOW BACKENDS`，FE 起来不等于能写。
5. **sql-client classpath**：`docker exec` 不走 entrypoint；必须 `-j` 三个 jar。
6. **假 PASS**：sql-client RC≠0 必须失败；验收用 `metric_id=ads_dau_di AND dau>0`，并在跑前清空 demo ADS。
7. **内存**：笔记本小流量（默认 3k events）；勿同机再拉 Spark/K8s。
8. **Flink jars 挂载**：改 compose 后可能需 `docker compose up -d --force-recreate jobmanager taskmanager`。

细节与拓扑：[`e2e-g2.md`](e2e-g2.md)。

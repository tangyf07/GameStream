# Golden Path 演示（6 步）

面向 WSL + Docker 现场演示。只证明一条链路、一个指标：

**事件 → Kafka → Flink → `ads_dau_di` → Doris**

六节拍固定：**正常 → 重复 → 迟到 → kill TM → 恢复 → 对 DAU**。映射到仓库**已有**脚本，不引入新 pipeline / 新 metric。

前置：在 WSL 里 `cd` 到仓库（例：`/mnt/c/Users/tangy/source/repos/GameStream`）。

```bash
docker compose up -d
# 核心服务 Up；BE Alive：docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G"
```

脚本若从 `/mnt/c` 直接跑出怪错，先拷到 `/tmp` 并去 CRLF（见文末坑）。

---

## 1. 正常 — G2 E2E（骨干）

一键：produce → Flink `g2_kafka_to_doris.sql` → 写 Doris ADS。

```bash
cp scripts/e2e_g2.sh /tmp/e2e_g2.sh && sed -i 's/\r$//' /tmp/e2e_g2.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream bash /tmp/e2e_g2.sh
# 薄封装：bash scripts/demo_golden_path.sh
# 可选：PLAYERS=500 EVENTS=3000 DAYS=1
```

看：`[g2] demo ADS tables cleared`；`[g2] publishing N lines`；`[g2] sql-client exit=0`。

成功信号：N ≈ `EVENTS`（默认 3000）；sql-client **exit=0**（RC≠0 立即失败，不假 PASS）。细节：[`e2e-g2.md`](e2e-g2.md)。

---

## 2. 重复 — G3 `event_id` 去重（可选演练）

同一次 G3 脚本里重放 `g3-dup-01`，窗口 `COUNT(DISTINCT event_id)` 不双计。

```bash
cp scripts/g3_stream_semantics.sh /tmp/g3.sh && sed -i 's/\r$//' /tmp/g3.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream bash /tmp/g3.sh
```

成功信号：W1 `event_cnt=7`（不是 8）→ dup 只计一次。细节：[`g3-stream-semantics.md`](g3-stream-semantics.md)。生产 ODS 模式见 `flink/sql/01_ods_clean.sql`（upsert-kafka + `event_id`）。

> 演示时间紧可只口述「G2 链路已跑通 + event_id 去重语义在 G3」，再链到文档。

---

## 3. 迟到 — G3 watermark 丢弃（可选，同 G3）

与上步**同一脚本**：相位灌数后 `g3-late-01` 在窗口关闭后到达 → Flink SQL 默认 drop。

成功信号：W1 `player_cnt=7`（不是 8）→ 迟到未进关闭窗口。政策：watermark 后丢弃；纯 SQL **无** side output。细节同上 G3 文档。

---

## 4. kill TM — G5 故障演练（可选）

真杀 TaskManager（JM / Kafka / Doris 保持）：

```bash
docker compose up -d --force-recreate jobmanager taskmanager
cp scripts/g5_fault_drill.sh /tmp/g5.sh && sed -i 's/\r$//' /tmp/g5.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream bash /tmp/g5.sh
```

成功信号：脚本内出现 `docker kill gs-flink-tm`；Flink UI 同 job 进入 RESTARTING。细节：[`g5-fault-drill.md`](g5-fault-drill.md)。（G4 是 cancel→手动 restore；G5 才是 kill TM + restart-strategy。）

---

## 5. 恢复 — G5 checkpoint 拉回（可选，同 G5）

TM 容器 `unless-stopped` 拉起后，`fixed-delay` restart-strategy 从最近完成 checkpoint 恢复；再灌重复 `event_id` → Rank 吸收，金额/计数不双计。

成功信号：同 job 回到 RUNNING；重复 id 不抬计数；新唯一 id 只 +1。**不**宣称端到端 EO-2PC（ALS + upsert/UNIQUE KEY）。

---

## 6. 对 DAU — 查 `ads_dau_di`（骨干，接回 G2）

```bash
docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e \
  "SELECT dt, server_id, dau, metric_id FROM ads.ads_dau_di WHERE metric_id='ads_dau_di' AND dau>0 ORDER BY dt, server_id;"
```

对照：[`e2e-g2-query-result.txt`](e2e-g2-query-result.txt)。

成功信号：至少 1 行；**`metric_id=ads_dau_di` 且 `dau>0`**（勿只用无过滤 `COUNT(*)>0`）。行数/具体 dau 随 `PLAYERS/EVENTS/DAYS` 可变，同形态即可。

---

## 常见坑（摘自 e2e-g2.md）

1. **CRLF / `/mnt/c`**：脚本先 `cp` 到 `/tmp` 再 `sed -i 's/\r$//'`；Windows 侧用 `wsl -e bash ...`。
2. **`replication_num=1`**：单 BE 必须用 G2 DDL（`sql/ddl/doris_ads_g2.sql`）；默认 3 副本会建表失败。
3. **先 produce 再 submit**：bounded `latest-offset` 在作业启动时截断；晚灌的事件本轮看不见。
4. **BE Alive**：`SHOW BACKENDS`，FE 起来不等于能写。
5. **sql-client classpath**：`docker exec` 不走 entrypoint；必须 `-j` 三个 jar。
6. **假 PASS**：sql-client RC≠0 必须失败；验收用 `metric_id=ads_dau_di AND dau>0`，并在跑前清空 demo ADS。
7. **内存**：笔记本小流量（默认 3k events）；G3/G5 可选演练勿与大压测同机硬刚。
8. **Flink jars 挂载**：改 compose 后可能需 `docker compose up -d --force-recreate jobmanager taskmanager`。

细节与拓扑：[`e2e-g2.md`](e2e-g2.md) · [`g3-stream-semantics.md`](g3-stream-semantics.md) · [`g5-fault-drill.md`](g5-fault-drill.md)。

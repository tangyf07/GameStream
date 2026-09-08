# docker compose 状态（如实）

目标：`docker compose up -d` 拉起 **Kafka + Flink JM/TM + Doris FE/BE**（G1 范围，不含 Spark/Iceberg/监控）。

## 实测记录（WSL2 Ubuntu @ tangyf）

| 项 | 值 |
|--|--|
| 日期 | 2026-09-07（UTC+8） |
| Docker | Engine **29.7.2** · Compose **v5.5.0** |
| 宿主内存 | ~**7.6Gi**（起栈后 available ~3.6Gi） |
| 仓库路径 | `<repo-root>` |
| 镜像源 | daemon.json mirrors：daocloud / 1ms.run / tencent |

### 服务结果（`docker compose up -d` 后探测）

| 服务 | 镜像 | 状态 | 访问 |
|--|--|--|--|
| kafka | `apache/kafka:3.7.0` | **healthy** | 容器内 `localhost:9092`；宿主机 `localhost:19092` |
| jobmanager | `flink:1.18-scala_2.12-java17` | **healthy** | http://localhost:8081 （overview 返回 taskmanagers=1） |
| taskmanager | 同上 | **running** | slots-total=2 |
| doris-fe | `apache/doris:fe-3.0.4` | **healthy** | http://localhost:8030 → 200；MySQL `9030` |
| doris-be | `apache/doris:be-3.0.4` | **running / Alive=true** | http://localhost:8040 → 200；已 `SHOW BACKENDS` 注册 |

### 资源与配置取舍

- Kafka mem_limit **768m**；Flink JM **900m** / TM **1200m**；单 TM、slots=2。
- Doris **去掉 mem_limit**（WSL2 cgroupv2 + BDBJE 会 NPE）；`JAVA_TOOL_OPTIONS=-XX:-UseContainerSupport`（FE 另加 `-Xmx1024m`）。
- Doris 固定 IP：`fe=172.28.1.10` / `be=172.28.1.11`（entrypoint 校验要 IPv4；网段避免 `*.0.*` 零八位坑）。
- **未使用** Redpanda：`docker.redpanda.com` 解析到 Docker Hub 后超时；Bitnami Kafka mirror **denied**；改用 Hub 可拉的 `apache/kafka:3.7.0`（仍为 Kafka API，Flink SQL `bootstrap.servers=kafka:9092` 不变）。

### 已知问题

- 本机内存紧：起满栈后 free 很低，勿再叠加重型容器。
- **G1 只验证组件可起与端口可达**；Flink Job 提交 / 端到端写入 Doris 属 **G2**，此处不宣称 E2E。
- 无 Kafka Lag / Flink Checkpoint / P95 实测数字（未跑压测，不编造）。

### 复现

```bash
cd "$PWD"  # 仓库根目录
docker compose up -d
docker compose ps
curl -s http://127.0.0.1:8081/overview
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8030/
```

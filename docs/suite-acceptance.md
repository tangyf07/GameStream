# 三仓固化验收（Suite Acceptance，NO G8）

目标：把 **GameStream + SQLGuard(sql-write-gate) + DataPilot** 钉在可复跑的 SHA 上，用一条脚本给出 **PASS / FAIL / SKIP / mock** 标签，并写明 **执行后端**（`doris` vs `mock` / `offline-pytest`）。  
**不实现 G8。不编造指标或 pass/fail。**

相关：[`g7-closed-loop.md`](g7-closed-loop.md) · [`versions.lock`](../versions.lock) · 最新报告 [`suite-acceptance-result.txt`](suite-acceptance-result.txt)

## 钉死 SHA（`versions.lock`）

| 仓 | Pin | 说明 |
|----|-----|------|
| **GameStream** | `2253b25`（或更新的 main） | G7 strict 基线；本验收交付落在此之上（落地后会有新 SHA） |
| **SQLGuard**（sql-write-gate） | `7dc85dd` → **v1.1.2** | `make test`；healthz `1.1.2`；继承 1.1.1 限定名身份（`hive.ads_dau_di` BLOCK）；路由 `/v1/check` `/v1/block` `/v1/execute` |
| **DataPilot** | `e3e603d` | `pytest -m suite_p0`；**行为基线含 `285202c` P0**（no-mock-fallback / 时间谓词 / HTTP 契约） |

路径（WSL 典型）：

```
/mnt/c/Users/tangy/source/repos/{GameStream,sql-write-gate,DataPilot}
```

## 怎么跑

```bash
# CRLF-safe（WSL）
cp scripts/suite_acceptance.sh /tmp/suite_acc.sh && sed -i 's/\r$//' /tmp/suite_acc.sh
GAMESTREAM_ROOT=/mnt/c/Users/tangy/source/repos/GameStream \
  SQLGUARD_REPO=/mnt/c/Users/tangy/source/repos/sql-write-gate \
  DATAPILOT_REPO=/mnt/c/Users/tangy/source/repos/DataPilot \
  bash /tmp/suite_acc.sh
# 报告：docs/suite-acceptance-result.txt
```

前置建议：

1. Doris ADS 有行（`mysql://root@127.0.0.1:9030/ads`）；空则先 `bash scripts/e2e_g2.sh`
2. SQLGuard：`git checkout 7dc85dd`；`~/g7-venv` 或 `pip install -e '.[mysql]'`；healthz `version=1.1.2`
3. DataPilot：`git checkout e3e603d && pip install -e ".[dev]"`

## 检查项与状态语义

| ID | 做什么 | PASS | FAIL | SKIP | mock |
|----|--------|------|------|------|------|
| **a) `g7_closed_loop`** | `MODE=fixture bash scripts/g7_closed_loop.sh` | 退出 0；真实 rowcount / gate 摘自结果文件 | 非 0（含 ADS empty） | 脚本缺失 | 仅当结果明确走 mock（本验收期望 **doris**） |
| **b) `sqlguard_cross_db_hive`** | `hive.ads_dau_di` 必须 **BLOCK**（1.1.2） | healthz=1.1.2 且 datapilot/action=BLOCK | 版本不对或未 BLOCK | serve 不可用且无 G7 证据 | — |
| **c) `datapilot_offline_p0`** | 优先 `pytest -m suite_p0 -q`（`e3e603d`）；否则三文件等价入口 | pytest exit 0 | 非 0 | clone 缺失 | **offline-pytest**（≠ Doris 集成） |
| **c2) `datapilot_doris_g7`** | 可选 `pytest tests/test_doris_g7.py` | 真连 Doris 且绿 | 联调失败 | Doris 不可达 → **SKIP（勿当 PASS）** | — |
| **d) `sqlguard_unit`** | `make test`（或 `pytest -q`）；可选 `tests/test_v110.py`；`gh run list` 实录 | 真实绿（live PG/MySQL SKIP 可接受） | 真实红 | clone 缺失 | — |

**重要：**

- `pytest` 全绿 **≠** Doris 集成已过；c 与 c2 分开记账。
- `DATAPILOT_LLM_MODE=mock` **不是** no-mock-fallback 测试；P0 测的是门禁失败时 **禁止** mock_fallback。
- 报告里的数字 / rowcount / rule_id **只允许**从当次运行或已提交的 result 文件抄录。

## SQLGuard 入口（钉 `7dc85dd` / v1.1.2）

```bash
cd sql-write-gate && git checkout 7dc85dd
make test   # pytest 全绿；live PG/MySQL 可 SKIP
# version → 1.1.2；healthz version 1.1.2
# 可选：pytest -q tests/test_v110.py
gh run list -R tangyf07/sql-write-gate --branch main --limit 3  # CI conclusion=success
```

HTTP 契约路由：`/v1/check` | `/v1/block` | `/v1/execute`。

## DataPilot 入口（钉 `e3e603d`，基线 `285202c`）

```bash
cd DataPilot && git checkout e3e603d && pip install -e ".[dev]" -q
# 推荐（suite_p0）：
pytest -m suite_p0 -q
# 等价文件入口（285202c 行为基线）：
pytest tests/test_gate_no_mock_fallback.py \
       tests/test_time_intent_predicates.py \
       tests/test_sqlguard_http_contract.py -q

# 可选 Doris（无则 SKIP）：
DATAPILOT_QUERY_BACKEND=doris DATAPILOT_DORIS_URL=mysql://root@127.0.0.1:9030/ads \
  DATAPILOT_GUARD_MODE=write_gate pytest tests/test_doris_g7.py -q
```

## 环境变量

| 变量 | 默认 | 说明 |
|------|------|------|
| `GAMESTREAM_ROOT` | 脚本推断 | GameStream 根 |
| `SQLGUARD_REPO` / `DATAPILOT_REPO` | WSL sibling 路径 | 两仓 clone |
| `LOCK_FILE` | `versions.lock` | pin 来源 |
| `RESULT_FILE` | `docs/suite-acceptance-result.txt` | 本报告 |
| `DORIS_URL` | `mysql://root@127.0.0.1:9030/ads` | Doris |
| `GUARD_URL` | `http://127.0.0.1:8787` | SQLGuard HTTP |
| `START_GUARD` | `1`（传给 G7） | 无 healthz 时自动 serve |

## 与 G7 / G8 边界

- **G7**：问数门禁闭环（fixture/DataPilot → SQLGuard → **已有** Doris ADS）。本 suite 的 a/b 直接复用。
- **G8**：**未做**；本目录与脚本 **不**实现「持续主流水线 + 问数」统一验收。

## 最新报告

每次跑完覆盖写入：[`suite-acceptance-result.txt`](suite-acceptance-result.txt)。  
摘要表含各 check 的 **STATUS + backend + 摘录 detail**（无虚构）。

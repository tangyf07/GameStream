# 三仓固化验收（Suite Acceptance）

目标：把 **GameStream + SQLGuard(sql-write-gate) + DataPilot** 钉在可复跑的 **verification baseline SHA** 上，用一条脚本给出 **PASS / FAIL / SKIP / INCOMPLETE** 标签，并写明 **执行后端**（`doris` vs `mock` / `offline-pytest`）。  
**不编造指标或 pass/fail。** G8 主流水线/bench **不在本脚本范围内**（另见 [`g8-continuous-mainline.md`](g8-continuous-mainline.md)）。

相关：[`g7-closed-loop.md`](g7-closed-loop.md) · [`versions.lock`](../versions.lock) · 最新报告 [`suite-acceptance-result.txt`](suite-acceptance-result.txt)

## 钉死 SHA（`versions.lock` = verification baselines）

| 仓 | Pin（baseline） | 说明 |
|----|-----------------|------|
| **GameStream** | `ed876e1`（或更新的 main） | suite 基线（strict gate + G8 文案诚实）；**实际 HEAD 可更新**（pin 为 ancestor 即 OK） |
| **SQLGuard**（sql-write-gate） | `7dc85dd` → **v1.1.2** | `make test`；healthz `1.1.2`；继承 1.1.1 限定名身份（`hive.ads_dau_di` BLOCK）；路由 `/v1/check` `/v1/block` `/v1/execute` |
| **DataPilot** | **`2541623`** | `pytest -m suite_p0`（**37 passed**；行为超集 `e3e603d`）；**行为基线含 `285202c` P0**（no-mock-fallback / 时间谓词 / HTTP 契约） |

**重要：** `versions.lock` 里的 pin 是 **验收对照基线**；结果文件 **另记** 各仓 **actual full SHA** 与 dirty/diff。二者可以不同（尤其 GameStream 前进时）。

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

### 严格门禁（默认）vs report-only

| `MODE` | 行为 | 退出码 |
|--------|------|--------|
| **`strict`（默认）** | required check **FAIL** 或 pin **MISMATCH** → 非 0；required **SKIP** → **INCOMPLETE** / 非 0 | `0` PASS · `1` FAIL · `2` INCOMPLETE |
| **`report-only`** | 照常写报告与真实 STATUS；**不**因 FAIL/SKIP/MISMATCH 改退出码 | 恒 `0` |

判定顺序：**先 FAIL / MISMATCH，再 SKIP**——绝不把「既有失败又有 skip」记成纯 SKIP。

```bash
# 默认严格：
bash /tmp/suite_acc.sh
# 只出报告（CI 旁路 / 本地扫一眼）：
MODE=report-only bash /tmp/suite_acc.sh
```

前置建议：

1. Doris ADS 有行（`mysql://root@127.0.0.1:9030/ads`）；空则先 `bash scripts/e2e_g2.sh`
2. SQLGuard：`git checkout 7dc85dd`；`~/g7-venv` 或 `pip install -e '.[mysql]'`；healthz `version=1.1.2`
3. DataPilot：`git checkout 2541623 && pip install -e ".[dev]"`

## 检查项与状态语义

| ID | 做什么 | required? | PASS | FAIL | SKIP |
|----|--------|-----------|------|------|------|
| **a) `g7_closed_loop`** | `MODE=fixture bash scripts/g7_closed_loop.sh` | **yes** | 退出 0；真实 rowcount / gate 摘自结果文件 | 非 0（含 ADS empty） | 脚本缺失 |
| **b) `sqlguard_cross_db_hive`** | `hive.ads_dau_di` 必须 **BLOCK**（1.1.2） | **yes** | healthz=1.1.2 且 datapilot/action=BLOCK | 版本不对或未 BLOCK | serve 不可用且无 G7 证据 |
| **c) `datapilot_offline_p0`** | 优先 `pytest -m suite_p0 -q`（`2541623`，期望 **37 passed**）；否则三文件等价入口 | **yes** | pytest exit 0 | 非 0 | clone 缺失 |
| **c2) `datapilot_doris_g7`** | 可选 `pytest tests/test_doris_g7.py` | no | 真连 Doris 且绿 | 联调失败（**FAIL 优先于 SKIP**） | Doris 不可达 → **SKIP（勿当 PASS）** |
| **d) `sqlguard_unit`** | `make test`（或 `pytest -q`）；可选 `tests/test_v110.py`；CI 实录 | **yes** | 本地绿 **或** 本地 env 阻断（无 make / python3.x-venv / ensurepip）且 **CI success@pin**（backend=`github-ci`，detail 写明 local≠PASS） | 本地真红且无 CI 证据 | clone 缺失 |

**Pin 结果 token：** `OK:<full_sha>:<dirty_summary>` / `MISMATCH:<full_sha>:…` / `MISSING:n/a`。GameStream 允许「pin 或更新」；SQLGuard / DataPilot 要求 short-SHA 对齐 pin。

**重要：**

- `pytest` 全绿 **≠** Doris 集成已过；c 与 c2 分开记账。
- `DATAPILOT_LLM_MODE=mock` **不是** no-mock-fallback 测试；P0 测的是门禁失败时 **禁止** mock_fallback。
- 报告里的数字 / rowcount / rule_id **只允许**从当次运行或已提交的 result 文件抄录。
- 结果文件记录 **actual full HEAD + dirty/diff**；`versions.lock` 只钉 baseline。

## SQLGuard 入口（钉 `7dc85dd` / v1.1.2）

```bash
cd sql-write-gate && git checkout 7dc85dd
make test   # pytest 全绿；live PG/MySQL 可 SKIP
# version → 1.1.2；healthz version 1.1.2
# 可选：pytest -q tests/test_v110.py
gh run list -R tangyf07/sql-write-gate --branch main --limit 3  # CI conclusion=success
```

HTTP 契约路由：`/v1/check` | `/v1/block` | `/v1/execute`。

## DataPilot 入口（钉 `2541623`，基线 `285202c`；超集 `e3e603d`）

```bash
cd DataPilot && git checkout 2541623 && pip install -e ".[dev]" -q
# 推荐（suite_p0；期望 37 passed）：
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
| `LOCK_FILE` | `versions.lock` | pin **baseline** 来源 |
| `RESULT_FILE` | `docs/suite-acceptance-result.txt` | 本报告（含 actual full SHA） |
| `DORIS_URL` | `mysql://root@127.0.0.1:9030/ads` | Doris |
| `GUARD_URL` | `http://127.0.0.1:8787` | SQLGuard HTTP |
| `START_GUARD` | `1`（传给 G7） | 无 healthz 时自动 serve |
| `MODE` / `SUITE_MODE` | `strict` | `strict` 或 `report-only` |

## 与 G7 / G8 边界

- **G7**：问数门禁闭环（fixture/DataPilot → SQLGuard → **已有** Doris ADS）。本 suite 的 a/b 直接复用。
- **G8**：持续主流水线 + 小规模分块负载验证 **另文档/脚本**；本 suite **不**把 G8 列入 required checks。

## 最新报告

每次跑完覆盖写入：[`suite-acceptance-result.txt`](suite-acceptance-result.txt)。  
摘要表含各 check 的 **STATUS + required + backend + 摘录 detail**，以及 **gate overall / exit_rc**（无虚构）。

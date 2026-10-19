# 07: PIT 宇宙落地（roadmap T0.4）

**What to build:** 把回测股票池从静态 `list_status='L'` 改为 point-in-time 宇宙：窗口内曾上市、以及后续退市的股票，在其可交易期间进入样本。这是 ADR 0001 列出的**冻结度量口径**之一（PIT 宇宙），但目前实际仍是 L-only——本卡把现实与契约对齐。

**Blocked by:** ~~data availability~~ → 已解除（数据层已回填并验收，2026-10-19）

**Status:** ready-for-human（默认已切 `pit`；L-only/PIT 全回测对照已完成并入账本 §10.4）

- [x] 用 `delist_date` 等时点信息构造 PIT 池；窗口内退市的股票在其可交易期间进入样本，而非被整体剔除。（驱动层已落地：`--universe pit` 按 `[list_date+120d, delist_date]` 建谓词；退市股清单与行情回填由数据层并行推进）
- [x] 与 L-only 池并列给出同口径指标，量化差异（重点看反转、涨停、小市值类策略的绩效变化）。**全回测已完成（2026-10-19，L-only + PIT 各一次，均 `exit=0`）**；同库同窗口对照见「双口径对照结果」与账本 §10.4。
- [x] 数据源失败按 AGENTS.md 的数据源 / fallback 约定降级，不拖垮主流程；数据缺失时明确告警而非静默回退到 L-only。（`list_status='D'` 行数为 0 或探针失败时告警并声明结果等价 L-only）
- [x] 若触发口径变化，`config_hash` 相应改变（`l-only=5eea0519…` 逐位不变；`pit=acb32bd95d3c94c9c575b00008409c483cd04064`，已单测）。（**账本对照指标已记录，见 §10.4**）
- [x] 落地后，若 03/04/05 已在 L-only 池上翻转默认值，须在 PIT 池上重验这些翻转。（**N/A：卡 A/B/C 均 `ready-for-agent`，尚未翻转任何默认值，无待重验项**）

**Notes:** 与 01、02 同属无阻塞的 frontier。已知风险：个股 PIT 需要退市股清单，行业 PIT 曾因源端 `out_date` 全空而数据受阻（见账本 §9.11 P3-❻）——数据可得性需先确认。当前 L-only 会系统性抬高部分策略绩效，本卡决定裁判口径是否可信。

---

## 数据可得性侦察（2026-10-19，只读）

对本地 PG（`tushare` 库）逐表核查，结论：**当前数据无法构造 PIT 宇宙**。

| 探测 | 结果 |
| --- | --- |
| `tushare_stock_basic.list_status` 分布 | **只有 `L`**，5577 行；**`D`(退市) / `P`(暂停) 均为 0 行** |
| `delist_date` 非空行 | 仅 2 行，值均为 `2026-10-19`（=探测当日，明显是脏值/占位），退市日期实际不可用 |
| filter 命中（`exchange∈SSE,SZSE AND market∈主板,中小板,创业板`） | L=4608，**D=0** |
| 行情表是否含退市股（孤儿码：`tushare_daily` 有行但 `stock_basic` 无行） | **仅 1 个**（`920305.BJ`，北交所，本就不在 filter 内）；窗口内孤儿码 = 1 |
| `tushare_daily` 覆盖 | 2016-07-01 ~ 2026-10-16，5578 个代码、10.58M 行 —— 全部是**当前在市**代码 |
| `TUSHARE_TOKEN` | 已配置（具备发起抓取的凭据） |

**判定：** 退市股既无清单行（`stock_basic` 无 `D`），也无任何历史行情行（无孤儿数据）。要在**可交易期间**把它们纳入样本，必须先做一次数据采集：

1. 抓取 `stock_basic(list_status='D'/'P')` → 得到退市代码 + `delist_date`（`data_provider/tushare_fetcher.get_stock_list()` 现硬编码 `list_status='L'` 且只返回 `code/name/industry/area/market`，需扩参并补 `list_date/delist_date`；`scripts/fetch_tushare_stock_list.py` 同理只抓 L）。
2. 回填这些退市码在窗口内的行情与辅助表（`tushare_daily` / `daily_basic` / `moneyflow` / `stk_limit` / `cyq` / `margin_detail`）——现有采集脚本全部按 `list_status='L'` 硬过滤，需扩展。
3. 之后才谈得上跑 PIT 池与 L-only 池的同口径对照（每次完整回测 ≈1.5h）+ `config_hash` 变更 + 03/04/05 翻转重验。

这是一个**联网、改库、且体量不小**的数据工程前置，不属于「写一段 PIT SQL」即可完成的改动。按 AGENTS.md 数据源/降级约定，本卡当前应：明确告警而非静默回退到 L-only（可低成本先做），真实 PIT 待数据回填后再启用。

**待决策：** 见下方 Options（需用户选择）。

---

## 路线 B 可行性（用户已选定：先采集再落地 PIT，只从 tushare 获取）

对 tushare API 的只读探测（未写库）：

| 探测 | 结果 |
| --- | --- |
| `stock_basic(list_status='D')` | **341 行**，`delist_date` 341/341 非空；命中回测 filter（SSE/SZSE × 主板/中小板/创业板）= **332** |
| `stock_basic(list_status='P')` | 0 行（无需处理） |
| 窗口内（2021-05-14~2026-10-16）退市码 | **205**（2021:20 / 2022:43 / 2023:43 / 2024:52 / 2025:29 / 2026:18） |
| `daily` / `daily_basic` / `moneyflow` / `stk_limit`（退市码，窗口内） | **均有完整历史**（例：`605081.SH` 1271 行、`603963.SH` 987 行，直到退市日） |
| `margin_detail` | 稀疏（仅曾为两融标的者，例 `603963.SH` 368 行）；LEFT JOIN，缺失可容忍 |
| `cyq_perf` | **无权限** —— 但 `tushare_cyq` 是**本地由 daily+daily_basic 计算**（`incremental_cyq.py`），不依赖该接口，回填 daily/daily_basic 后重算即可 |

**结论：路线 B 可行**，无需 `cyq_perf` 权限。

### 落地风险（已定位）
现有采集链路**全部按 `list_status='L'` 硬过滤**，其中 `incremental_base.py` 对 `tushare_stock_basic` 做 `DELETE`+`INSERT` **全量刷新**——手工塞入的 `D` 行会在下一次日常刷新时被抹掉。因此必须把 `D/P` 支持做进采集链路，而非一次性插行：
- `scripts/data_collection/incremental_base.py`（stock_basic 全量刷新，需纳入 D/P）
- `scripts/data_collection/incremental_daily.py`（daily / adj_factor / daily_basic）
- `scripts/data_collection/incremental_factor.py`（moneyflow / stk_limit / margin_detail）
- `scripts/data_collection/incremental_cyq.py`（筹码重算，按 stock_basic 取码）
- `scripts/data_collection/ts2pg.py`
- `data_provider/tushare_fetcher.py::get_stock_list()` 与 `scripts/fetch_tushare_stock_list.py`（支持 D/P + 返回 list_date/delist_date）

### 执行计划
1. **数据层**：扩展上述脚本支持 D/P；对 205 个窗口内退市码回填 daily/adj_factor/daily_basic/moneyflow/stk_limit/margin_detail，并重算 cyq；用覆盖率查询验收。
2. **驱动层**：`scripts/backtest_5y_23strategies.py` 新增 `--universe {l-only,pit}`（**默认由 `l-only` 切为 `pit`**，见「默认口径切换裁决」；`l-only` 为可逆 opt-out，其 SQL / 产物文件名 / 冻结 `config_hash` 逐位不变）；PIT 谓词以 `list_date`/`delist_date` 界定可交易期，移除 `list_status='L'` 硬过滤；PIT 数据缺失时**显式告警**（不静默回退）。
3. **对照**：L-only vs PIT 同口径指标对照（重点：反转/涨停/小市值类），`config_hash` 变更入账本；若 03/04/05 已翻转则在 PIT 池重验。

---

## 落地与验收（2026-10-19，主 agent 独立复核）

路线 B 已实施并验证。**数据层**（`incremental_base/bootstrap/daily/factor/cyq`、`tushare_pg_utils`、`tushare_fetcher`、`fetch_tushare_stock_list.py`）与**驱动层**（`backtest_5y_23strategies.py`）均落地。

独立复核结果（非子代理自述）：

| 检查 | 结果 |
| --- | --- |
| `list_status` 分布 | **D=340 / L=5575**（`D` 行持久化，340/340 有 `delist_date`） |
| L 由 5577→5575 的原因 | `601198.SH`（东兴证券）、`601059.SH`（信达证券）于 `2026-10-19` 退市，API 全量刷新把这两行从 `L` 正确移入 `D`（**非数据丢失**） |
| `tushare_daily` 去重代码数 | 5578 → **5783（+205）**；孤儿码 **0** |
| 覆盖样例 | `605081.SH` sheet=1271 / `cyq`=1271；`603963.SH`=987 —— 与探测值逐位一致 |
| `T600018.SH` | 未入库（`symbol='T600018'` 7 字符 > `varchar(6)`；按约束不改 schema，跳过并告警；不在 205 集内） |
| 单测 | `39 passed`（PIT 12 + fetcher 20 + fetch 脚本 7）；既有组合/回测套件回归通过 |
| L-only SQL 字节级 | 与 `HEAD` 内联块**逐字节一致**（124 字符；`list_status='L'` 在 CUR 全文仅出现 1 次，无重复/游离过滤） |
| 驱动层端到端（短窗口 `load_data`，2024Q1） | `l-only` 4415 码 / 254,936 行；`pit` **4512 码 / 260,241 行**；**pit-only=97 且 l-only-only=0**（PIT 为 L-only 严格超集） |

**结论：** PIT 宇宙已真实可构造，生存者偏差修复在池层面已闭环（+97 退市码、0 个在池个股被剔除）。

**收尾（2026-10-19 全部完成）：**
1. **PIT 与 L-only 的同口径绩效对照**——已完成（L-only + PIT 各一次全回测，均 `exit=0`；同库同窗口 `20210514~20261016`）。结果见下「双口径对照结果」与账本 §10.4。
2. ~~是否切默认~~ → **已裁决切为 `pit`**（见下）。
3. ~~03/04/05 翻转在 PIT 池重验~~ → **N/A：卡 A/B/C 均未翻转默认值。**

**遗留（非本卡阻塞）：** `T600018.SH` 未入库（`symbol` 7 字符 > `varchar(6)`，不在 205 退市码集内）；`cyq_perf` 无权限（本地由 `daily + daily_basic` 计算，已绕过）；行业 PIT 仍受账本 §9.11 P3-❻ 阻塞（`tushare_index_member_all.out_date` 全空，独立问题）。

---

## 默认口径切换裁决与产物冲突修复（2026-10-19）

用户裁决：**`--universe` 默认由 `l-only` 切为 `pit`**（ADR 0001 冻结度量口径），`l-only` 保留为显式 opt-out。

- 常量：`UNIVERSE_DEFAULT = "pit"`；`l-only` 分支 SQL / 产物文件名 / 冻结 `config_hash`（`5eea0519…`）逐位不变，故 T0.1 基线仍可逐字节复现。
- **产物文件名冲突修复**：`_write_portfolio_sim_artifacts` 原文件名 `portfolio_sim_<start>_<end>` **不含口径**，PIT 与 L-only 会写同一路径并覆盖冻结基线。已抽出 `_portfolio_sim_artifact_stem()`：非 `l-only` 追加 `_<universe>` 后缀（PIT → `portfolio_sim_<start>_<end>_pit`），`l-only` 文件名不变。
- 单测更新为 13 例（`tests/test_backtest_pit_universe.py`）：新增「产物文件名口径隔离」，默认 flag 断言改为 `pit`，`l-only` 相关断言改为显式传参。
- `docs/CHANGELOG.md` 同步更新默认值表述。

---

## 双口径对照结果（2026-10-19，同库同窗口）

命令（`result/backtest_胜率收益率提升规划_v2_20261017.md:213` 基线 + 口径旋钮）：

```
python scripts/backtest_5y_23strategies.py --force --no-handoff --portfolio-sim --entry-timing next_open --universe pit
python scripts/backtest_5y_23strategies.py --force --no-handoff --portfolio-sim --entry-timing next_open --universe l-only
```

次序执行、各一次，均 `exit=0`。窗口 `20210514~20261016`（两口径一致；`n_days=1400`、`oos_days=1147`、`oos_start=2022-01-17`）。产物：`result/portfolio_sim_20210514_20261016_pit.json` / `result/portfolio_sim_20210514_20261016.json`。`config_hash`：PIT `acb32bd9…` / L-only `5eea0519…`。

| 指标 | L-only | PIT | Δ | 相对 |
| --- | --- | --- | --- | --- |
| 样本外 CAGR | 3.88% | **4.20%** | +0.32pct | +8.26% |
| 全期 CAGR | 3.66% | **3.93%** | +0.26pct | +7.16% |
| 夏普 | 0.423 | **0.444** | +0.021 | +4.94% |
| 最大回撤 | **13.11%** | 13.91% | +0.80pct | +6.11% |
| 总收益 | 22.12% | **23.84%** | +1.72pct | +7.80% |
| 胜率 | 43.67% | 43.93% | +0.26pct | +0.59% |
| 成交笔数 | 561 | 560 | −1 | −0.18% |
| 期末权益 | 1,221,170 | **1,238,420** | +17,250 | +1.41% |
| 验收门槛 | pass | pass | — | — |

**判读：** PIT 修复生存者偏差后，样本外 CAGR / 夏普 / 总收益**小幅改善**，代价是 maxDD +0.80pct（仍在门槛内）。成交笔数几乎不变（−1），但**构成显著迁移**——反转（超跌）与涨停族成交增加（`washout_break +26`、`limit_up_pullback +11`、`volume_surge_std +3`），`holder_conc_break −24`、`rsi_bullish_divergence −9` 等减少；方向与「生存者偏差主要抬高部分策略」的预期一致。

**L→D 迁移零影响已证：** 同库 L-only 全回测逐位复现 2026-10-18 冻结基线（metrics 与 `config_hash` 完全一致），确认 `601198.SH`/`601059.SH` 的 L→D 迁移不影响 L-only 池绩效。

**03/04/05 翻转重验：** 卡 A/B/C 均 `ready-for-agent`（未翻转），本轮无待重验项。

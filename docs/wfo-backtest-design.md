# WFO 滚动向前回测方案设计（4Y 训练 / 1Y 验证 / 6M 步长）

> 状态：**待审阅**（本文件为设计稿，审阅通过后再进入实现）
> 关联代码：`scripts/backtest_5y_23strategies.py`（现有 23 策略 5 年回测脚本，本方案最大程度复用其管线）
> 关联数据：本地 Tushare PostgreSQL（`tushare_daily` / `tushare_daily_basic` / `tushare_cyq` / `tushare_moneyflow` / `tushare_stk_limit` / `tushare_margin_detail` / `tushare_stk_holdernumber` / `tushare_forecast` / `tushare_pledge_stat` / `tushare_top_inst` / `tushare_fina_indicator` / `tushare_index_daily` / `tushare_adj_factor` 等）

---

## 1. 背景与目标

现有 `scripts/backtest_5y_23strategies.py` 在**单一固定窗口**（2021-01-01 ~ 最近第 6 个交易日）上对 23 个规则策略做全量回测，再用最近 5 个交易日做一次小样本验证。存在两个结构性问题：

1. **选择偏差**：策略入选（Top-N）、组合权重、最优持有期都在**同一段数据**上选出并评估，没有真正的样本外验证，绩效被系统性高估。
2. **无滚动**：一次运行只覆盖一个时间切片，无法回答"策略集在不同市场阶段是否稳定"。

本方案引入 **Walk-Forward Optimization（WFO）滚动向前回测**：

- **窗口**：4Y 训练（In-Sample，IS）/ 1Y 验证（Out-of-Sample，OOS），**6M 步长**滚动；
- **固化逻辑**：策略阈值、过滤、成本、冷却期等全部冻结为全局常量，**窗口间绝不重调参**；每窗口只"训练" 3 项（组合权重 / 最优持有期 / 入选策略集），且全部只读 IS 数据；
- **每日选股**：落地为"部署工件 + 每日轻量信号"的生产模式，与 WFO 评估口径一致；
- **重训练时机**：默认 6M 日历驱动，可选显式强制重训。

## 2. 现状分析：可复用资产

现有脚本的核心管线**已经是因果、无前视**的，全部原样复用，不重写：

| 阶段 | 函数 | 复用方式 |
|---|---|---|
| 数据加载（行情+基本面+过滤） | `load_data` | 直接复用 |
| 前复权 | `load_adj_factors_from_db` + `apply_forward_adjustment` | 直接复用 |
| 增强信号 as-of 合并（股东/预告/质押/龙虎榜） | `load_signal_aux` | 直接复用 |
| 技术指标 | `compute_indicators` | 直接复用（因果，可全量帧预计算） |
| 市场 regime | `load_index_daily` + `compute_market_ok` | 直接复用 |
| 财务质量 as-of 对齐 | `load_fina_indicator` + `merge_fina_by_ann_date` | 直接复用 |
| 动态退出收益（ATR 止损/移动止盈/时间止损） | `compute_dynamic_exit_returns` | 直接复用（因果） |
| 信号 + 买入掩码 + 策略族过滤 | `_strategy_signal` | 直接复用（IS/OOS/每日共用同一入口） |
| 冷却期去重 | `_apply_cooldown` | 直接复用 |
| 单策略回测（分持有期绩效） | `_backtest_single` | 复用，**增加选期口径参数**（见 §4.2） |
| 绩效指标（期望/夏普/回撤/凯利） | `calc_metrics` | 直接复用 |
| 基准对比（沪深300/等权回退） | `compute_benchmark_metrics` | 直接复用 |
| 每日推荐打分 | `get_next_day_recommendations` | 复用，输入改为部署工件 |
| 交易日历 / 数据库连接 / 并行配置 | `is_trading_day` / `_get_pg_engine` / `resolve_parallel_config` | 直接复用 |

**现有脚本里真正"会学"的元素只有 3 个**，恰好就是 WFO 每窗口的训练对象：

| 现有"学习"元素 | 现状 | WFO 中的角色 |
|---|---|---|
| `_ENSEMBLE_WEIGHTS`（组合策略权重） | 全 5Y 窗口按组件胜率归一化（`run_backtests` L2118-2135） | 每窗口 IS 训练项 |
| `best_p`（每策略最优持有期） | 全窗口按 `total_return` 选取（`_backtest_single` L2041） | 每窗口 IS 训练项，**口径改为 `expectation`**（已确认） |
| 策略排序 / Top-N 入选 | 全窗口按 `expectation` 降序（`run_backtests` L2167） | 每窗口 IS 训练项（入选策略集） |

## 3. 总体设计

```
┌──────────────────────────── 全量数据帧（一次加载，多次切片）────────────────────────────┐
│  load_data(2021-01-01, today) → 复权 → aux as-of → 指标 → regime → 财务 as-of → dyn_ret │
│  （全部因果计算，无前视；指标/动态退出只算一次）                                           │
└────────────────────────────────────────┬────────────────────────────────────────────────┘
                                         │ 按窗口切片
              ┌──────────────────────────┼──────────────────────────┐
              ▼                          ▼                          ▼
        W1: IS 2021-01~2024-12     W2: IS 2021-07~2025-06     W3: IS 2022-01~2025-12
            OOS 2025-01~2025-12        OOS 2025-07~2026-06        OOS 2026-01~2026-12(部分)
              │                          │                          │
              └──────────┬───────────────┴───────────┬──────────────┘
                         ▼                           ▼
              逐窗口 IS 训练（3 项）         逐窗口 OOS 评估（冻结选择）
                         │                           │
                         └───────────┬───────────────┘
                                     ▼
              聚合 OOS（按"每日激活映射"按天去重）→ 全样本外绩效 + 基准对比 + WFE
                                     ▼
              部署工件 result/wfo_model_latest.json（最新完整 train 窗口）
                                     ▼
              每日选股（读工件 + 最新数据跑信号 + IS 胜率打分）
```

## 4. 窗口网格与"激活映射"

### 4.1 网格构造

```
Wk: train = [T0 + k·6M,  T0 + k·6M + 4Y)      IS
    val   = [T0 + k·6M + 4Y, T0 + k·6M + 5Y)  OOS
```

- `T0 = 2021-01-01`（与现有 `main()` 的 `anchored_start` 锚定逻辑一致，见脚本 L2627-2629）；
- 边界按 `tushare_trade_cal` 吸附到实际交易日（复用 `is_trading_day` 的查询方式）；
- 窗口生成条件：`train_start_k + 4Y <= 数据最新交易日`（train 完整才生成）。

以 2026-08-14 为"今天"的实例：

| 窗口 | 训练区间（IS） | 验证区间（OOS） | 状态 |
|---|---|---|---|
| W1 | 2021-01-01 ~ 2024-12-31 | 2025-01-01 ~ 2025-12-31 | ✅ 完整 |
| W2 | 2021-07-01 ~ 2025-06-30 | 2025-07-01 ~ 2026-06-30 | ✅ 完整 |
| W3 | 2022-01-01 ~ 2025-12-31 | 2026-01-01 ~ 2026-12-31 | ⚠️ 部分（~150 交易日） |
| W4 | 2022-07-01 ~ 2026-06-30 | 2026-07-01 ~ 2027-06-30 | ⚠️ 部分（~30 交易日） |

> W5（train 2023-01-01~2026-12-31）的 train 尚未完整，不生成。下一次重训在 2027-01-01 前后触发 W5。

### 4.2 激活映射（评估口径 = 生产口径）

步长 6M < 验证 1Y，**相邻窗口的 OOS 区间有 6 个月重叠**。若直接拼接各窗口 OOS 收益，同一交易日会被多个窗口重复计数，样本自相关被高估。

定义**每日激活模型**：交易日 `d` 生效的模型 = "train 恰好完整、且 val 起点 ≤ d" 的最近窗口：

```
day d ∈ [val_start_k, val_start_{k+1})  →  由 Wk 的 IS 选择结果驱动
```

这正好等于生产模式（每天用最新已完成训练的模型跑信号）。因此：

- **逐窗口 OOS**（区间重叠，诊断用）：每窗口独立 IS→OOS 衰减对比；
- **聚合 OOS**（按天不相交，生产一致）：把时间轴按激活映射切段，每个交易日只统计一次 → 既避免重复计数，又保证"评估结果 = 线上表现"。聚合 OOS 的绩效（胜率/期望/回撤/年化/夏普/超额）是研究运行的**头条结论**。

**聚合绩效口径（真实组合曲线）**：交易级指标（胜率/期望/盈亏比）沿用 `calc_metrics` 对全 OOS 交易收益序列的计算；**回撤/年化/夏普不再对混合持有期收益序列直接计算**（不同 best_p 的收益混入单一序列会严重高估回撤），改为按天持仓市值重建真实组合曲线：

- 每笔交易等权 1 单位资金，入场日收盘买入（市值 1.0），持有期逐日按收盘价 mark-to-market，出场日按实际退出价结算（`ret` 已含动态退出与成本）；
- 组合日收益 = Σ(当日持仓市值变动) / Σ(前日持仓市值)，无持仓日为 0（现金）；
- 组合净值 = 日收益连乘，最大回撤/年化/夏普从日收益序列计算（年化按 252 交易日，夏普 `mean/std × √252`）；
- 出场日由 `_find_exit_day_offset` 按与 `compute_dynamic_exit_returns` 完全同码的动态退出逻辑（ATR 止损 / 移动止盈 / 时间止损）定位，保证出场时点与 `dyn_ret_{p}d` 一致。

### 4.3 数据跨度与 warmup（已确认）

- **统一起始日期 `DATA_START = "2021-01-01"`**：WFO 数据加载起点与现有 5Y 回测锚定一致，不额外加载 2021 年前历史；
- **warmup 用 burn-in 消化**：指标（MA120/vol_60d 等）与 as-of 对齐在数据起点处需要预热期。定义 `BURN_IN_TRADING_DAYS = 120`（交易日，≈6 个月）：**每个 train 切片前 120 个交易日的信号不进入 IS 统计**。
  - 实际影响：W1 的 IS 有效区间 ≈ 2021-07-01 ~ 2024-12-31（~3.5Y）；W2+ 的 train 起点天然晚于数据起点 ≥ 120 交易日，burn-in 无实际影响（规则统一，仅 W1 生效）；
  - OOS 切片起点 ≥ 数据起点 + 4Y，指标全部已预热，无需 burn-in。
- **数据就绪检查**：运行前 SQL 校验各核心表 `min(trade_date) <= 2021-01-01`。当前采集脚本默认起始已覆盖：`incremental_factor.py` / `incremental_index.py` / `incremental_macro.py`（`DEFAULT_START = 20160101`）、`ts2pg.py`（10 年回补）、`incremental_cyq.py`（2016-01-01 起）。**若个别表缺失 2021-01-01 前数据，调整对应 `scripts/data_collection/incremental_*.py` 的起始参数统一回填，不另起炉灶。**

## 5. 固化逻辑 vs 滚动变量（边界契约）

### 5.1 固化（全局常量，任何窗口都不得改动）

| 类别 | 内容 |
|---|---|
| 策略阈值 | 23 个信号函数的打分权重/触发阈值（`>=6`、`>=10` 等） |
| 交易成本 | `TRADING_COST_PCT = 0.15`（%），单边往返 |
| 可成交/质量过滤 | 涨停/跌停判定、市值带（20~500 亿）、量比 ≥1.5、换手带、获利盘带、质押 ≤40%、股息率 ≥1%、龙虎榜净买、财务 ROE/营收 |
| 冷却期 | `SIGNAL_COOLDOWN_DAYS = 5` |
| 持有期集合 | `HOLDING_PERIODS = [1, 3, 5, 10]` |
| 选择策略（超参数） | `WFO_TOP_K = 8`、`WFO_MIN_IS_TRADES = 50`、`WFO_BURN_IN_TRADING_DAYS = 120`、`WFO_MIN_OOS_TRADING_DAYS = 60`、选期口径 `expectation` |

> 固化约束：排序指标、K、过滤规则在窗口间**完全不变**。窗口间变化的只是"哪 8 个策略入选 + 各自权重/持有期"，从机制上防止逐窗口调参过拟合。

### 5.2 滚动（每窗口在 IS 上重算，仅这 3 项）

1. **组合策略权重**：`_ENSEMBLE_WEIGHTS` ← Wk 的 IS 切片上 3 个组件（`ma_crossover` / `volume_surge_std` / `multi_ma_resonance`）胜率归一化（逻辑照搬 `run_backtests` L2118-2135，输入换成 IS 切片；实现上抽成 `_compute_ensemble_weights(component_results)` 供两处共用）；
2. **每策略最优持有期** `best_p`：按 IS 的 `expectation` 取最大（**已确认：从 `total_return` 改为 `expectation`**——单笔期望不受样本量影响，选期更稳）；
3. **入选策略集**：IS 上对全部 23 策略打分 → 过滤（`total_trades >= 50`，且剔除 burn-in 信号）→ 按 `expectation` 降序取 **Top-8**（已确认 K=8）。

## 6. 训练阶段（IS，每窗口）

对 Wk 的 train 切片，逐策略复用 `_backtest_single(df_is, name, select_period_by="expectation")`（信号 + 冷却期 + 动态退出 + 扣成本 + 分持有期指标），产出：

```
IS 指标表：strategy × holding_period → {trades, win_rate, expectation, sharpe, max_dd, annualized, kelly}
```

要点：

- **一次加载、多次切片**：指标/regime/财务/aux/动态退出收益在**全量帧上只算一次**（全部因果，见 §9），每窗口只做 `df_all[date 区间]` 切片 + 跑信号，不重算指标；
- `_ENSEMBLE_WEIGHTS` 是模块级全局：**窗口间串行**执行（先 set 权重再启动该窗口的策略线程池），窗口内并行只读该权重，无竞态；
- 组合策略的 3 个组件在 IS 上单独计分（它们既是组件又是可入选策略）。

## 7. 验证阶段（OOS，每窗口）

用 Wk 的**冻结选择**（入选集 + 各自 best_p + 组合权重）在 val 切片上跑同一管线，产出：

- **逐窗口 OOS 指标**（与 IS 同口径）；
- **IS→OOS 衰减**：`Δexpectation`、`Δwin_rate`、`WFE = OOS_annualized / IS_annualized`（Walk-Forward Efficiency）；
- **策略集 churn**：相邻窗口入选集的 Jaccard 相似度，量化选择稳定性。

**聚合 OOS**（生产一致口径，按 §4.2 激活映射按天去重）：

- 交易级指标（总胜率 / 期望 / 盈亏比）来自全 OOS 交易收益序列；
- 回撤 / 年化 / 夏普来自**真实组合曲线**（§4.2：按天持仓市值重建，出场日与动态退出同码）；
- 与同期沪深 300 对比超额（复用 `compute_benchmark_metrics`）；
- **IS/OOS 排序相关性**（Spearman）：IS 上表现好的策略 OOS 是否依然靠前——WFO 的核心价值指标。

**OOS 门槛**：窗口 val 切片有效交易日 < `WFO_MIN_OOS_TRADING_DAYS`（60）→ 该窗口**只用于产出部署模型，不参与聚合统计**（避免极小样本污染结论，如 W4）。

## 8. 每日选股（生产模式）

**部署工件** `result/wfo_model_latest.json`（由最近一次完整 train 窗口生成）：

```json
{
  "trained_at": "2026-07-01",
  "window": {"train_start": "2022-07-01", "train_end": "2026-06-30"},
  "strategies": [
    {"name": "n_pattern", "best_period": 5, "is_win_rate": 57.3, "is_expectation": 0.82}
  ],
  "ensemble_weights": {"ma_crossover": 0.31, "volume_surge_std": 0.40, "multi_ma_resonance": 0.29}
}
```

**每日流程**（轻量，不跑全量 WFO）：

1. 读工件 + 加载最近 ~1 年数据（含指标/regime/财务/aux，全部因果）到最新交易日；
2. 对入选策略逐日跑信号（复用 `_strategy_signal`），冷却期照常；
3. 按 `get_next_day_recommendations` 同款打分：`total_score = Σ 命中策略的 IS 胜率`，按（策略数, 平均胜率）降序输出前 N 只，标注每策略对应持有期；
4. 输出到日志/文件，可接现有 `run_main_program_for_stocks` 调主程序做个股决策。

## 9. 重训练时机

| 触发 | 规则 | 说明 |
|---|---|---|
| **主触发（固化）** | 日历驱动：每 6M 步长边界（每年 1/1、7/1 前后最近交易日），且该 train 窗口已完整 | 重算全量 WFO → 更新部署工件。示例：W4 的 train 到 2026-06-30 完整，2026-07-01 起部署 W4 模型；下次 2027-01-01 触发 W5 |
| 兜底 | 首次运行 / 工件缺失时强制重建 | 避免"无工件可用" |
| 可选（默认关闭） | `--force-retrain` 显式触发 | 供人工在市场 regime 剧变或在线 OOS 滚动胜率跌破阈值时使用 |

固化原则：**默认只按时间重训，提前重训必须显式开启**——避免"感觉不对就重训"变成另一种过拟合。

## 10. 防前视与口径一致性（不变量）

1. **IS/OOS 边界**：选择逻辑只读 IS 切片统计；OOS 只用冻结结果。任何"顺便看一眼 OOS"的统计都不得进入选择；
2. **预计算无泄漏**：指标/regime/财务/aux/动态退出全部因果（向后看或 ann_date 对齐），在全量帧上预计算后切片不产生泄漏；`ret_{p}d` 是标签（前视），窗口内属正常标注，val 尾部自然为 NaN（与现状一致）；
3. **过滤/成本/冷却**：IS 与 OOS 完全同码（`_strategy_signal` + `_apply_cooldown` + 扣 `TRADING_COST_PCT`），一个入口两处调用；
4. **每日选股**只用部署模型 + 截至今日的数据，禁止回看未来。

## 11. 实施落点（最大复用，不新增平行实现）

- **新建** `scripts/backtest_wfo.py`（预计 400~500 行），`from backtest_5y_23strategies import ...` 复用 §2 列出的全部函数；
- **对现有脚本的最小侵入改动**：
  - `_backtest_single` 增加参数 `select_period_by: str = "total_return"`（默认保持现有行为，WFO 传 `"expectation"`）；
  - 抽出 `_compute_ensemble_weights(component_results: Dict[str, Dict])`（从 `run_backtests` L2118-2135 提取，两处共用；入参为组件策略回测结果 dict，避免重复回测组件）。
- 新增构件（均在 `backtest_wfo.py`）：
  - `build_window_grid(data_end)` → 窗口列表（吸附交易日）；
  - `run_is_selection(df_all, window)` → IS 指标表 + 入选集 + best_p + 组合权重；
  - `run_oos_eval(df_all, window, selection, price_paths)` → 逐窗口 OOS + 衰减 + churn（交易表含 `exit_date`）；
  - `aggregate_oos(windows, results, data_end, price_paths)` → 按激活映射按天去重聚合（回撤/年化/夏普用真实组合曲线）+ 基准对比 + Spearman；
  - `save/load_deployed_model()` → 工件读写；
  - `daily_select()` → 每日选股入口。
- **CLI**：
  ```
  python scripts/backtest_wfo.py --mode research   # 全量 WFO：窗口网格 + IS/OOS + 聚合 + 工件
  python scripts/backtest_wfo.py --mode deploy    # 仅重建部署工件（--force-retrain 强制）
  python scripts/backtest_wfo.py --mode daily     # 每日选股（读工件 + 最新数据）
  ```
- **产出物**：`result/wfo_report_<date>.md`（逐窗口 IS/OOS 表 + 聚合 OOS + WFE + 排序相关性 + churn）、`result/wfo_model_latest.json`、`logs/backtest_wfo_<date>.log`（沿用现有日志风格）。

## 12. 验证计划

- `python -m py_compile scripts/backtest_wfo.py scripts/backtest_5y_23strategies.py`；
- 现有离线测试：`python -m pytest -m "not network"`（确认无回归）；
- 数据就绪检查：SQL 校验各核心表 `min(trade_date) <= 2021-01-01`；
- 行为验证：`--mode research` 跑通后核对——窗口数（当前应为 4）、W1/W2 完整 OOS、聚合 OOS 交易日去重无重复、工件 JSON 字段完整；
- 文档同步：本设计文档定稿后，`docs/CHANGELOG.md` 的 `[Unreleased]` 段按扁平格式登记。

## 13. 风险与边界

| 风险 | 说明 | 缓解 |
|---|---|---|
| 相邻 train 窗口 87.5% 数据重叠 | 步长 6M vs 训练 4Y 的固有代价，属标准 WFO 实践 | 接受；用窗口间策略集 churn 指标监控选择是否漂移 |
| OOS 窗口互相重叠（步长 < 验证长） | 聚合统计重复计数 | **激活映射按天去重**（§4.2），聚合口径与生产一致 |
| 规则阈值冻结 = 结构性失效风险 | 未来交易制度变化（如 T+0、涨跌幅再改）会使固化阈值失真 | 固化 ≠ 永不改：文档标注"阈值变更必须走全量 WFO 重审" |
| `best_p` 口径变更（total_return → expectation） | 与现有脚本选期结果不同 | 已确认；`_backtest_single` 加参数保持默认行为兼容 |
| 部分窗口有完整 train 无完整 OOS（如 W4） | 部署模型几乎没有样本外验证史 | 部署时附带"模型新老程度"提示；完整 OOS 窗口数 < 2 时输出警示 |
| 数据起点 2021-01-01 无 warmup 历史 | W1 的 IS 前 120 交易日指标未预热 | `BURN_IN_TRADING_DAYS = 120` 统一剔除；W2+ 天然避开 |

## 14. 决策记录（已确认）

| # | 决策点 | 结论 |
|---|---|---|
| 1 | `best_p` 选期口径 | 从 `total_return` 改为 `expectation` |
| 2 | 入选策略数 K | 默认 8 |
| 3 | 数据跨度 | warmup 后最早加载 ~2021-01-01 起的数据（与现有锚定统一）；数据获取脚本不足则调整对应 `incremental_*.py` 统一起始日期 |
| 4 | 交付顺序 | 先定稿本文档并同步 CHANGELOG，审阅通过后再实现 |
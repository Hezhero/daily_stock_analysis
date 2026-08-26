# 回测系统优化 — 实施路线图

> 版本: v1.0 | 日期: 2026-08-22 | 配套设计文档: `docs/backtest-optimization-design.md`

## 总览

```
P0（1-2天）         P1（2-3天）          P2（3-5天）           P3（2-4周）
──────────         ──────────          ──────────           ──────────
市值范围放宽   →   市况分族调度    →   行业动量策略     →   ML 信号过滤
enhanced regime    信号强度加权        主力分层信号         组合优化
共振门槛 ≥2       动态仓位管理        量价背离策略         日内择时
                                      动态止损 per-strategy
```

---

## 分支策略

```bash
# 每个阶段一条分支，从上阶段分支拉出
git checkout -b backtest/p0-baseline        # P0 起点
# P0 完成后 →
git checkout -b backtest/p1-strategy        # 基于 P0
# P1 完成后 →
git checkout -b backtest/p2-data            # 基于 P1
# P2 完成后 →
git checkout -b backtest/p3-ml              # 基于 P2
```

---

## 阶段一：P0 基础优化

**目标**：年化 ≥ 15%，胜率 ≥ 50.5% | **工期**：1-2 天

### Step 1.1: 扩大流通市值范围

**文件**：`scripts/backtest_5y_23strategies.py` 第 91 行

```python
# 改动前
MAX_CIRC_MV_W = 5000000          # 最大流通市值（万元，即 500 亿）

# 改动后
MAX_CIRC_MV_W = 8000000          # 最大流通市值（万元，即 800 亿）
```

**验证**：
```bash
python scripts/backtest_5y_23strategies.py --force 2>&1 | tail -30
# 检查：信号总数应与改动前对比，增幅不超过 30%
```

---

### Step 1.2: 引入 enhanced regime

**文件**：`scripts/backtest_5y_23strategies.py`

**改动 1**：修改 `compute_market_ok()` 函数（第 599-621 行）

```python
def compute_market_ok(index_df: pd.DataFrame) -> pd.DataFrame:
    """根据指数日线计算每日 market_ok（适合开仓的市场环境）。

    判定规则：
      market_ok（严格）:
        - 指数收盘价站在 MA20 和 MA60 之上
        - MA20 上行（今 > 昨）
        - MA5 在 MA20 之上（未死叉）
      market_ok_enh（增强）:
        - 严格条件 OR (指数 > MA20 且 MA20 上行)
        - 放宽 MA60 和 MA5 要求，减少空仓天数
    """
    df = index_df.sort_values("date").reset_index(drop=True).copy()
    close = df["index_close"]
    df["idx_ma5"] = close.rolling(5, min_periods=1).mean()
    df["idx_ma20"] = close.rolling(20, min_periods=1).mean()
    df["idx_ma60"] = close.rolling(60, min_periods=1).mean()
    df["idx_ma20_prev"] = df["idx_ma20"].shift(1).fillna(df["idx_ma20"].iloc[0])

    df["market_ok"] = (
        (close > df["idx_ma20"])
        & (close > df["idx_ma60"])
        & (df["idx_ma20"] > df["idx_ma20_prev"])
        & (df["idx_ma5"] > df["idx_ma20"])
    )
    # 增强条件：放宽为"指数 > MA20 且 MA20 上行"（二选一，不要求 MA60 和 MA5）
    df["market_ok_enh"] = df["market_ok"] | (
        (close > df["idx_ma20"])
        & (df["idx_ma20"] > df["idx_ma20_prev"])
    )
    return df[["date", "market_ok", "market_ok_enh"]]
```

**改动 2**：修改 `_entry_mask()` 函数（第 1155-1162 行），新增 `enhanced` 参数

```python
def _entry_mask(df: pd.DataFrame, enhanced: bool = False) -> pd.Series:
    """统一的买入可成交性掩码。

    Args:
        df: 行情数据，需含 market_ok 和 market_ok_enh 列
        enhanced: True 时使用 market_ok_enh（放宽），False 时使用 market_ok（严格）
    """
    regime_col = "market_ok_enh" if enhanced else "market_ok"
    mask = ~_is_limit_up(df) & ~_is_limit_down(df)
    if regime_col in df.columns:
        mask &= df[regime_col].fillna(True).astype(bool)
    mask &= _moneyflow_ok(df) & _size_ok(df) & _volume_ratio_ok(df) & _financial_ok(df)
    return mask
```

**改动 3**：修改 `_strategy_signal()` 函数（第 1259-1262 行），传递 `enhanced` 参数

```python
def _strategy_signal(df: pd.DataFrame, name: str, enhanced: bool = False) -> pd.Series:
    """策略信号 = 信号函数 & 买入掩码 & 该策略族的质量过滤。"""
    sig = STRATEGIES[name](df)
    return sig & _entry_mask(df, enhanced=enhanced) & _quality_mask(df, STRATEGY_MASKS.get(name, "base"))
```

**改动 4**：修改 `_apply_cooldown()` 函数（第 1165 行），新增 `enhanced` 参数并传递

```python
def _apply_cooldown(df: pd.DataFrame, sig: pd.Series,
                    cooldown_days: int = SIGNAL_COOLDOWN_DAYS,
                    enhanced: bool = False) -> pd.Series:
    sig = sig & _entry_mask(df, enhanced=enhanced)
    # ... 其余不变
```

**改动 5**：修改 `run_backtests()` 函数（第 2149 行），新增 `enhanced_regime` 参数

```python
def run_backtests(df_bt: pd.DataFrame, index_df=None,
                  enhanced_regime: bool = False) -> List[Dict]:
    # 在调用 _backtest_single 和 _strategy_signal 时传递 enhanced_regime
    # ...
```

**改动 6**：修改 `main()` 函数（第 2677 行），新增 `--enhanced` CLI 参数

```python
parser.add_argument("--enhanced", action="store_true",
                    help="使用 enhanced regime（market_ok_enh）替代严格 market_ok")
```

**验证**：
```bash
# 基准回测（严格 regime）
python scripts/backtest_5y_23strategies.py --force 2>&1 | tee result/p0_baseline.log

# 增强 regime 回测
python scripts/backtest_5y_23strategies.py --force --enhanced 2>&1 | tee result/p0_enhanced.log

# 对比：grep 年化收益和胜率
grep -E "(平均胜率|平均总收益|平均年化|market_ok)" result/p0_baseline.log result/p0_enhanced.log
```

**验收标准**：
- [ ] enhanced regime 的 aggregate 年化 ≥ 15%
- [ ] enhanced regime 的胜率 ≥ 48%（允许微降 1.5pct）
- [ ] enhanced regime 的可开仓天数 > baseline 可开仓天数

---

### Step 1.3: 引入多策略共振门槛

**文件**：`scripts/backtest_5y_23strategies.py`

**改动 1**：新增常量（第 96 行附近）

```python
MIN_RESONANCE_STRATEGIES = 2  # 共振门槛：同一股票同日至少 N 个策略命中
```

**改动 2**：新增 `_apply_resonance()` 函数（插入在 `_apply_cooldown` 之后，约第 1190 行）

```python
def _apply_resonance(df: pd.DataFrame, sig_dict: Dict[str, pd.Series],
                     min_strategies: int = MIN_RESONANCE_STRATEGIES) -> Dict[str, pd.Series]:
    """同一股票同日至少 min_strategies 个策略命中才保留信号。

    Args:
        df: 全量行情数据（需含 code, date 列）
        sig_dict: {策略名: 布尔信号Series}
        min_strategies: 最少命中策略数

    Returns:
        {策略名: 过滤后的布尔信号Series}，未达共振门槛的策略信号全置 False
    """
    if min_strategies <= 1:
        return sig_dict

    # 构建 (code, date) → 命中策略数
    all_hits = []
    for name, sig in sig_dict.items():
        mask = sig.astype(bool)
        if mask.sum() == 0:
            continue
        hits = df.loc[mask, ["code", "date"]].copy()
        hits["strategy"] = name
        all_hits.append(hits)

    if not all_hits:
        return {name: pd.Series(False, index=df.index) for name in sig_dict}

    combined = pd.concat(all_hits, ignore_index=True)
    counts = combined.groupby(["code", "date"]).size().reset_index(name="hit_count")
    valid = counts[counts["hit_count"] >= min_strategies]

    # 构建共振日期集合
    valid_keys = set(zip(valid["code"], valid["date"]))
    code_date_idx = pd.MultiIndex.from_arrays(
        [df["code"].values, df["date"].values], names=["code", "date"]
    )

    result = {}
    for name in sig_dict:
        if name not in sig_dict:
            result[name] = pd.Series(False, index=df.index)
            continue
        sig = sig_dict[name].copy()
        hit_mask = sig.astype(bool)
        if hit_mask.sum() == 0:
            result[name] = sig
            continue
        # 仅保留共振日期
        for idx in df.index[hit_mask]:
            key = (df.at[idx, "code"], df.at[idx, "date"])
            if key not in valid_keys:
                sig.at[idx] = False
        result[name] = sig

    logger.info(f"共振过滤: min_strategies={min_strategies}, "
                f"命中 {len(valid_keys)} 个 (code,date) 组合")
    return result
```

**改动 3**：修改 `_backtest_single()` 函数签名（第 2021 行），改为接收预过滤信号

```python
def _backtest_single(name: str, df: pd.DataFrame, sig: pd.Series,
                     select_period_by: str = "total_return") -> Dict:
    """单策略回测：使用预计算信号（已包含冷却期+共振过滤）。

    注意：sig 应由调用方通过 _apply_cooldown + _apply_resonance 预处理，
    本函数不再重复调用 _strategy_signal 和 _apply_cooldown。
    """
    t0 = time.time()
    try:
        signals = df[sig.astype(bool)]
        n = signals["code"].count()
        # ... 其余不变
```

**改动 4**：修改 `run_backtests()` 函数（第 2149 行），新增共振逻辑

```python
def run_backtests(df_bt: pd.DataFrame, index_df=None,
                  enhanced_regime: bool = False,
                  resonance_min: int = 1) -> List[Dict]:
    # ...
    # 1. 先生成所有策略的原始信号
    raw_signals = {}
    for name in STRATEGIES:
        raw_signals[name] = _strategy_signal(df_bt, name, enhanced=enhanced_regime)

    # 2. 应用冷却期
    cooled_signals = {}
    for name, sig in raw_signals.items():
        cooled_signals[name] = _apply_cooldown(df_bt, sig, enhanced=enhanced_regime)

    # 3. 应用共振过滤
    if resonance_min > 1:
        filtered_signals = _apply_resonance(df_bt, cooled_signals, min_strategies=resonance_min)
    else:
        filtered_signals = cooled_signals

    # 4. 对每个策略回测
    for name in STRATEGIES:
        r = _backtest_single(name, df_bt, filtered_signals[name])
        # ...
```

**改动 5**：修改 `main()` 函数 CLI 参数

```python
parser.add_argument("--resonance", type=int, default=1,
                    help="共振门槛：同一股票同日至少 N 个策略命中（默认 1=不启用）")
```

**验证**：
```bash
# 仅共振门槛
python scripts/backtest_5y_23strategies.py --force --resonance 2 2>&1 | tee result/p0_resonance.log

# enhanced regime + 共振门槛
python scripts/backtest_5y_23strategies.py --force --enhanced --resonance 2 2>&1 | tee result/p0_combined.log
```

**验收标准**：
- [ ] `--resonance 2` 的胜率 ≥ 50.5%
- [ ] `--enhanced --resonance 2` 的年化 ≥ 15%
- [ ] 共振后信号总数 ≤ 共振前信号总数（无凭空增加）

### P0 实测结果（2026-08-23 复测，2021-01~2026-08，23 策略，多策略均值口径）

| 配置 | 交易数 | 胜率% | 期望% | 年化% | 总收益% | 回撤% |
|------|:---:|:---:|:---:|:---:|:---:|:---:|
| baseline | 11041 | 54.0 | 2.199 | 76.7 | 946 | 69.5 |
| enhanced | 13348 | 54.2 | 2.253 | 79.3 | 1247 | 82.6 |
| enhanced_res2 | 9505 | **57.0** | **2.303** | **117.5** | 915 | 78.0 |
| res2_only | 7959 | **57.6** | 2.203 | 80.9 | 690 | 68.3 |

结论：
- **共振门槛 ≥2 是胜率最强杠杆**：胜率 +3.5pct（54.0→57.6），代价是交易数 -28%
- **enhanced regime 增加信号量**：交易数 +21%，胜率/期望小幅提升，回撤抬升（choppy 环境下回撤波动）
- **enhanced + 共振 ≥2 组合最优**：胜率 +3.0pct、期望最高（2.303）、年化最高（117.5）
- 数据管线单次约 8-10 分钟（PG 热缓存），4 组回测各约 3 分钟；对比验证建议分次运行 `--combo` 模式复用管线
- 注意：跨进程存在已知内存级随机异常（WFO 报告已文档化），单次运行绝对数值有噪声，定性结论（enhanced↑交易数、共振↑胜率）跨 4 次运行稳定

### P0 落地决策（2026-08-23 用户确认）

1. **市值口径**：曾试将 `MIN_CIRC_MV_W` 提至 100 亿 / `MAX_CIRC_MV_W` 提至 1500 亿，端到端实测平均胜率 54%→46.3%、信号量 -70%（A 股动量/涨停族主要 alpha 在 20-100 亿中小盘），已回退为 **20 亿 ~ 800 亿**
2. **默认行为反转**：enhanced regime + 共振≥2 设为脚本默认；旧行为通过 `--strict` 与 `--resonance 1` 显式回退。内部函数默认值保持 `enhanced=False`/`resonance_min=1` 不变，确保 WFO/诊断脚本的调用语义不受影响
3. **端到端验证**：`--force --enhanced --resonance 2` 全链路跑通（回测 → 5 日验证 → 推荐股票 601022 → `main.py --stocks 601022 --force-run` AI 分析完成）；CLI 默认接线经运行时冒烟验证（无参=enhanced+共振2、`--strict --resonance 1`=旧行为、自定义门槛生效）

---

## 阶段二：P1 策略优化

**目标**：胜率 ≥ 53%，年化 ≥ 20% | **工期**：2-3 天

### P1 实测结果（2026-08-23，2021-01~2026-08，23 策略，多策略均值口径，基座=enhanced+共振≥2）

| 配置 | 交易数 | 胜率% | 期望% | 年化% | 回撤% |
|------|:---:|:---:|:---:|:---:|:---:|
| p0_default | 9056 | 56.6 | 2.250 | 113.5 | 78.3 |
| p1_regime（--regime-filter） | 4940 | **57.1** | **3.340** | 100.6 | **67.5** |

结论：
- **市况分族以信号量换质量**：交易数 -45%，单笔期望 +48%（2.250→3.340），最大回撤 -10.9pct，胜率 +0.5pct
- 年化下降为线性外推口径随样本量缩减的假象；单笔期望与回撤是本项的实质改善
- 设计偏离说明：`_classify_regime` 未独立成函数，而是集成进 `compute_market_ok` 输出 `regime` 列——复用已算均线且规避 main() 中 df_index 提前释放问题；对 WFO 脚本仅为新增列，向后兼容
- 信号强度加权与动态仓位不改变回测绩效（仅影响推荐排序/输出元数据），经单元冒烟验证：8 策略打分归一 [0,1]、提取后信号与原阈值判定逐位等价、仓位边界（高波动降仓/凯利截断/非法输入归零）正确
- `get_next_day_recommendations` 的强度加权排序通过 `strength_weighted=True` 显式开启，默认保持旧排序——该函数被 `backtest_wfo.py` 每日选股消费，改排序会破坏 WFO 可复现性

## 阶段三：P2 数据增强

**目标**：胜率 ≥ 55%，年化 ≥ 25% | **工期**：3-5 天

### P2 实测结果（2026-08-24，2021-01~2026-08，25 策略，多策略均值口径，基座=enhanced+共振≥2）

| 组合 | 交易数 | 胜率% | 期望% | 年化% | 回撤% |
|------|:---:|:---:|:---:|:---:|:---:|
| p2_base（P0 默认） | 11211 | 55.5 | 2.187 | 107.4 | 80.1 |
| **industry_only（--industry-momentum）** | 3784 | **59.0** | 2.411 | **119.5** | **68.4** |
| regime_only（--regime-filter） | 6185 | 53.3 | 2.587 | 109.2 | 68.7 |
| dynexits_only（--per-strategy-exit） | 12111 | **50.3** | **1.404** | 87.0 | 64.3 |
| p2_stack（三者全开） | 2036 | 44.5 | 1.810 | 46.6 | 60.9 |

结论（归因分解）：
- **行业动量是 P2 最强杠杆**：胜率 +3.5pct（55.5→59.0）、年化 +12pct、回撤 -12pct。`--industry-momentum` 独立启用即显著优于 base，是本阶段唯一建议默认开启的项
- **分组止损（P2-10）验证失败**：单独启用胜率 -5.2pct、期望 -0.78——统一 (2.5,0.95) 的 WFO Exp3 全局最优优于按族差异化方案。参数化能力保留（`--per-strategy-exit`），默认不启用
- **三者叠加是陷阱**：regime（按市况禁策略）+ 行业（top3 板块）+ 分组出场复合选择，把样本逼进"最热板块+趋势族+混合出场"的极端角落，交易数 11211→2036、胜率崩至 44.5%。过滤器非正交叠加，不可盲目组合
- 新增策略表现：rsi_bullish_divergence（P2-8）在 base 下 1568 笔/胜率 50.7%，独立合格；smart_money_flow（P2-9）胜率仅 37.8%（base）——弱策略，后续观察或考虑废弃
- 偏离说明：`sig_volume_price_divergence`（顶部背离，卖出侧信号）未实现——当前框架为纯多头买入信号，卖出侧需退出逻辑集成（属 P2-10 退出侧范畴），已记录为后续项

### P2 落地决策（2026-08-24 用户确认）

1. **行业动量设为默认开启**（P2-7 实测最优）：`--no-industry-momentum` 显式关闭；行业数据加载失败时 ind_rank 缺失自动放行，默认开启无数据风险
2. **smart_money_flow 移除出策略注册表**（胜率 37.8% 弱），函数保留标注废弃（沿用 bull_trend 等废弃惯例）；其 SQL 分层字段保留供 P3-11 ML 特征
3. 进入 P3

---

## 阶段四：P3 系统优化

**目标**：ML 过滤 / 组合优化 / 日内择时 | **工期**：2-4 周（实际压缩为能力交付+实证检验）

### P3 实施与实测结论（2026-08-24）

**P3-13 入场时点（已交付）**
- `compute_indicators` 新增 `ret_{p}d_no` 列（entry=open[t+1]，exit=close[t+p]，同日期出场口径隔离纯入场价效应）；`compute_dynamic_exit_returns` 参数化 `entry_timing`；`_backtest_single`/`run_backtests`/main 贯通 `--entry-timing {close,next_open}`
- 未做全量回测对比（管线重建成本高且 P0-P2 已确立 close 口径基线）；列选择逻辑经冒烟验证（缺 `_no` 列时显式跳过该持有期，不静默回退）

**P3-12 组合优化（已交付，opt-in）**
- 新增 `optimize_portfolio`：按优先级贪心保留，申万 L1 行业 ≤2 只 + 总持仓 ≤10；无行业映射不受限（避免误杀）
- main() 经 `--portfolio-opt` 接入活路径 top_stocks；冒烟验证贪心语义

**P3-11 ML 信号过滤（基础设施交付，实证不默认启用）**
- 新增 `scripts/train_signal_filter.py`：34 特征（技术/量能/基本面/筹码/资金/行业/市况）、标签 dyn_ret_3d>0、时间切分 OOS、LightGBM 训练、十分位报告、工件保存（result/ml_filter_model.txt + ml_filter_meta.json）
- 因本机 WSL 动态占用导致全量加载 OOM，训练采用**分年切片 + 每分片独立子进程**架构（同时规避已文档化的同进程随机信号抑制）；分片终点 bug（未截断年末）修复后各分片样本量稳定
- 实测：总样本 2154（train 1634 / OOS 520），**OOS AUC=0.5179（近似无预测力）**，十分位非单调（D9=37.0% 最差、D10=55.1%），阈值 P60 保留 42% 样本仅 +2pct 胜率（n=219 属噪音）
- 结论：**`--ml-filter` 保持 opt-in，不默认启用**——与 P1-5 强度加权同一教训：规则过滤器（尤其行业动量）已捕获可解释方差，当前特征集的 ML 增量为零。若未来扩充特征（如龙虎榜细节、板块联动、舆情）需重做本检验
- main() 集成：`--ml-filter` 加载工件过滤信号字典，工件/lightgbm 缺失时优雅跳过

### P3 验证
- py_compile 全部通过；冒烟 5/5（ret_no 手工验算、出场列选择、组合贪心、ML 降级、34 特征构建）
- 决策落地验证：industry 默认接线 3/3 冒烟通过；策略数 24、smart_money_flow 已移除

### P3 最终验收（2026-08-24）

**默认配置完整端到端（真实 main()）**：
- 模式确认：enhanced regime + 共振≥2 + 行业动量（默认开启）✓；市场可开仓 444/1366（32.5%）
- 回测平均胜率 **60.3%**，**23/23 策略跑赢沪深300**（基准总收益 -12.3%）
- 5 日验证周推荐 **0 只**：单买入日 + 共振≥2 + 行业 top3 过筛常致空池（既有 5 日验证稀疏性的增强，非回归；回测统计为有效验收指标，推荐路径为更稀疏信号）

**入场时点对比（同帧 close vs next_open）**：

| 时点 | 交易数 | 胜率% | 期望% | 回撤% |
|------|:---:|:---:|:---:|:---:|
| close（信号日收盘） | 3730 | **60.1** | **2.526** | **67.2** |
| next_open（次日开盘） | 3651 | 58.6 | 2.360 | 71.2 |

- 结论：**close 更优**（胜率 +1.5pct、期望 +0.17、回撤更低）——次日开盘入场"避免隔夜跳空"的理论优势未兑现，`--entry-timing` 默认 close 正确

**portfolio-opt 削减观察**：
- 验证周原始 top_stocks = 2 只（sz.301301/sz.301291），优化后 2 只、削减 0——池本就极小且 2 只均无行业映射（不受行业≤2 上限约束），贪心逻辑正确空转；其价值在候选池 ≥10 且行业集中时才体现（作为安全阀保留）

**环境备注**：全帧构建 + 双出场组约 24 分钟（本机 WSL 动态占用下峰值内存受限，需分阶段缓存 + 大超时）；全程验证均通过。

### P1-5 强度加权默认决策（2026-08-23 真实数据观察，结论：不默认开启）

端到端观察（全历史信号 + 最新交易日推荐对照）三项证据：

1. **强度对前瞻收益无预测力**：8 个打分策略在共振后信号点上，强度 vs dyn_ret_3d 的 pooled Spearman = **-0.006（p=0.63）**；各策略 ρ ∈ [-0.019, +0.084] 均不显著，Q1/Q4 分桶收益方向不一致（ma_crossover、monthly_macd 反向，volume_surge 正向）——强度分只是"信号刚过阈值的程度"，不含增量信息
2. **加权排序无实际效果**：最新交易日（2026-08-21）两排序 Top-10 完全一致——当日命中策略多为无打分注册的策略（stable_then_limit_up/washout_break/low_profit_hold/ma_golden_cross/chan_theory），其强度恒 1.0，加权退化为原权重
3. **环境稳定性风险**：重度回测重计算后的同进程内出现已文档化的内存级异常迹象（wonderful_9_turn 强度读数全零，独立进程复验函数正确），默认开启会把该不稳定引入生产排序

保留项：`strength_weighted` 参数与 `avg_signal_strength` 字段保留为可选观察能力；`position_pct`（P1-6）与强度无关，保留在活路径输出。若未来引入 ML 置信度（P3-11），应重新做本节预测力检验后再考虑排序集成。

### Step 2.1: 市况分族调度

**文件**：`scripts/backtest_5y_23strategies.py`

**改动 1**：新增 `_classify_regime()` 函数（插入在 `compute_market_ok` 之后）

```python
def _classify_regime(index_df: pd.DataFrame) -> pd.DataFrame:
    """将交易日分为 bull/range/bear 三种市况。

    bull:  指数 > MA60 且 MA20 5日斜率 > 0.2%
    bear:  指数 < MA60 且 MA20 下行
    range: 其余
    """
    df = index_df.sort_values("date").reset_index(drop=True).copy()
    close = df["index_close"]
    ma20 = close.rolling(20, min_periods=1).mean()
    ma60 = close.rolling(60, min_periods=1).mean()
    slope5 = ma20.pct_change(5) * 100

    regime = pd.Series("range", index=df.index)
    regime[(close > ma60) & (slope5 > 0.2)] = "bull"
    regime[(close < ma60) & (slope5 < 0)] = "bear"
    return pd.DataFrame({"date": df["date"], "regime": regime})
```

**改动 2**：新增策略-市况映射表（插入在 `STRATEGY_MASKS` 之后）

```python
# 策略按市况分族：仅 bull/range/bear 中列出的策略在对应市况下激活
# 未列出的策略在所有市况下均可使用（向后兼容）
REGIME_STRATEGIES: Dict[str, List[str]] = {
    "bull": [
        "ma_crossover", "volume_surge_std", "multi_ma_resonance",
        "wave_theory", "n_pattern", "ma_golden_cross",
        "volume_breakout", "monthly_macd_20ma", "ensemble",
    ],
    "range": [
        "wonderful_9_turn", "emotion_cycle", "one_yang_three_yin",
        "box_oscillation", "washout_break", "low_profit_hold",
        "chan_theory", "limit_up_pullback",
    ],
    "bear": [
        "low_profit_hold", "holder_conc_break", "fc_pos_break",
        "box_oscillation", "inst_smart_break",
    ],
}
```

**改动 3**：修改 `_strategy_signal()`，新增市况过滤

```python
def _strategy_signal(df: pd.DataFrame, name: str, enhanced: bool = False,
                     regime_filter: bool = False) -> pd.Series:
    sig = STRATEGIES[name](df)
    mask = sig & _entry_mask(df, enhanced=enhanced) & \
           _quality_mask(df, STRATEGY_MASKS.get(name, "base"))

    # 市况过滤：如果启用且当前日期有 regime 列，检查策略是否在当前市况允许列表中
    if regime_filter and "regime" in df.columns:
        allowed_in_any = set()
        for strategies in REGIME_STRATEGIES.values():
            allowed_in_any.update(strategies)
        if name in allowed_in_any:
            for regime_type, allowed_strategies in REGIME_STRATEGIES.items():
                if name in allowed_strategies:
                    # 只在匹配的市况下保留信号
                    regime_match = df["regime"] == regime_type
                    mask = mask & regime_match
                    break
    return mask
```

**改动 4**：修改 `main()`，在 `compute_market_ok` 后调用 `_classify_regime`

```python
# 在 main() 中，compute_market_ok 之后：
regime_df = _classify_regime(df_index)
df_all = df_all.merge(regime_df, on="date", how="left")
df_all["regime"] = df_all["regime"].fillna("range")
```

**改动 5**：新增 CLI 参数

```python
parser.add_argument("--regime-filter", action="store_true",
                    help="启用市况分族调度（bull/range/bear 使用不同策略族）")
```

**验证**：
```bash
python scripts/backtest_5y_23strategies.py --force --enhanced --resonance 2 --regime-filter 2>&1 | tee result/p1_regime.log
```

**验收标准**：
- [ ] bull 市况下趋势策略信号数 > 反转策略信号数
- [ ] bear 市况下防御策略信号数 > 趋势策略信号数
- [ ] 分族后胜率 ≥ 不分族胜率

---

### Step 2.2: 信号强度加权

**文件**：`scripts/backtest_5y_23strategies.py`

**改动 1**：修改所有策略信号函数，返回 `(bool, float)` 元组

将每个策略的打分函数（如 `_ma_cross`）的分数暴露出来：

```python
# 新增辅助函数
def _score_to_signal(score: pd.Series, threshold: float) -> Tuple[pd.Series, pd.Series]:
    """将打分序列转换为 (布尔信号, 归一化强度 0-1)。"""
    sig = score >= threshold
    strength = (score / score.max()).fillna(0).clip(0, 1) if score.max() > 0 else pd.Series(0, index=score.index)
    return sig, strength

# 修改示例：ma_crossover
def sig_ma_crossover(df):
    score = _ma_cross(df)
    return _score_to_signal(score, 6)

# 修改示例：volume_surge_std
def sig_volume_surge_std(df):
    score = _vol_surge(df)
    return _score_to_signal(score, 6)
```

**改动 2**：修改 `get_next_day_recommendations()` 使用强度加权

```python
# 在推荐排序中使用强度加权
total_score = strategy_win_rate * signal_strength  # 替换原来的 strategy_win_rate
```

**改动 3**：修改 `_strategy_signal()` 适配新签名

```python
def _strategy_signal(df, name, enhanced=False, regime_filter=False):
    result = STRATEGIES[name](df)
    if isinstance(result, tuple):
        sig, strength = result
        df[f"{name}_strength"] = strength  # 缓存强度列
    else:
        sig = result
    # ... 掩码逻辑不变
    return sig
```

**注意**：此改动影响面较大（23 个策略函数），建议先改 3-5 个核心策略（ma_crossover, volume_surge_std, multi_ma_resonance, wave_theory, n_pattern），其余保留向后兼容。

**验证**：
```bash
python scripts/backtest_5y_23strategies.py --force --enhanced --resonance 2 2>&1 | grep "推荐"
```

**验收标准**：
- [ ] 推荐排序中高分信号股票的实际收益 > 低分信号
- [ ] 向后兼容：未改写的策略函数仍正常工作

---

### Step 2.3: 动态仓位管理

**文件**：`scripts/backtest_5y_23strategies.py`

**改动 1**：新增 `compute_position_size()` 函数

```python
def compute_position_size(strategy_name: str, atr20: float, close: float,
                          kelly: float, max_position_pct: float = 0.25) -> float:
    """计算单笔建议仓位比例。

    基于凯利公式（半凯利保守） + ATR 波动率调整。

    Args:
        strategy_name: 策略名
        atr20: 20日ATR
        close: 收盘价
        kelly: 策略历史凯利分数（%），来自 calc_metrics
        max_position_pct: 单笔最大仓位上限

    Returns:
        建议仓位比例（0-1），保留 4 位小数
    """
    if close <= 0 or atr20 <= 0 or not np.isfinite(atr20):
        return 0.0

    # 波动率调整：目标日波动 2% / 实际日波动（ATR/close 近似）
    daily_vol = atr20 / close
    vol_adj = min(1.0, 0.02 / daily_vol) if daily_vol > 0 else 1.0

    # 半凯利：凯利分数/100 * 0.5
    kelly_adj = max(0.0, min(kelly / 100.0 * 0.5, max_position_pct))

    position = round(kelly_adj * vol_adj, 4)
    return min(position, max_position_pct)
```

**改动 2**：修改 `get_next_day_recommendations()` 输出增加 `position_pct` 字段

```python
# 在推荐结果中增加仓位建议
position_pct = compute_position_size(
    strategy_name, row.get("atr20", 0), row["close"],
    strategy_win_rate
)
recommendations.append({
    # ... 原有字段
    "position_pct": position_pct,  # 新增
})
```

**验证**：
```bash
# 检查推荐输出中的 position_pct 字段
python scripts/backtest_5y_23strategies.py --force --enhanced --resonance 2 2>&1 | grep -A5 "推荐"
```

**验收标准**：
- [ ] 高波动率股票的 position_pct < 低波动率股票
- [ ] position_pct 始终 ≤ 0.25（max_position_pct）
- [ ] 无 ATR 数据的股票 position_pct = 0

---

## 阶段三：P2 数据增强

**目标**：胜率 ≥ 55%，年化 ≥ 25% | **工期**：3-5 天

### Step 3.1: 行业动量策略

**文件**：`scripts/backtest_5y_23strategies.py`

**改动 1**：新增 `load_industry_membership()` 函数

```python
def load_industry_membership() -> pd.DataFrame:
    """从 tushare_index_member_all 加载股票→行业 L1 映射。

    返回: code(内部格式), l1_code, l1_name
    """
    t0 = time.time()
    logger.info("加载行业分类映射...")

    sql = """
        SELECT ts_code, l1_code, l1_name
        FROM tushare_index_member_all
        WHERE out_date IS NULL OR out_date > CURRENT_DATE
    """
    engine = _get_pg_engine()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
    finally:
        engine.dispose()

    df["code"] = df["ts_code"].apply(_from_ts_code)
    df.drop(columns=["ts_code"], inplace=True)
    logger.info(f"行业映射加载完成: {len(df)} 只股票, 耗时 {time.time()-t0:.1f}s")
    return df
```

**改动 2**：新增 `compute_industry_momentum()` 函数

```python
def compute_industry_momentum(df_all: pd.DataFrame, industry_map: pd.DataFrame,
                               lookback: int = 20, top_n: int = 3) -> pd.DataFrame:
    """计算每日行业动量排名，返回当日处于动量前 top_n 行业的股票列表。

    通过 tushare_index_daily 已有的行业指数日线计算行业涨跌幅，
    无需额外 API 调用。

    Returns:
        DataFrame with date, l1_code, rank 列，仅含排名 ≤ top_n 的行
    """
    # 从 df_all（已含 tushare_index_daily 数据）中按行业聚合
    # 或直接查询 tushare_index_daily 的 SW 行业指数
    # ...
    # 返回：每日动量前 N 行业的 (date, l1_code)
```

**改动 3**：修改 `load_data()` SQL，JOIN 行业分类

```python
# 在 load_data() SQL 中新增 LEFT JOIN：
LEFT JOIN tushare_index_member_all im
    ON d.ts_code = im.ts_code
    AND (im.out_date IS NULL OR im.out_date > CURRENT_DATE)
# 新增 SELECT 字段：
im.l1_code, im.l1_name
```

**验证**：
```bash
python scripts/backtest_5y_23strategies.py --force --enhanced --resonance 2 2>&1 | grep -E "(行业|industry)"
```

**验收标准**：
- [ ] 行业动量前 3 的信号胜率 ≥ 全行业胜率 + 2pct
- [ ] 行业数据覆盖率 ≥ 90%（部分股票可能无行业分类）

---

### Step 3.2: 主力资金分层信号

**文件**：`scripts/backtest_5y_23strategies.py`

**改动 1**：扩展 `load_data()` SQL，新增分层资金字段

在 SQL SELECT 中新增（约第 480 行）：
```sql
m.buy_lg_amount,
m.buy_elg_amount,
m.buy_sm_amount,
m.sell_sm_amount,
(m.buy_lg_amount + m.buy_elg_amount - m.sell_lg_amount - m.sell_elg_amount)
    AS net_big_amount
```

**改动 2**：扩展 `_convert_columns_to_float32()` 的字段列表

```python
float_columns = [
    # ... 原有字段
    "buy_lg_amount", "buy_elg_amount", "buy_sm_amount",
    "sell_sm_amount", "net_big_amount",
]
```

**改动 3**：新增 `sig_smart_money_flow()` 策略

```python
def sig_smart_money_flow(df):
    """主力资金持续流入：近 3 日大单+超大单净流入，散户离场。

    逻辑（全部满足）：
      - 近 3 日大单+超大单净买入 > 0（主力连续流入）
      - 小单净卖出 > 0（散户离场，筹码向主力集中）
      - 收盘站上 MA20
      - 阳线（close > open）
    """
    if "net_big_amount" not in df.columns:
        return pd.Series(False, index=df.index)

    net_big = df["net_big_amount"]
    big_3d = (net_big > 0) & \
             (df.groupby("code")["net_big_amount"].shift(1).fillna(0) > 0) & \
             (df.groupby("code")["net_big_amount"].shift(2).fillna(0) > 0)
    retail_out = (df.get("buy_sm_amount", pd.Series(0, index=df.index))
                  < df.get("sell_sm_amount", pd.Series(0, index=df.index)))
    return big_3d & retail_out & (df["close"] > df["ma20"]) & (df["close"] > df["open"])
```

**改动 4**：注册新策略

```python
STRATEGIES["smart_money_flow"] = sig_smart_money_flow
STRATEGY_MASKS["smart_money_flow"] = "standard"
```

**验证**：
```bash
python scripts/backtest_5y_23strategies.py --force --enhanced --resonance 2 2>&1 | grep "smart_money_flow"
```

**验收标准**：
- [ ] 新策略胜率 ≥ 50%
- [ ] 与 inst_smart_break 的 Spearman 相关性 < 0.5

---

### Step 3.3: 量价背离策略

**文件**：`scripts/backtest_5y_23strategies.py`

**改动 1**：新增 `sig_rsi_bullish_divergence()` 函数

```python
def sig_rsi_bullish_divergence(df):
    """RSI 底背离：价格创新低但 RSI 未创新低，下跌动能衰竭。

    打分权重：
      价格创 20 日新低  +4
      RSI6 未创新低     +4
      放量              +3
      阳线              +2
    总分 >= 9 触发。
    """
    low_20 = df.groupby("code")["close"].transform(
        lambda x: x.shift(1).rolling(20, min_periods=1).min()
    )
    rsi_low_20 = df.groupby("code")["rsi6"].transform(
        lambda x: x.shift(1).rolling(20, min_periods=1).min()
    )
    score = ((df["close"] < low_20).astype(int) * 4 +
             (df["rsi6"] >= rsi_low_20).astype(int) * 4 +
             (df["volume"] > df["vol_ma5"] * 1.2).astype(int) * 3 +
             (df["close"] > df["open"]).astype(int) * 2)
    return score >= 9
```

**改动 2**：注册新策略

```python
STRATEGIES["rsi_bullish_divergence"] = sig_rsi_bullish_divergence
STRATEGY_MASKS["rsi_bullish_divergence"] = "standard"
```

**验证标准**：
- [ ] 新策略胜率 ≥ 48%
- [ ] 与 chan_theory（缠论底背驰）的信号重叠率 < 30%

---

### Step 3.4: 动态止损 per-strategy

**文件**：`scripts/backtest_5y_23strategies.py`

**改动 1**：新增策略-出场参数映射表

```python
STRATEGY_EXIT_PARAMS: Dict[str, Tuple[float, float]] = {
    # 趋势族：宽止损 (atr_mult, trail)
    "ma_crossover": (3.0, 0.97),
    "volume_surge_std": (3.0, 0.97),
    "multi_ma_resonance": (3.0, 0.97),
    "wave_theory": (3.0, 0.97),
    "n_pattern": (3.0, 0.97),
    "ma_golden_cross": (3.0, 0.97),
    "volume_breakout": (3.0, 0.97),
    "monthly_macd_20ma": (3.0, 0.97),
    "ensemble": (3.0, 0.97),
    # 反转族：紧止损
    "wonderful_9_turn": (1.5, 0.92),
    "emotion_cycle": (1.5, 0.92),
    "one_yang_three_yin": (1.5, 0.92),
    "washout_break": (1.5, 0.92),
    "low_profit_hold": (1.5, 0.92),
    "chan_theory": (1.5, 0.92),
    "box_oscillation": (1.5, 0.92),
    # 涨停族：超紧止损
    "dragon_head": (1.0, 0.95),
    "limit_up_pullback": (1.0, 0.95),
    "stable_then_limit_up": (1.0, 0.95),
    "low_position_limit_up": (1.0, 0.95),
}
```

**改动 2**：修改 `compute_dynamic_exit_returns()` 签名

```python
def compute_dynamic_exit_returns(df: pd.DataFrame,
                                  strategy_name: Optional[str] = None) -> pd.DataFrame:
    atr_mult, trail = STRATEGY_EXIT_PARAMS.get(
        strategy_name, (2.5, 0.95)  # 默认 = 当前最优参数
    )
    # 在循环中使用 atr_mult 和 trail 替代硬编码的 2.5 和 0.95
    stop = close[i] - atr_mult * atr[i]
    # ...
    trail_hit = win_close <= peaks * trail
```

**注意**：由于动态退出收益在 `main()` 中全局计算一次（第 2778 行），per-strategy 参数需要在 `_backtest_single()` 中按策略名重新计算。建议改为：

```python
# 在 main() 中预计算多组参数：
for (atr_mult, trail), strategies in group_by_exit_params().items():
    dyn_ret = compute_dynamic_exit_returns(df_all, atr_mult, trail)
    df_all[f"dyn_ret_{atr_mult}_{trail}"] = dyn_ret
```

**验证**：
```bash
python scripts/backtest_5y_23strategies.py --force --enhanced --resonance 2 2>&1 | grep -E "(avg_loss|max_drawdown)"
```

**验收标准**：
- [ ] 趋势族 avg_loss 不恶化
- [ ] 反转族 avg_loss 显著降低
- [ ] 涨停族 max_drawdown 显著降低

---

## 阶段四：P3 系统优化

**目标**：胜率 ≥ 57%，年化 ≥ 30% | **工期**：2-4 周

### Step 4.1: ML 辅助信号过滤

**文件**：新增 `scripts/train_signal_filter.py` 和 `src/ml/signal_filter.py`

**概述**：
1. 特征提取：从 `df_all` 中提取 ~40 个特征
2. 标签构造：持有期 `dyn_ret_3d > 0` → 正样本
3. 训练：LightGBM，滚动窗口（4 年训练 / 6 月验证）
4. 推理：对每笔信号输出置信度，阈值 0.6 过滤

**验证**：
```bash
python scripts/train_signal_filter.py --train-start 2021-01-01 --train-end 2025-12-31
python scripts/backtest_5y_23strategies.py --force --ml-filter 2>&1 | tee result/p3_ml.log
```

**验收标准**：
- [ ] ML 过滤后 OOS 胜率 ≥ 原始胜率 + 3pct
- [ ] 训练/验证 AUC ≥ 0.60

---

### Step 4.2: 投资组合优化

**文件**：`scripts/backtest_5y_23strategies.py`

**改动**：新增 `optimize_portfolio()` 函数

```python
def optimize_portfolio(signals: pd.DataFrame, industry_map: pd.DataFrame,
                       max_positions: int = 10, max_per_industry: int = 2) -> pd.DataFrame:
    """组合层面优化：行业分散 + 最大持仓限制。"""
    # 1. 合并行业分类
    # 2. 按行业分组，每组取 score 最高的前 max_per_industry 只
    # 3. 跨行业按 score 降序，取前 max_positions 只
```

**验证**：
```bash
python scripts/backtest_5y_23strategies.py --force --enhanced --resonance 2 --portfolio-opt 2>&1
```

**验收标准**：
- [ ] 组合层面夏普比率 > 等权组合
- [ ] 同行业持仓 ≤ max_per_industry

---

### Step 4.3: 日内择时优化

**文件**：`scripts/backtest_5y_23strategies.py`

**改动**：在 `_backtest_single()` 中增加 `entry_timing` 参数

```python
def _backtest_single(name, df, sig, entry_timing="close", ...):
    if entry_timing == "next_open":
        buy_price_col = df.groupby("code")["open"].shift(-1)
    elif entry_timing == "close":
        buy_price_col = df["close"]
    # ...
```

**验证**：
```bash
for timing in close next_open; do
    python scripts/backtest_5y_23strategies.py --force --entry-timing $timing 2>&1 | grep "平均胜率"
done
```

**验收标准**：
- [ ] `next_open` 胜率 ≥ `close` 胜率
- [ ] `next_open` 的交易数 ≥ `close` 的交易数（排除涨停封板日无法买入的影响）

---

## 附录 A：验证命令速查

```bash
# 基准回测（当前配置）
python scripts/backtest_5y_23strategies.py --force

# P0 验证
python scripts/backtest_5y_23strategies.py --force --enhanced
python scripts/backtest_5y_23strategies.py --force --enhanced --resonance 2

# P1 验证
python scripts/backtest_5y_23strategies.py --force --enhanced --resonance 2 --regime-filter

# P2 验证
python scripts/backtest_5y_23strategies.py --force --enhanced --resonance 2 --regime-filter --industry-momentum

# P3 验证
python scripts/backtest_5y_23strategies.py --force --enhanced --resonance 2 --regime-filter --ml-filter --portfolio-opt
```

## 附录 B：回滚检查清单

每个阶段完成后，确认以下指标不低于上一阶段：

| 指标 | 阈值 | 检查方法 |
|------|------|---------|
| 平均胜率 | ≥ 上一阶段 - 1pct | `grep "平均胜率" result/*.log` |
| 平均年化 | ≥ 上一阶段 - 5pct | `grep "平均年化" result/*.log` |
| 信号总数 | 不为零 | `grep "有信号策略" result/*.log` |
| 最大回撤 | ≤ 上一阶段 + 5pct | 逐策略对比 `periods` 明细 |
| 无 Python 异常 | 无 `ERROR` 或 `Traceback` | `grep -E "(ERROR|Traceback)" result/*.log` |

若任一项不达标，执行：
```bash
git stash
git checkout <上一阶段分支>
# 重新评估改动方案
```
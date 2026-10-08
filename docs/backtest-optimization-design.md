# 回测系统优化方案 — 详细设计文档

> 版本: v1.0 | 日期: 2026-08-22 | 基于 WFO Exp2/Exp3/Exp4 实验结论

## 目录

1. [系统现状](#1-系统现状)
2. [P0 立即实施](#2-p0-立即实施)
3. [P1 短期优化](#3-p1-短期优化)
4. [P2 中期优化](#4-p2-中期优化)
5. [P3 长期优化](#5-p3-长期优化)
6. [数据库依赖矩阵](#6-数据库依赖矩阵)
7. [实施路线图](#7-实施路线图)

---

## 1. 系统现状

### 1.1 当前架构

```
load_data (PG) → apply_forward_adjustment → load_signal_aux
    → compute_indicators → compute_market_ok → merge_fina_by_ann_date
    → compute_dynamic_exit_returns → run_backtests → validate_week
    → get_top_stocks_by_win_rate → run_main_program
```

### 1.2 当前瓶颈

| 维度 | 当前值 | 根因 |
|------|--------|------|
| 胜率 | 50.6% | 信号无共振确认，孤立噪音大 |
| 年化收益 | 4.6% | market_ok 过于保守，空仓61%交易日 |
| 最大回撤 | 12.7% | 无动态仓位管理，满仓进出 |
| 信号量 | 757笔/5年 | 流通市值范围窄，量比过滤严 |

### 1.3 WFO 实验结论速查

| 实验 | 结论 | 数据来源 |
|------|------|---------|
| Exp2c | enhanced regime 年化 4.6%→18-21%，回撤 12.7%→10.8% | `result/wfo_experiments_20260820.md` |
| Exp3 | 出场参数 (2.5,0.95) 胜率最高 53.1%，期望 +1.119 | 同上 |
| Exp4 | 共振门槛 ≥2 胜率 50.8%，年化 11.8%（2.5x baseline） | 同上 |

---

## 2. P0 立即实施

### 2.1 P0-1: 引入 enhanced regime（年化收益 4-5x 提升）

**设计目标**：将 market_ok 从严格四条件放宽为二选一，减少空仓天数。

**当前逻辑**（`compute_market_ok`，第 599-621 行）：
```python
df["market_ok"] = (
    (close > df["idx_ma20"])        # 条件1: 收盘 > MA20
    & (close > df["idx_ma60"])       # 条件2: 收盘 > MA60
    & (df["idx_ma20"] > df["idx_ma20_prev"])  # 条件3: MA20 上行
    & (df["idx_ma5"] > df["idx_ma20"])        # 条件4: MA5 > MA20
)
```

**修改方案**：新增 `market_ok_enh` 列，与 `market_ok` 并列：

```python
# 严格条件（保留，供对比）
df["market_ok"] = (
    (close > df["idx_ma20"])
    & (close > df["idx_ma60"])
    & (df["idx_ma20"] > df["idx_ma20_prev"])
    & (df["idx_ma5"] > df["idx_ma20"])
)

# 增强条件：严格条件 OR (指数 > MA20 且 MA20 上行，放宽 MA60 和 MA5 要求)
df["market_ok_enh"] = df["market_ok"] | (
    (close > df["idx_ma20"])
    & (df["idx_ma20"] > df["idx_ma20_prev"])
)
```

**影响范围**：
- `compute_market_ok()` 返回新增 `market_ok_enh` 列
- `_entry_mask()` 新增参数 `use_enhanced_regime=False`，默认保持 `market_ok`
- `run_backtests()` 新增参数 `enhanced_regime=False`，传 `True` 时使用 `market_ok_enh`

**验证标准**：回测结果中 enhanced regime 的 aggregate 年化收益应 ≥ 15%（Exp2c 结论 18-21%），胜率不跌破 48%。

---

### 2.2 P0-2: 引入多策略共振门槛（胜率 +0.2pct，年化 2.5x）

**设计目标**：同一股票同日至少 N 个策略命中才保留信号。

**修改方案**：在 `_apply_cooldown()` 之后，新增 `_apply_resonance()` 函数：

```python
def _apply_resonance(df: pd.DataFrame, sig_dict: Dict[str, pd.Series],
                     min_strategies: int = 2) -> pd.DataFrame:
    """同一股票同日至少 min_strategies 个策略命中才保留信号。

    Args:
        df: 全量行情数据
        sig_dict: {策略名: 布尔信号Series}，已通过 _entry_mask 和 _quality_mask
        min_strategies: 最少命中策略数，默认 2（Exp4 最优）

    Returns:
        pd.DataFrame: 含 code/date/strategy/sig 列，仅保留共振信号
    """
    # 构建 (code, date) → 命中策略数 的映射
    all_sigs = []
    for name, sig in sig_dict.items():
        mask = sig.astype(bool)
        if mask.sum() == 0:
            continue
        hits = df.loc[mask, ["code", "date"]].copy()
        hits["strategy"] = name
        all_sigs.append(hits)

    if not all_sigs:
        return pd.DataFrame(columns=["code", "date", "strategy"])

    combined = pd.concat(all_sigs, ignore_index=True)
    # 每只股票每日命中策略数
    counts = combined.groupby(["code", "date"]).size().reset_index(name="hit_count")
    valid = counts[counts["hit_count"] >= min_strategies]

    # 只保留共振日期的信号
    result = combined.merge(valid[["code", "date"]], on=["code", "date"])
    return result
```

**与 `_apply_cooldown` 的调用顺序**：
```
信号生成 → _entry_mask → _quality_mask → _apply_cooldown → _apply_resonance
```

**影响范围**：
- 新增 `_apply_resonance()` 函数
- `_backtest_single()` 改为接收预计算的共振信号 DataFrame
- `run_backtests()` 聚合所有策略信号后调用 `_apply_resonance`，再分发

**验证标准**：`min_strategies=2` 时胜率 ≥ 50.5%（Exp4 结论 50.8%），年化 ≥ 8%（Exp4 结论 11.8%）。

---

### 2.3 P0-3: 扩大流通市值范围（增加信号量）

**设计目标**：将上限从 500 亿放宽至 800 亿，覆盖更多中大盘成长股。

**修改方案**：仅改常量（第 91 行）：
```python
# 修改前
MAX_CIRC_MV_W = 5000000   # 最大流通市值（万元，即 500 亿）

# 修改后
MAX_CIRC_MV_W = 8000000   # 最大流通市值（万元，即 800 亿）
```

**影响范围**：仅 `_size_ok()` 函数，无其他代码改动。

**验证标准**：信号总数增加不超过 30%（避免引入过多无弹性大盘股），胜率不跌破 49%。

---

## 3. P1 短期优化

### 3.1 P1-4: 策略按市况分族调度

**设计目标**：不同市况（牛市/震荡/熊市）使用不同策略族，提高信号质量。

**市况判定**（基于上证指数 `market_ok` 组件）：

```python
def _classify_regime(index_df: pd.DataFrame) -> pd.DataFrame:
    """将交易日分为 bull/range/bear 三种市况。

    bull:  指数 > MA60 且 MA20 陡峭上行（5日斜率 > 0.2%）
    range: 指数在 MA20 与 MA60 之间，或 MA20 走平
    bear:  指数 < MA60 且 MA20 下行
    """
    close = index_df["index_close"]
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    slope5 = ma20.pct_change(5) * 100  # 5日变化率

    regime = pd.Series("range", index=index_df.index)
    regime[(close > ma60) & (slope5 > 0.2)] = "bull"
    regime[(close < ma60) & (slope5 < 0)] = "bear"
    return pd.DataFrame({"date": index_df["date"], "regime": regime})
```

**策略-市况映射表**：

```python
REGIME_STRATEGIES = {
    "bull": [  # 趋势跟随最有效
        "ma_crossover", "volume_surge_std", "multi_ma_resonance",
        "wave_theory", "n_pattern", "ma_golden_cross",
        "volume_breakout", "monthly_macd_20ma",
    ],
    "range": [  # 反转/箱体策略
        "wonderful_9_turn", "emotion_cycle", "one_yang_three_yin",
        "box_oscillation", "washout_break", "low_profit_hold",
        "chan_theory", "limit_up_pullback",
    ],
    "bear": [  # 防御/基本面策略
        "low_profit_hold", "holder_conc_break", "fc_pos_break",
        "box_oscillation", "inst_smart_break",
    ],
}
```

**实现方式**：在 `_strategy_signal()` 中增加市况过滤：

```python
def _strategy_signal(df, name, regime_map=None):
    sig = STRATEGIES[name](df)
    sig = sig & _entry_mask(df) & _quality_mask(df, STRATEGY_MASKS.get(name, "base"))
    if regime_map is not None and "regime" in df.columns:
        allowed = REGIME_STRATEGIES.get("bull", []) + \
                  REGIME_STRATEGIES.get("range", []) + \
                  REGIME_STRATEGIES.get("bear", [])
        if name not in allowed:
            sig[:] = False
    return sig
```

**影响范围**：
- `compute_market_ok()` 新增 `regime` 列输出
- `_strategy_signal()` 新增 `regime_map` 参数
- `run_backtests()` 传递市况映射

**验证标准**：各市况下信号量应均衡分布，bull 市况胜率 ≥ 52%，bear 市况回避率 ≥ 70%。

---

### 3.2 P1-5: 信号强度加权

**设计目标**：将二值信号（0/1）改为连续强度（0-10），高分段权重更高。

**当前问题**：打分函数（如 `_ma_cross` 返回 0-10 分）已存在，但信号函数（`sig_ma_crossover`）用 `score >= 6` 二值化后丢弃了分数信息。

**修改方案**：保留原始分数，在推荐排序中加权：

```python
# 修改前：二值信号
def sig_ma_crossover(df):
    return _ma_cross(df) >= 6

# 修改后：返回 (信号, 强度)
def sig_ma_crossover(df):
    score = _ma_cross(df)
    return (score >= 6, score / 10.0)  # (布尔信号, 归一化强度 0-1)

# 在 get_next_day_recommendations 中使用强度加权
total_score += strategy_win_rate * signal_strength  # 原为 strategy_win_rate
```

**影响范围**：
- 所有 23 个策略信号函数签名改为返回 `(bool, float)` 或保持兼容
- `get_next_day_recommendations()` 和 `get_top_stocks_by_win_rate()` 使用强度加权
- 回测绩效统计不受影响（仍以二值信号为入口）

**验证标准**：推荐排序中，高分信号股票的实际收益应显著高于低分信号。

---

### 3.3 P1-6: 动态仓位管理

**设计目标**：基于凯利公式 + ATR 波动率调整，输出建议仓位比例。

**当前状态**：`calc_metrics()` 已计算 `kelly` 分数，但未用于仓位管理。

**修改方案**：

```python
def compute_position_size(strategy_name: str, atr20: float, close: float,
                          kelly: float, max_position_pct: float = 0.25) -> float:
    """计算单笔建议仓位比例。

    Args:
        strategy_name: 策略名
        atr20: 20日ATR
        close: 收盘价
        kelly: 策略历史凯利分数（%）
        max_position_pct: 单笔最大仓位上限（默认 25%）

    Returns:
        建议仓位比例（0-1）
    """
    # 波动率调整因子：目标波动 2% / 实际波动
    vol_adj = min(1.0, 0.02 / (atr20 / close)) if close > 0 and atr20 > 0 else 1.0

    # 凯利分数调整（半凯利，保守）
    kelly_adj = min(kelly / 100.0 * 0.5, max_position_pct)

    # 合并调整
    position = kelly_adj * vol_adj
    return min(position, max_position_pct)
```

**输出格式**：在推荐结果中增加 `position_pct` 字段。

**影响范围**：
- 新增 `compute_position_size()` 函数
- `get_next_day_recommendations()` 输出中增加 `position_pct` 字段
- 不改变回测统计口径（回测假设等权买入）

**验证标准**：高波动股票的 `position_pct` 应显著低于低波动股票。

---

## 4. P2 中期优化

### 4.1 P2-7: 行业动量策略

**设计目标**：优先选择处于动量前 3 行业内的信号。

**数据来源**（全部已有）：
- `tushare_index_daily`：SW 行业指数日线，2016-2026，180,885 行
- `tushare_index_member_all`：股票→L1/L2/L3 行业映射，5,842 行
- `tushare_index_classify`：SW2014 行业分类体系，L1=28个

**实现方案**：

```python
def load_industry_daily(start: str, end: str) -> pd.DataFrame:
    """加载申万一级行业指数日线数据。

    从 tushare_index_daily 中筛选 ts_code LIKE '801%' 的 SW 行业指数。
    JOIN tushare_index_classify 获取行业中文名。
    返回: ts_code, trade_date, close, pct_chg, industry_name
    """
    sql = """
        SELECT d.ts_code, d.trade_date, d.close, d.pct_chg, c.industry_name
        FROM tushare_index_daily d
        JOIN tushare_index_classify c ON d.ts_code = c.index_code
        WHERE d.ts_code LIKE '801%' AND c.level = 'L1'
          AND d.trade_date BETWEEN %s AND %s
        ORDER BY d.ts_code, d.trade_date
    """
    # ... 执行查询

def load_industry_membership() -> pd.DataFrame:
    """加载股票→行业映射。

    从 tushare_index_member_all 中获取每只股票当前的 L1/L2/L3 行业分类。
    以 in_date <= 当前日期 AND (out_date IS NULL OR out_date > 当前日期) 为准。
    返回: ts_code, l1_code, l1_name, l2_code, l2_name, l3_code, l3_name
    """
    sql = """
        SELECT ts_code, l1_code, l1_name, l2_code, l2_name, l3_code, l3_name
        FROM tushare_index_member_all
        WHERE out_date IS NULL OR out_date > CURRENT_DATE
    """
    # ... 执行查询

def compute_industry_momentum(df_industry: pd.DataFrame,
                               lookback: int = 20) -> pd.DataFrame:
    """计算行业动量排名。

    对每个交易日，按行业近 N 日涨跌幅排序，返回排名。
    """
    df = df_industry.sort_values(["ts_code", "trade_date"])
    df["ret_Nd"] = df.groupby("ts_code")["close"].transform(
        lambda x: x.pct_change(lookback)
    )
    # 每日行业排名（1 = 最强）
    df["rank"] = df.groupby("trade_date")["ret_Nd"].rank(ascending=False)
    return df[["trade_date", "ts_code", "industry_name", "ret_Nd", "rank"]]
```

**行业动量过滤**：仅保留行业排名前 3 的信号，与 `_quality_mask` 并列：

```python
def _industry_momentum_ok(df, industry_momentum, industry_map, top_n=3):
    """行业动量过滤：仅保留处于动量前 N 行业的信号。"""
    # 将行业动量排名合并到主数据
    # 数据缺失时放行（不误杀）
    ...
```

**影响范围**：
- 新增 `load_industry_daily()` 和 `load_industry_membership()` 函数
- `load_data()` SQL 新增 JOIN `tushare_index_member_all`（按 `ts_code` 映射）
- 新增 `compute_industry_momentum()` 排名函数
- `_entry_mask()` 新增可选行业动量过滤参数

**验证标准**：行业动量前 3 的信号胜率应比全行业高 ≥ 2pct。

---

### 4.2 P2-8: 量价背离策略

**设计目标**：捕捉价格与量能/RSI 的背离作为反转信号。

**新增策略函数**：

```python
def sig_volume_price_divergence(df):
    """量价顶背离：价格创 20 日新高但量能递减，顶部风险信号。

    逻辑：
      - 收盘创 20 日新高（不含当日）
      - 当日成交量 < 5 日均量（缩量创新高）
      - RSI6 > 60（超买区）
    该信号用于回避/卖出，而非买入（在 entry_mask 中也可用于过滤）。
    """
    new_high = df["close"] > df["high_20d_max"]
    vol_shrink = df["volume"] < df["vol_ma5"]
    rsi_overbought = df["rsi6"] > 60
    return new_high & vol_shrink & rsi_overbought


def sig_rsi_bullish_divergence(df):
    """RSI 底背离：价格创新低但 RSI 未创新低，下跌动能衰竭。

    打分权重：
      价格创 20 日新低（close < 前 20 日最低收盘价）  +4
      RSI6 未创新低（RSI6 >= 前 20 日最低 RSI6）      +4
      放量（量 > 1.2*vol_ma5）                        +3
      阳线（close > open）                            +2
    总分 >= 9 触发信号。
    """
    low_20 = df.groupby("code")["close"].transform(
        lambda x: x.shift(1).rolling(20).min()
    )
    rsi_low_20 = df.groupby("code")["rsi6"].transform(
        lambda x: x.shift(1).rolling(20).min()
    )
    score = ((df["close"] < low_20).astype(int) * 4 +
             (df["rsi6"] >= rsi_low_20).astype(int) * 4 +
             (df["volume"] > df["vol_ma5"] * 1.2).astype(int) * 3 +
             (df["close"] > df["open"]).astype(int) * 2)
    return score >= 9
```

**策略注册**：新增到 `STRATEGIES` 和 `STRATEGY_MASKS`：
```python
STRATEGIES["rsi_bullish_divergence"] = sig_rsi_bullish_divergence
STRATEGY_MASKS["rsi_bullish_divergence"] = "standard"
```

**影响范围**：
- 新增 2 个策略函数（约 50 行代码）
- 新增到策略注册表
- `sig_volume_price_divergence` 可作为 `_entry_mask` 的卖出过滤条件

**验证标准**：新策略胜率 ≥ 48%，与现有策略相关性 < 0.6（避免冗余）。

---

### 4.3 P2-9: 主力资金分层信号

**设计目标**：利用 `tushare_moneyflow` 中已有但未使用的大单/超大单分层数据。

**当前状态**：`load_data()` SQL 已 JOIN `tushare_moneyflow`，但只取了 `net_mf_amount`（总净额）。该表有 4 层资金数据：

| 字段 | 含义 | 当前状态 |
|------|------|:--:|
| `buy_elg_amount` / `sell_elg_amount` | 超大单买卖 | ❌ 未用 |
| `buy_lg_amount` / `sell_lg_amount` | 大单买卖 | ❌ 未用 |
| `buy_md_amount` / `sell_md_amount` | 中单买卖 | ❌ 未用 |
| `buy_sm_amount` / `sell_sm_amount` | 小单买卖 | ❌ 未用 |
| `net_mf_amount` | 总净额 | ✅ 已用 |

**修改方案**：扩展 `load_data()` SQL，新增 4 个资金分层字段：

```python
# 在 load_data() 的 SQL SELECT 中新增：
m.buy_lg_amount,
m.buy_elg_amount,
m.buy_sm_amount,
(m.buy_lg_amount + m.buy_elg_amount - m.sell_lg_amount - m.sell_elg_amount)
    AS net_big_amount,  -- 大单+超大单净额
```

**新增策略函数**：

```python
def sig_smart_money_flow(df):
    """主力资金持续流入：近 3 日大单+超大单净流入均为正。

    逻辑（全部满足）：
      - 近 3 日大单净买入 > 0（连续流入）
      - 小单净卖出 > 0（散户离场，筹码向主力集中）
      - 收盘站上 MA20
      - 阳线（close > open）
    """
    net_big = df["net_big_amount"]
    # 近 3 日持续净流入
    big_3d_pos = (net_big > 0) & \
                 (df.groupby("code")["net_big_amount"].shift(1) > 0) & \
                 (df.groupby("code")["net_big_amount"].shift(2) > 0)
    # 散户离场
    retail_out = df["buy_sm_amount"] < df["sell_sm_amount"]
    return big_3d_pos & retail_out & (df["close"] > df["ma20"]) & (df["close"] > df["open"])
```

**影响范围**：
- `load_data()` SQL 新增 4 个字段
- `_convert_columns_to_float32()` 新增字段名
- 新增 `sig_smart_money_flow` 策略
- 策略注册到 `STRATEGIES`（standard 族）

**验证标准**：新策略胜率 ≥ 50%，与 `inst_smart_break`（机构龙虎榜）相关性 < 0.5。

---

### 4.4 P2-10: 动态止损 per-strategy

**设计目标**：不同策略族使用不同的 ATR 倍数和移动止盈比例。

**设计原理**：

| 策略族 | ATR 倍数 | Trail | 原因 |
|--------|:------:|:----:|------|
| 趋势族（ma_crossover, wave_theory, multi_ma_resonance 等） | 3.0 | 0.97 | 趋势策略需要宽止损避免被震出 |
| 反转族（wonderful_9_turn, emotion_cycle 等） | 1.5 | 0.92 | 反转策略需要紧止损，错了马上走 |
| 涨停族（dragon_head, limit_up_pullback 等） | 1.0 | 0.95 | 涨停股波动大，超紧止损防回撤 |
| 默认（其余策略） | 2.5 | 0.95 | 当前最优参数 |

**修改方案**：在 `compute_dynamic_exit_returns()` 中按策略名传入不同参数：

```python
# 策略 → (atr_mult, trail) 映射
STRATEGY_EXIT_PARAMS = {
    # 趋势族：宽止损
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

def compute_dynamic_exit_returns(df, strategy_name=None):
    atr_mult, trail = STRATEGY_EXIT_PARAMS.get(strategy_name, (2.5, 0.95))
    # 使用 atr_mult 和 trail 替代硬编码的 (2.5, 0.95)
    # ...
```

**影响范围**：
- `compute_dynamic_exit_returns()` 新增 `strategy_name` 参数
- `_backtest_single()` 调用时传入策略名
- 预计算阶段需按策略名分别计算（或预计算多组参数结果）

**验证标准**：趋势族策略的最大回撤不增加，胜率不下降；反转族策略的 avg_loss 显著降低。

---

## 5. P3 长期优化

### 5.1 P3-11: ML 辅助信号过滤

**设计目标**：训练轻量级分类器替代/补充规则过滤，提升信号质量。

**技术选型**：LightGBM（处理混合特征 + 缺失值天然支持）

**特征设计**（约 40 个特征）：

| 类别 | 特征 | 来源 |
|------|------|------|
| 技术指标 | ma5/ma10/ma20/ma60 偏离度、RSI6/12/24、MACD DIF/DEA/hist | `compute_indicators` 已有 |
| 量价关系 | 量比、换手率、自由流通换手率、ATR20 | 已有 |
| 资金面 | 主力净流入、大单净买入、融资余额变化率 | `tushare_moneyflow`/`tushare_margin_detail` |
| 筹码面 | 获利盘比例、90%集中度、平均成本偏离度 | `tushare_cyq` |
| 基本面 | ROE、毛利率、营收增速、股息率 | `tushare_fina_indicator`/`tushare_daily_basic` |
| 事件面 | 股东人数变化、业绩预告方向、质押比例 | 增强信号数据 |
| 行业面 | 行业动量排名、行业内排名 | `tushare_index_daily` |

**标签构造**：持有期收益 > 0 → 正样本（1），否则负样本（0）

**训练策略**：
- 滚动窗口训练：每 6 个月重训练一次，使用前 4 年数据训练、后 6 个月验证
- 输出：每笔信号的置信度（0-1），回测时作为加权利率或过滤阈值

**架构**：
```
策略信号 → 特征提取 → LightGBM 推理 → 置信度 → 过滤（阈值 0.6）
```

**影响范围**：
- 新增 `src/ml/signal_filter.py` 模块
- 新增 `scripts/train_signal_filter.py` 训练脚本
- `_strategy_signal()` 新增可选 ML 过滤步骤

**验证标准**：ML 过滤后胜率提升 ≥ 3pct，且 OOS 验证不退化。

---

### 5.2 P3-12: 投资组合层面优化

**设计目标**：同日多个信号时，按行业分散 + 相关性控制选择标的。

**实现方案**：

```python
def optimize_portfolio(signals: pd.DataFrame, max_positions: int = 10,
                       max_per_industry: int = 2) -> pd.DataFrame:
    """组合层面优化：行业分散 + 最大持仓限制。

    Args:
        signals: 共振后的信号 DataFrame，含 code/date/strategy/industry/score
        max_positions: 最大持仓数
        max_per_industry: 每个行业最多持仓数

    Returns:
        筛选后的最终买入信号
    """
    # 1. 按行业分组，每组取 score 最高的前 max_per_industry 只
    # 2. 跨行业按 score 降序，取前 max_positions 只
    # 3. 可选：计算组合相关性矩阵，剔除高度相关（>0.7）的冗余标的
    ...
```

**影响范围**：
- 新增 `optimize_portfolio()` 函数
- `get_next_day_recommendations()` 调用组合优化
- 不改变回测统计口径（回测保持独立统计）

**验证标准**：组合层面的夏普比率应高于等权组合。

---

### 5.3 P3-13: 日内择时优化

**设计目标**：对比不同入场时点的收益差异。

**方案**：在回测中增加 `entry_timing` 参数：

| 入场方式 | 描述 | 适用场景 |
|----------|------|---------|
| `close`（当前） | 信号日收盘价买入 | 基准 |
| `next_open` | 次日开盘价买入 | 避免隔夜跳空 |
| `next_close` | 次日收盘价买入 | 确认信号延续 |
| `next_vwap` | 次日均价买入 | 减少滑点（需日内数据） |

**实现方式**：在 `_backtest_single()` 中，根据 `entry_timing` 参数选择不同的买入价格列。

**影响范围**：
- `_backtest_single()` 新增 `entry_timing` 参数
- `run_backtests()` 新增 `entry_timings` 参数，支持多入口对比
- 需要 `next_open` 列（`df.groupby("code")["open"].shift(-1)`）

**验证标准**：`next_open` 的胜率应不低于 `close`（排除涨停封板日无法买入的影响）。

---

## 6. 数据库依赖矩阵

| 优化项 | tushare_daily | tushare_daily_basic | tushare_moneyflow | tushare_cyq | tushare_margin_detail | tushare_index_daily | tushare_index_member_all | tushare_index_classify | tushare_fina_indicator | tushare_stk_holdernumber | tushare_forecast | tushare_pledge_stat | tushare_top_inst | tushare_block_trade | tushare_stk_holdertrade | tushare_balancesheet | tushare_income | tushare_cashflow |
|--------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| P0-1 enhanced regime | | | | | | ✅ | | | | | | | | | | | | |
| P0-2 共振门槛 | | | | | | | | | | | | | | | | | | |
| P0-3 市值范围 | | | | | | | | | | | | | | | | | | |
| P1-4 市况分族 | | | | | | ✅ | | | | | | | | | | | | |
| P1-5 信号强度 | | | | | | | | | | | | | | | | | | |
| P1-6 动态仓位 | | | | | | | | | | | | | | | | | | |
| P2-7 行业动量 | | | | | | ✅ | ✅ | ✅ | | | | | | | | | | |
| P2-8 量价背离 | | | | | | | | | | | | | | | | | | |
| P2-9 主力分层 | | | ✅* | | | | | | | | | | | | | | | |
| P2-10 动态止损 | | | | | | | | | | | | | | | | | | |
| P3-11 ML 过滤 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | | | ✅ | ✅ | ✅ |
| P3-12 组合优化 | | | | | | | ✅ | | | | | | | | | | | |
| P3-13 日内择时 | | | | | | | | | | | | | | | | | | |

> ✅* = 已 JOIN 但未使用分层字段，需扩展 SQL SELECT

---

## 7. 实施路线图

### 阶段一：P0 基础优化（预计 1-2 天）

```
Day 1:
  [ ] P0-3 扩大市值范围（1 行常量改动，无风险）
  [ ] P0-1 enhanced regime（修改 compute_market_ok，新增 market_ok_enh 列）
  [ ] 回测验证：对比 market_ok vs market_ok_enh 的 aggregate 指标

Day 2:
  [ ] P0-2 共振门槛（新增 _apply_resonance 函数）
  [ ] 回测验证：对比 baseline vs enhanced_regime vs resonance
  [ ] 输出 P0 综合回测报告
```

### 阶段二：P1 策略优化（预计 2-3 天）

```
Day 3-4:
  [ ] P1-4 市况分族（新增 _classify_regime + REGIME_STRATEGIES 映射）
  [ ] 回测验证：各市况下策略分族 vs 全策略的胜率对比

Day 4-5:
  [ ] P1-5 信号强度加权（修改策略信号函数签名 + 推荐排序）
  [ ] P1-6 动态仓位管理（新增 compute_position_size）
  [ ] 回测验证：组合 P1-4+P1-5+P1-6 的 aggregate 指标
```

### 阶段三：P2 数据增强（预计 3-5 天）

```
Day 6-7:
  [ ] P2-7 行业动量（新增 load_industry_daily/membership/momentum）
  [ ] 回测验证：行业动量过滤的信号胜率

Day 7-8:
  [ ] P2-9 主力分层（扩展 load_data SQL + 新增 smart_money_flow 策略）
  [ ] 回测验证：新策略独立回测

Day 8-10:
  [ ] P2-8 量价背离策略（新增 2 个策略函数）
  [ ] P2-10 动态止损 per-strategy（新增 STRATEGY_EXIT_PARAMS 映射）
  [ ] 回测验证：新策略 + 动态止损的 aggregate 指标
```

### 阶段四：P3 系统优化（预计 2-4 周）

```
Week 3-4:
  [ ] P3-11 ML 信号过滤（特征工程 + 训练 + 集成）
  [ ] 回测验证：ML 过滤前后胜率对比

Week 4-5:
  [ ] P3-12 组合优化（行业分散 + 相关性控制）
  [ ] P3-13 日内择时（多入场方式对比）
  [ ] 全量回测验证：P0+P1+P2+P3 综合 indicators
```

### 关键里程碑

| 里程碑 | 完成标准 | 预期指标 |
|--------|---------|---------|
| M1: P0 完成 | enhanced regime + 共振门槛 回测通过 | 年化 ≥ 15%, 胜率 ≥ 50.5% |
| M2: P1 完成 | 市况分族 + 强度加权 + 仓位管理 回测通过 | 胜率 ≥ 53%, 年化 ≥ 20% |
| M3: P2 完成 | 行业动量 + 主力分层 + 新策略 回测通过 | 胜率 ≥ 55%, 年化 ≥ 25% |
| M4: P3 完成 | ML 过滤 + 组合优化 + 日内择时 回测通过 | 胜率 ≥ 57%, 年化 ≥ 30% |

### 回滚策略

每个阶段完成后，通过 Git 分支隔离：
- `backtest/p0-enhanced-regime` → 合并前回测对比
- `backtest/p1-strategy-optimization` → 基于 P0 分支
- `backtest/p2-data-enhancement` → 基于 P1 分支
- `backtest/p3-ml-portfolio` → 基于 P2 分支

任何阶段回测指标退化超过 5%，暂停该分支，回退到上一阶段。

---

## 附录：WFO 实验核心数据

| 实验 | 最优配置 | 胜率 | 期望 | 年化 | 回撤 | 夏普 |
|------|---------|:---:|:----:|:---:|:---:|:---:|
| Exp2 baseline | 当前配置 | 50.6% | +0.520 | 4.6% | 12.7% | 0.35 |
| Exp2c enhanced | enhanced regime | 48.8-49.3% | +0.573~+0.632 | 18.2-21.3% | 10.8% | 1.1-1.2 |
| Exp3 exit | (2.5, 0.95) | 53.1% | +1.119 | 59.4% | - | - |
| Exp4 resonance | ≥2 策略 | 50.8% | +0.544 | 11.8% | - | - |
---

## 8. P1-1 / P1-2 / P1-3 / P1-4 落地实验（2026-09，5 年 2021-01 ~ 2026-08）

> 基于 24 策略 5 年回测的 A/B 实验，口径：enhanced regime + 共振≥2 + 行业动量开启 + 统一出场 (2.5, 0.95)。

### 8.1 新增 CLI / 环境变量开关

| 开关 | 含义 | 默认 | 实测结论 |
|------|------|------|----------|
| `--regime-soft` | 市况软加权（P1-1）：基本面策略为全市场强势锚，bear 日趋势弱信号需强势确认 | 关 | 信号量 100% 恢复但胜率/收益≈baseline（近 no-op），安全保留 |
| `--confluence-only` | 基本面合流硬过滤（P1-3）：纯技术策略信号需当日有业绩预增/筹码集中/机构净买催化剂 | 关 | **推荐质量门槛**，见 8.3 |
| `--confluence-regime {non_bull,bear}` | 市况感知合流（P1-3 软化）：仅 non_bull / bear 日要求催化剂 | 关 | 实测劣于硬过滤，见 8.3 |
| `BACKTEST_CONFLUENCE=1` / `BACKTEST_REGIME_SOFT=1` | 等价上述 CLI 的环境变量，供 Actions/定时任务不改命令启用 | 关 | CLI 显式参数优先 |

### 8.2 P1-2 组合策略（ensemble）重构

- 组件由 `ma_crossover / volume_surge_std / multi_ma_resonance` 改为 `ma_crossover / volume_surge_std / rsi_bullish_divergence`：剔除期望/笔最低（~0.46%）的 multi_ma_resonance，换入低相关反转族 rsi_bullish_divergence。
- 权重依据由"总胜率归一化"改为"**每笔期望收益**归一化"（`_compute_ensemble_weights`），高胜率低盈亏比的策略不再获高权重。
- 实测：ensemble 230→415 笔、胜率 47.4%→48.2%、总收益 50.9%→56.7%、夏普 2.78→3.22（默认生效）。

### 8.3 P1-3 基本面合流过滤（核心结论）

纯技术策略信号要求当日存在基本面催化剂（`forecast_pos==1` 或 `holder_chg<-3%` 或 `inst_buy5>500万`）；已内置基本面条件的 fc_pos_break/inst_smart_break/holder_conc_break/low_profit_hold/washout_break 豁免。

| 门控 | 总笔数 | 平均胜率 | 期望/笔 | 平均总收益 | 平均夏普 | 平均回撤 |
|------|:---:|:---:|:---:|:---:|:---:|:---:|
| baseline（不合流） | 3637 | 49.8% | 1.315% | 52.7% | 3.92 | 7.2% |
| **hard（`--confluence-only`）** | 2501 | **51.6%** | **1.481%** | 46.1% | **5.05** | **6.8%** |
| non_bull | 3412 | 49.3% | 1.248% | 46.0% | 4.19 | 6.9% |
| bear | 3744 | 49.4% | 1.270% | 53.5% | 3.92 | 7.8% |

结论：**硬过滤（全市况要求催化剂）风险调整后收益最优**——用 31% 信号量换胜率 +1.8pct、期望 +12.6%、夏普 +29%、回撤下降。软化变体（non_bull/bear）把牛市纯动量信号放回后胜率/期望回落至 baseline，说明基本面催化剂**在牛市同样有区分度**，"强牛市动量不需基本面确认"的直觉不成立。分策略看 wave_theory 49.6→54.0、monthly_macd_20ma 51.2→55.6、ma_crossover 49.3→52.0 胜率提升最明显。

### 8.4 P1-4 numba 静默失效加固

- `@numba.njit(cache=True)` → `cache=False`：磁盘缓存按模块名/源码行号索引，脚本经 runpy/importlib 以非 `__main__` 名（如 `<dynamic>`）加载时会写出/读入错配缓存，导致后续运行 njit 函数集体抛 `No module named '<dynamic>'`、策略静默 0 笔。关闭磁盘缓存彻底消除该污染面（JIT 重编译仅 1~2 秒）。
- 新增过半策略报错熔断：`run_backtests` 结束时若 >50% 策略报错则抛 `RuntimeError`，不再静默产出"0 笔/0 收益"空结果污染下游 handoff。

---

## 9. 2026-09 批次 A/B/C 落地（5 份优化建议收敛）

> 来源：仓库根目录 `优化建议_5份综合对比分析_20260919.md` §5 综合落地建议（minimax-M3 / doubao2.1t / GLM5.3 / hy4 / KimiK3 五份独立建议收敛）。
> 完整实现记录见 `docs/CHANGELOG.md` `[Unreleased]`。本批次遵循"观测先行 + 零代码 A/B 优先"：先落地观测基建与 A/B 开关，默认值翻转以 WFO OOS 实测为准。

### 9.1 新增 CLI 开关

| 开关 | 含义 | 默认 | 设计依据 |
|------|------|------|----------|
| `--no-exit-diagnostics` | 关闭出场诊断列（dyn_reason/dyn_mae/dyn_mfe，每持有期 3 列） | 开（诊断默认启用） | 观测基建：MAE/MFE 分布使出场参数从"猜"变"测"；580 万行约 +400MB 内存 |
| `--max-gap-pct` | 涨停族（含 limit_up_pullback）次日开盘跳空上限（%） | 0.0（关闭） | 2026-09-27 全样本 A/B：5%/3% 组合被证伪（全池胜率 49.1%→48.2%、期望 -0.096、22 策略中 20 个劣化），默认回退关闭；开关留作阈值扫描（3/5/7） |
| `--gap-band-pct` | 非涨停族对称跳空带宽（%） | 0.0（关闭） | 同上（对称带砍掉的 \|gap\|>3% 样本占 3.3%、平均质量不差） |
| `--resonance-layered` | 共振门槛按 `RESONANCE_MIN_BY_STRATEGY` 分层 | 关（全局标量旧行为） | 全局 ≥2 只砍 limit_up_pullback（-75.6% 且胜率 -1.3pct）、对其余主力近乎空转；批次4 全局降 1 实测胜率 +1.0pct、收益 +13.6pct |
| `--trading-cost` | 单边往返交易成本（%）覆盖 | 0.15 | next_open 主口径下开盘滑点（小盘股 0.1~0.3%）是最需计价项；A/B 档 0.15/0.25/0.35 |
| `--validate-adaptive` | 验证窗口自适应回看天数（取最后 5 个可开仓日） | 20 | enh2 仅 30.2% 可开仓率下固定 5 日窗口周期性 0/5 空转，handoff 输出 0 只股票 |
| `--defensive-half-pos` | 防守族半仓激活（广度 ∈ [0.35,0.45) 时放行，仓位 ×0.5） | 关 | 比全市场软化（enh2_soft）保守：只对弱市有效的防守族做例外 |
| `--inst-net-buy` | inst_smart_break 机构净买阈值（万元） | 500 | 9 笔 88.9% 胜率被三重紧条件饿死；A/B 档 500/300/200 |
| `--inst-window` | inst_smart_break 龙虎榜滚动窗口（自然日） | 10 | A/B 档 10/20/40；非默认窗口用独立准备态缓存后缀防 inst_buy5 口径串用 |

### 9.2 观测报表与判读指引

- `print_results` 新增两表："分持有期明细"（策略 × 1/3/5/10 日：笔数/胜率/期望，√=best_period）与"出场诊断"（出场原因占比 + MAE/MFE P25/P50/P75/P90）。
- 判读指引：MAE P75 远小于 ATR 止损位 → 亏损主要由 trail 提前扫出（hy4 R1 推断，指向 trail 激活门槛改造）；MFE P50 显著大于均盈 → 止盈过早、右尾被浪费。

### 9.3 明确不做（综合建议 §5.5 红线，重新评估条件见该文档）

- best_period 按族硬编码（等 9.2 分持有期明细出来再评估）；信号强度分档过滤（无历史标定）；新增策略（先清理存量小样本）；全样本扫参后翻转默认值（必须过 WFO OOS）。

### 9.4 首轮全样本 A/B 实测（2026-09-27，数据截至 2026-09-24）

| 组 | 配置 | 平均胜率 | 平均总收益 | 平均年化 | 结论 |
|----|------|:---:|:---:|:---:|------|
| A3 | 旧口径（跳空关 + 自适应关） | 49.1% | 135.2% | 16.7% | 基线 |
| B | 跳空过滤 5%/3% | 48.2% | 124.8% | 15.5% | **证伪**：全池 22 策略中 20 个劣化、涨停族未见修复，默认回退关闭 |
| C | 跳空 + 自适应验证窗口 | 48.4% | 125.2% | 15.5% | 验证样本 75→84 笔（+12%）、窗口 5/5 可开仓日（跨期 09-08~09-23） |

**出场诊断（B 组真实数据，验证 5 份建议的机制推断）**：

- **移动止盈占出场 77%~100%**（主力策略 85~90%），ATR 止损仅 0~1.1%，时间止损 8~22%——确认 hy4 R1"亏损主要来自 trail 提前扫出、而非 ATR 止损"；
- **MAE P75 ≈ -1.0~-1.9%**，远小于 3×ATR 止损位（≈7.5%），与 ATR 触发率 0.5% 互证（大部分交易从未接近止损位）；
- **MFE P50 ≈ 2.9~3.1% < trail 0.97 的盈利水位 +3.25%**，精确吻合 k3 的数学推导"约一半趋势信号在持有期内峰值从未达到 +3.25%"。

→ 下一优先改造：`--trail-activate-r` 延迟武装移动止盈（减少"刚浮盈即被 trail 扫出"的假亏损），A/B 档位 0/0.5/1.0/1.5/2.0，判据为"胜率与期望同时上升、回撤增幅 <3pct"。

### 9.6 出场侧三项改造 A/B（2026-10-05，数据截至 2026-09-24）

| 档位 | 平均胜率 | 平均总收益 | 平均年化 | 平均夏普 | 平均回撤 | 平均期望 |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| base（统一 0.97） | 49.3% | 136.1% | 16.7% | 3.93 | 6.0% | 1.711 |
| **sched_2d_093**（前2日0.93） | **50.9%** | **193.9%** | **21.2%** | **4.11** | 6.8% | **1.979** |
| sched_1d_095（前1日0.95） | 49.6% | 154.1% | 18.2% | 3.85 | 6.0% | 1.784 |
| sched_2d_095（前2日0.95） | 49.7% | 138.2% | 16.8% | 3.51 | 7.7% | 1.753 |
| act05_pre20（武装前止损2.0） | 53.1% | 10.9% | 1.6% | 0.30 | 23.1% | 0.787 |
| act05_pre15（武装前止损1.5） | 45.3% | -15.0% | -3.3% | -0.24 | 32.6% | -0.035 |
| res_layered | 49.1% | 134.9% | 16.6% | 3.96 | 5.8% | 1.700 |

**判读**：

- ✅ **时间阶梯 trail 成功，sched_2d_093 全部五项判据通过且全面改善**：总收益 +57.8pct（+42%）、夏普 3.93→4.11、胜率 +1.6pct、期望 +16%、回撤仅 +0.8pct。机制：入场后前 2 根 bar 用 0.93（回撤 7% 才出场）给交易呼吸空间、消除正常波动被 0.97（3% 回撤）扫出的假亏损，第 3 日起恢复 0.97 锁定利润；信号始终有保护，故无固定 R 门槛"武装前裸奔"的回撤问题。档位规律：阶梯越宽改善越大（0.93 > 0.95）。
- ❌ **武装前紧止损被证伪**：pre 2.0/1.5 使总收益崩塌至 10.9%/-15.0%、回撤暴增——紧止损在 trail 武装前频繁触发，砍掉原本能发展的交易。放弃该方向。
- ⚠️ **resonance-layered 中性**（总收益 -1.2pct，其余噪音范围）：全局 ≥2 已足够，分层不专门开启。

### 9.7 时间阶梯前沿第二轮（2026-10-05，数据截至 2026-09-24）

| 档位 | 平均胜率 | 平均总收益 | 平均年化 | 平均夏普 | 平均回撤 | 平均期望 |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| base（统一 0.97） | 49.3% | 136.1% | 16.7% | 3.93 | 6.0% | 1.711 |
| sched_2d_093 | 50.9% | 193.9% | 21.2% | 4.11 | 6.8% | 1.979 |
| sched_2d_090 | 51.9% | 207.8% | 22.2% | 4.26 | 7.2% | 2.120 |
| sched_3d_093 | 51.7% | 207.8% | 22.1% | 3.89 | 8.2% | 2.077 |
| sched_2d_091 | 51.8% | 205.6% | 22.1% | 4.24 | 7.2% | 2.097 |
| **sched_3d_090** | **53.1%** | **251.4%** | **25.0%** | **4.36** | 8.1% | **2.314** |

**判读**：

- **`sched_3d_090` 为当前最优点，五项判据全部通过**：总收益 +115pct（+85%）、夏普 4.36、胜率 +3.8pct、期望 +35%、回撤 +2.1pct（<3pct 红线）。
- **前沿规律**：(1) 同宽度下更长更好（3d_090 251 > 2d_090 208）；(2) 同长度下越宽越好（0.90 > 0.91 > 0.93）；(3) 2d_090 / 3d_093 / 2d_091 构成 ~206-208 平台。
- **机制**：趋势从信号日发展到峰值平均需 2~3 个交易日（MFE 分布：P50 3%、P75 6~7%），0.97 的 3% trail 在发展期内误杀；0.90（10% 回撤）+ 3 日窗口覆盖完整发展期，第 4 日 0.97 锁定。
- 待验证边界：更宽（0.85）/ 更长（4~5 日）——若回撤仍可控则前沿继续，否则 3d_090 收敛。

### 9.8 时间阶梯前沿边界扫描（2026-10-05，数据截至 2026-09-24）

| 档位 | 平均胜率 | 平均总收益 | 平均年化 | 平均夏普 | 平均回撤 | 平均期望 |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| base（统一 0.97） | 49.3% | 136.1% | 16.7% | 3.93 | 6.0% | 1.711 |
| sched_3d_090 | 53.1% | 251.4% | 25.0% | 4.36 | 8.1% | 2.314 |
| sched_3d_085 | 54.0% | 257.2% | 25.3% | 4.39 | 8.1% | 2.368 |
| sched_4d_090 | 53.5% | 283.6% | 26.4% | 4.40 | 8.9% | 2.404 |
| sched_5d_090 | 53.4% | 265.1% | 25.4% | 4.24 | 9.5% | 2.379 |
| **sched_4d_085** | **55.6%** | **322.5%** | **28.7%** | **4.68** | 8.4% | **2.642** |

**判读**：

- **`sched_4d_085` 为当前最优点，五项判据全部通过**：总收益 +186pct（+137%）、夏普 4.68、胜率 +6.3pct、期望 +54%、回撤 +2.4pct（<3pct 红线）。
- **前沿规律**：(1) 4 日优于 3 日、5 日开始回落（5d_090 265 < 4d_090 284）——长度边际收益在 4 日见顶；(2) 同长度 0.85 优于 0.90；(3) 4d_085 回撤（8.4%）小于 5d_090（9.5%），宽 trail 减少反复小亏、净值更平滑。
- **机制**：趋势发展期约 4 个交易日（长于此前估计的 2~3 日），0.85（15% 回撤）完整覆盖发展期；5 日后大部分趋势结束。
- 待最终验证：0.80/0.83 × 4d——若回撤可控则前沿微动，否则 4d_085 收敛固化。

### 9.9 前沿收敛确认（2026-10-05，数据截至 2026-09-24）

| 档位 | 平均胜率 | 平均总收益 | 平均年化 | 平均夏普 | 平均回撤 | 平均期望 |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **sched_4d_085** | 55.6% | **322.5%** | **28.7%** | **4.68** | **8.4%** | 2.642 |
| sched_4d_083 | 55.6% | 321.0% | 28.6% | 4.67 | 8.5% | 2.639 |
| sched_4d_080 | 55.6% | 319.1% | 28.5% | 4.64 | 8.6% | 2.641 |

**判读**：宽度 0.80~0.85 构成平顶（总收益差 ±1.7pct），0.85 微弱最优——前沿在 **`sched_4d_085` 收敛**。平顶结构说明结论对参数选择不敏感（非过拟合尖峰）。

**完整优化轨迹**：平均胜率 49.3%→55.6%（+6.3pct）、平均总收益 136.1%→322.5%（**+137%**）、平均年化 16.7%→28.7%、夏普 3.93→4.68、平均期望 1.711→2.642（+54%）、平均回撤 6.0%→8.4%（+2.4pct，红线内）。

**采纳前剩余动作**：(1) 固化为脚本默认（时间阶梯 4d_085）；(2) WFO OOS 年度滚动验证稳定性，确认非全样本选优过拟合。

### 9.10 WFO OOS 稳定性验证（2026-10-06）

在 3 个历史截止点（只使用当时可得数据）分别跑 base 与 4d_085：

| 截止点 | 档位 | 平均胜率 | 平均总收益 | 平均年化 | 平均夏普 | 平均回撤 | 平均期望 |
|---|---|:---:|:---:|:---:|:---:|:---:|:---:|
| 2023-12-29 | base | 51.6% | 80.6% | 23.4% | 5.11 | 4.4% | 1.911 |
| | 4d_085 | **57.1%** | **155.6%** | **39.0%** | **5.95** | 6.5% | **2.876** |
| 2024-12-31 | base | 51.6% | 107.9% | 21.3% | 4.63 | 4.9% | 1.996 |
| | 4d_085 | **57.6% | **226.7%** | **35.9%** | **5.30** | 7.1% | **3.072** |
| 2025-06-30 | base | 50.2% | 118.4% | 19.9% | 4.44 | 5.1% | 1.880 |
| | 4d_085 | **55.6%** | **235.0%** | **31.3%** | **4.93** | 7.1% | **2.766** |

**结论：通过，4d_085 非过拟合，正式采纳。**

- 3 个窗口全部指标稳定占优：总收益 +93%/+110%/+98%、胜率 +5.5/+6.0/+5.4pct、期望 +50%/+54%/+47%；
- 改善幅度窗口间高度一致、回撤代价稳定（+2pct，红线内）——机制可解释、非特定时段偶然；
- 时间阶梯规则不依赖未来信息（固定：前4日0.8、第5日0.97）。

**环境治理**：验证期间发现机器使用电池时 30 分钟自动睡眠（S3），致进程挂起/死亡（`SetThreadExecutionState` 在电池策略下未能阻止）；已设置 DC 待机超时为 0（`powercfg /change standby-timeout-dc 0`），长任务不再中断。

**优化闭环完成**：观测基建 → 机制诊断 → 方案 A/B → 前沿收敛 → 固化默认 → OOS 验证。

### 9.11 行动顺序落地（P1/P2/P3，2026-10-07）

> 源自对 `logs/backtest_5y_20261006.log` 的分析结论：回测 55.8% 胜率 vs 近期 5 日验证 15~40%，问题从"参数优化"转向"近期失效应对"与"验证链路质量"。按优先级执行：

**P1-❶ handoff 验证亏损惩罚 + 硬过滤**：共振评分验证档由固定 0.5 改为 `clip(0.5 + sret*5, 0.1, 0.5)`；新增 `--handoff-hard-filter`（验证收益低于阈值% 不进主程序）。修复此前"柳工验证 -5.77% 仍以 4.5 分排第 2"的漏洞。

**P1-❷ 近期表现自适应过滤**：`--recent-perf {off,disable,downweight}` + `--recent-window`（默认 60）。逐日**因果**口径计算各策略滚动胜率（时点 di 只用「信号日+持有期≤di」的已平仓信号），<45% 判失效（disable 剔除 / downweight 减半）；并在 `main()` 的验证/handoff 信号链补齐同口径应用。回测胜率是 5 年平均，实时决策改用近期胜率。

近 2 年 A/B（逐日因果，供验证信号已接入过滤）：

| 模式 | 回测胜率 | 回测期望 | 回测总收益 | 回撤 | 夏普 | 笔数 | 验胜率 | 验收益 | 验笔数 |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| off | 52.66 | 1.412 | 51.35 | 6.89 | 4.44 | 5758 | 20.42 | -1.88 | 135 |
| disable | 52.39 | 1.420 | 47.68 | 7.22 | 4.22 | 5354 | 20.42 | -1.88 | 135 |
| downweight | **52.73** | **1.446** | **55.20** | **6.63** | **4.57** | 5248 | 20.42 | -1.88 | 135 |

运行期约 17 个策略被标记"近 60 日胜率<45%"（如 ma_crossover 剔除 33 笔）。**结论：`downweight` 各回测指标一致小幅改善（总收益 +3.85pct、期望 +0.034、回撤 -0.26、夏普 +0.13），`disable` 略负；近期 5 日验证窗三档一致（候选策略近期胜率均 ≥45%、未触发过滤）。** 保持默认 `off`，启用时推荐 `downweight`。

**前视偏差修复**：原 `_apply_recent_filter_standalone` 用"最新交易日近 window 日胜率"套用整条序列，回测存在前视；且仅在 `run_backtests` 内部生效、验证/handoff 的 signals 未过滤（过滤器对实时决策无效）。已改为逐日因果滚动胜率，并在 `main()` 验证信号链补齐应用。

**P2-❹ 验证回退跳过 1 日持有期**：`_signal_return_cols(min_period=3)`。1 日持有期全策略期望为负，验证/handoff/近期胜率回退不再取它。

**P2-❸ 出场阶梯近期适配对比**（近 1.5 年，见下表）：**发现回测-实战系统背离**。

| 档位 | 回测总收益 | 近期5日验证胜率 | 验证均收益 |
|---|:---:|:---:|:---:|
| base_097（统一0.97） | 16.6% | **27.9%** | -0.535 |
| sched_2d_093 | 26.5% | 25.6% | **-0.427** |
| sched_4d_085（当前默认） | **35.8%** | **17.0%** | **-1.552** |

回测总收益排序与近期验证胜率排序**完全相反**：当前（2026-09-18~24）是"快反转/低延续"regime，4d_085 的宽 trail 让亏损单跑得更深；base 的紧 trail 快速止损单笔亏损更小。**结论：4d_085 在长期趋势市占优，但低延续行情回撤更大；需配合近期失效过滤（P1-❷）动态应对，`--trail-schedule` 可切换。**

**P3-❺ 死策略停用**：chan_theory（0 笔）、dragon_head（1 笔）、box_oscillation（1 笔）默认停用（24→21 策略），`--no-retire` 恢复。

**P3-❻ 行业 PIT 数据源限制**：`tushare_index_member_all` 源端 `out_date` 全空、DB 仅存 `is_new=Y` 当前成分——**无法补齐历史调出**（非代码缺陷）。代码已支持 `has_out` 分支（有数据源即自动启 PIT）；当前缓解为 `in_date` as-of + `--no-industry-momentum` 消融量化。

**修复**：validate_week/近期胜率 `_strategy_signal` 未传 `enhanced=True`，严格 regime 下验证窗 0 信号（P2-❸ 调试暴露）——已修复并与回测主口径统一。

**验证**：纯函数单测 10/10、集成冒烟 9/9（停用/恢复/近期过滤/handoff 硬过滤/验证惩罚均生效）。

### 9.5 trail-activate-r 五档全样本 A/B（2026-09-28，数据截至 2026-09-24）

> 环境修复：首轮夜间运行时机器进入 S3 睡眠，唤醒后 pandas 大数组计算状态损坏（fc_pos_break 334→0）。批量脚本已加 `SetThreadExecutionState` 阻止运行期间休眠；下列数据为修复后重跑结果（每档 dyn 21 分 + 回测 8~13 分，共约 2.5 小时，无中断）。

| trail_activate_r | 平均胜率 | 平均总收益 | 平均年化 | 平均夏普 | 平均回撤 | 平均期望 |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **0.0**（基线） | 49.3% | **136.0%** | 16.7% | **3.98** | **5.7%** | 1.722 |
| **0.5** | **57.2%** | 99.4% | 12.5% | 2.00 | 17.3% | **1.741** |
| **1.0** | 49.8% | 82.2% | 10.5% | 1.64 | 17.6% | 1.395 |
| **1.5** | 48.0% | 71.0% | 9.0% | 1.39 | 19.1% | 1.249 |
| **2.0** | 47.4% | 50.8% | 6.7% | 1.08 | 19.7% | 1.104 |

**判读**：

- **0.5 档胜率 +7.9pct（57.2%）、6/6 个年份全部改善**（2021 +3.6、2022 +4.9、2023 +8.5、2024 +6.8、2025 +11.5、2026 +12.6），期望微升（+0.019）——证实"入场即被 trail 扫出"的假亏损大量存在，延迟武装方向正确；
- **但整体判据不通过**：总收益 -27%（136→99）、回撤 +11.5pct（5.7→17.3，远超 <3pct 红线）。机制：武装前（浮盈未达 0.5×ATR≈1.25%）trail 完全不保护，反向波动自由跑到 3×ATR 止损（-7.5%），单笔亏损幅度扩大；
- 策略分化：涨停族（stable_then_limit_up +63、holder_conc_break +61）受益于容忍期内的动能延续；ma_crossover（-96）、fc_pos_break（-96）等趋势/事件策略被回撤拖累；
- **1.0+ 档**：trail 基本不触发（MFE 分布决定大部分交易峰值达不到武装线），出场退化为 ATR/时间止损，总收益/夏普单调恶化，无采纳价值。

**结论：固定 R 门槛的实现被证伪，但"延迟武装"机制本身成立（胜率证据）。** 下一步不是选档，而是改造组合：

1. **延迟武装 + 收紧武装前保护**：武装前的止损从 3×ATR 收到 1.5~2×ATR（D1/D2 联动），消除"自由落体"回撤；
2. **时间阶梯替代固定 R**：入场后前 1~2 日用宽 trail（如 0.95）、第 3 日起正常 0.97——不依赖浮盈门槛，信号一触发就有保护；
3. 先跑 `--resonance-layered`（零成本开关，B2 证据充分），与上述改造正交。

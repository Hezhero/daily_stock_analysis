# Tushare 本地库（tushare）2000 积分接口建表缺口梳理

> 连接方式：PG `172.28.230.26:5431` / db=`tushare` / user=`root`
> 梳理时间：2026-08-08
> 判定口径：以 Tushare 官方「积分明细表」(doc_id=108) 中**最低分值 ≤ 2000** 的接口为准；"已建表"指库中已存在对应实体表（含按年分区的 daily/daily_basic/adj_factor）。

## 一、库中现有表结构（逻辑实体）

库内 83 张表 = 3 类按年分区的行情表 + 14 张单表：

| 逻辑实体 | 表形态 | 数据量 | 对应接口 | 积分 |
|---------|--------|--------|---------|------|
| 日线行情 | `tushare_daily_YYYY`(2010–2030+default) | 已落 2016–2026 | daily | 120 起 ✅ |
| 每日指标 | `tushare_daily_basic_YYYY` | 已落 2016–2026 | daily_basic | 2000 起 ✅ |
| 复权因子 | `tushare_adj_factor_YYYY` | 已落 2016–2026 | adj_factor（复权需求） | ✅ |
| 利润表 | `tushare_income` | 200,851 | income | 2000 起 ✅ |
| 资产负债表 | `tushare_balancesheet` | 191,287 | balancesheet | 2000 起 ✅ |
| 现金流量表 | `tushare_cashflow` | 199,117 | cashflow | 2000 起 ✅ |
| 业绩预告 | `tushare_forecast` | 71,579 | forecast | 2000 起 ✅ |
| 业绩快报 | `tushare_express` | 18,531 | express | 2000 起 ✅ |
| 分红送股 | `tushare_dividend` | 0（空） | dividend | 2000 起 ✅ |
| 财务指标 | `tushare_fina_indicator` | 81,382 | fina_indicator | 2000 起 ✅ |
| 财务审计 | `tushare_fina_audit` | 55,725 | fina_audit | 2000 起 ✅ |
| 主营构成 | `tushare_fina_mainbz` | 697,849 | fina_mainbz | 2000 起 ✅ |
| IPO 列表 | `tushare_ipo_list` | 2,000 | new_share | 120 ✅ |
| 股票列表 | `tushare_stock_basic` | 5,539 | stock_basic | 120 ✅ |
| 公司信息 | `tushare_stock_company` | 6,291 | stock_company | 120 ✅ |
| 交易日历 | `tushare_trade_cal` | 4,228 | trade_cal | 120 ✅ |
| 更名历史 | `tushare_namechange` | 10,000 | namechange | 120 ✅ |
| 筹码分布 | `tushare_cyq` | 本地计算 | cyq（**特色数据，需 5000+，超出本次范围**）→ 已改为本地三角形分布法计算（`incremental_cyq.py`，基于 tushare_daily + tushare_daily_basic） | ✅ |

> 注：`tushare_sync_log` 为内部同步日志表，非 Tushare 接口。`tushare_dividend` 已建表但**行数为 0**，等于未实际落库；`tushare_cyq` 原为空表，现已由本地三角形分布法计算写入（`incremental_cyq.py`，不依赖 Tushare cyq 接口）。

## 二、2000 积分「未建表」接口缺口（按模块）

### 1. 股票行情补充（主干已建，缺周/月线）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| weekly | 周线行情 | 2000 | ❌ 未建（可由 daily 重采样，可不建） |
| monthly | 月线行情 | 2000 | ❌ 未建（可由 daily 重采样，可不建） |
| pro_bar | 复权行情 | 2000 | ✅ 已由 adj_factor + daily 覆盖 |

### 2. 股票衍生 / 情绪 / 博弈类（**全部缺失，量化价值高**）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| moneyflow | 个股资金流向（主/#amp;大单） | 2000 | ❌ 未建 |
| margin | 融资融券交易汇总 | 2000 | ❌ 未建 |
| margin_detail | 融资融券交易明细 | 2000 | ❌ 未建 |
| stk_holdernumber | 股东人数（筹码集中度） | 2000 | ❌ 未建 |
| top_list | 龙虎榜每日明细 | 2000 | ❌ 未建 |
| top_inst | 龙虎榜机构交易 | 2000 | ❌ 未建 |
| pledge_detail | 股权质押明细 | 2000 | ❌ 未建 |
| pledge_stat | 股权质押统计 | 2000 | ❌ 未建 |
| repurchase | 股票回购 | 2000 | ❌ 未建 |
| block_trade | 大宗交易 | 2000 | ❌ 未建 |
| stk_holdertrade | 股东增减持 | 2000 | ❌ 未建 |
| stk_limit | 每日涨跌停价格 | 2000 起 | ❌ 未建 |
| hk_hold | 沪深股通持股明细 | 2000 起 | ❌ 未建 |

### 3. 财务补充
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| disclosure_date | 财报披露计划 | 2000 起 | ❌ 未建 |

### 4. 基金（**全部缺失**）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| fund_basic | 公募基金列表 | 2000 | ❌ 未建 |
| fund_company | 公募基金公司 | 2000 | ❌ 未建 |
| fund_nav | 公募基金净值 | 2000 | ❌ 未建 |
| fund_daily | 场内基金日线 | 2000 | ❌ 未建 |
| fund_div | 公募基金分红 | 2000 | ❌ 未建 |

### 5. 期货（**全部缺失**）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| fut_basic | 期货合约列表 | 2000 | ❌ 未建 |
| fut_daily | 期货日线行情 | 2000 | ❌ 未建 |
| fut_holding | 每日成交持仓排名 | 2000 | ❌ 未建 |
| fut_wsr | 仓单日报 | 2000 | ❌ 未建 |
| fut_settle | 结算参数 | 2000 | ❌ 未建 |
| index_daily(南华) | 南华期货指数 | 2000 | ❌ 未建 |
| （期货交易日历） | 由 `trade_cal` 按 exchange 过滤覆盖 | 2000 | ⚠️ 复用现有表 |

### 6. 期权（缺失）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| opt_basic | 期权合约列表 | 2000 起 | ❌ 未建 |

### 7. 债券 / 可转债（**全部缺失**）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| cb_basic | 可转债基础信息 | 2000 | ❌ 未建 |
| cb_issue | 可转债发行数据 | 2000 | ❌ 未建 |
| cb_daily | 可转债日线 | 2000 | ❌ 未建 |

### 8. 外汇（缺失）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| fx_obasic | 外汇基础信息 | 2000 | ❌ 未建 |
| fx_daily | 外汇日线行情 | 2000 | ❌ 未建 |

### 9. 指数（**全部缺失，行业/基准必备**）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| index_basic | 指数基本信息 | 2000 | ❌ 未建 |
| index_daily | 指数日线行情 | 2000 起 | ❌ 未建 |
| index_weekly | 指数周线 | 2000 起 | ❌ 未建 |
| index_monthly | 指数月线 | 2000 起 | ❌ 未建 |
| index_weight | 指数成分和权重 | 2000 | ❌ 未建 |
| index_classify | 申万行业分类 | 2000 | ❌ 未建 |
| index_member_all | 申万行业成分 | 2000 | ❌ 未建 |

### 10. 港股基础（缺失）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| hk_basic | 港股列表 | 2000 | ❌ 未建 |

### 11. 宏观利率（缺失）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| shibor | SHIBOR 利率 | 2000 | ❌ 未建 |
| shibor_quote | SHIBOR 报价 | 2000 | ❌ 未建 |
| shibor_lpr | LPR 贷款基础利率 | 120 | ❌ 未建 |
| libor | LIBOR 拆借利率 | 120 | ❌ 未建 |
| hibor | HIBOR 拆借利率 | 120 | ❌ 未建 |
| wz_index | 温州民间借贷利率 | 2000 | ❌ 未建 |
| gz_index | 广州民间借贷利率 | 2000 | ❌ 未建 |

## 三、缺口统计
- 已覆盖 2000 积分接口（含 120 基础）：约 **18 个**（核心行情 + 财务三表 + 财务衍生 + 基础信息）
- **未建表接口：约 48 个**，集中在 基金 / 期货 / 期权 / 可转债 / 外汇 / 指数 / 港股 / 宏观 八大模块，以及股票衍生情绪类 13 个。
- 已建但空表（需补数据）：`tushare_dividend`；`tushare_cyq` 原为空表，已改为本地三角形分布法计算写入（不依赖 5000+ 积分接口）。

## 四、给你的量化项目（daily_stock_analysis）的落地优先级

**P0 — 股票衍生因子（直接增强 22 策略，几乎零成本）**
1. moneyflow 资金流向 → 主力/大单净流入因子
2. margin / margin_detail 融资融券 → 杠杆情绪因子
3. stk_holdernumber 股东人数 → 筹码集中度因子
4. stk_limit 涨跌停 → 流动性/极端情绪
5. top_list / top_inst 龙虎榜 → 游资跟踪

**P1 — 基准与行业中性（回测必需的对照系）**
6. index_basic + index_weight + index_daily → 指数基准
7. index_classify + index_member_all → 申万行业分类，做行业中性
8. disclosure_date → 财报披露日历，避开窗口期

**P2 — 多资产扩展（看策略是否需要）**
9. cb_basic / cb_issue / cb_daily 可转债
10. fund_basic / fund_nav / fund_daily 基金
11. fut_basic / fut_daily 期货

**P3 — 宏观 / 海外（辅助）**
12. shibor / shibor_lpr 利率环境
13. hk_basic / hk_hold 港股通
14. fx_daily 外汇

**可跳过**：weekly / monthly（用 daily 重采样即可）；期货交易日历（复用 trade_cal）。

## 五、后续建议
- 建表可沿用现有「按年分区」模式（如 `tushare_moneyflow_YYYY`）与 `tushare_sync_log` 同步机制，保持风格统一。
- 未建表的 48 个接口里，P0/P1 共约 15 个即可覆盖绝大多数日级因子扩展；其余按策略需要再补。
- 注意 `tushare_dividend` 虽建表但行数为 0，需排查同步脚本是否漏跑。

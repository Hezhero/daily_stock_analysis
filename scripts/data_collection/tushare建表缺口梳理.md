# Tushare 本地库（tushare）2000 积分接口建表缺口梳理

> 连接方式：PG `172.28.230.26:5431` / db=`tushare` / user=`root`
> 梳理时间：2026-08-13（按实际库表重新盘点）
> 判定口径：以 Tushare 官方「积分明细表」(doc_id=108) 中**最低分值 ≤ 2000** 的接口为准；"已建表"指库中已存在对应实体表（含按年分区表）；"已移除"指曾建表后按仓库决策整体删除、不再采集（见 `docs/CHANGELOG.md`）。

## 一、库中现有表结构（逻辑实体）

库内 40 张父表 = 7 类按年分区的行情/衍生表 + 33 张单表（含分区子表共 187 个物理表）：

### 行情 / 指数

| 逻辑实体 | 表形态 | 数据量 | 对应接口 | 积分 |
|---------|--------|--------|---------|------|
| 日线行情 | `tushare_daily_YYYY`(2010–2030+default) | 10,352,759 | daily | 120 起 ✅ |
| 每日指标 | `tushare_daily_basic_YYYY` | 10,637,238 | daily_basic | 2000 起 ✅ |
| 复权因子 | `tushare_adj_factor_YYYY` | 10,668,108 | adj_factor（复权需求） | ✅ |
| 指数日线 | `tushare_index_daily_YYYY` | 26,048,254 | index_daily | 2000 起 ✅ |
| 指数周线 | `tushare_index_weekly` | 537,884 | index_weekly | 2000 起 ✅ |
| 指数月线 | `tushare_index_monthly` | 126,514 | index_monthly | 2000 起 ✅ |
| 指数成分权重 | `tushare_index_weight` | 535,203 | index_weight | 2000 起 ✅ |
| 指数基本信息 | `tushare_index_basic` | 10,545 | index_basic | 2000 ✅ |
| 申万行业分类 | `tushare_index_classify` | 359 | index_classify | 2000 ✅ |
| 申万行业成分 | `tushare_index_member_all` | 3,001 | index_member_all | 2000 ✅ |

### 股票衍生 / 情绪 / 博弈

| 逻辑实体 | 表形态 | 数据量 | 对应接口 | 积分 |
|---------|--------|--------|---------|------|
| 资金流向 | `tushare_moneyflow_YYYY` | 10,690,082 | moneyflow | 2000 起 ✅ |
| 融资融券汇总 | `tushare_margin` | 6,000 | margin | 2000 起 ✅ |
| 融资融券明细 | `tushare_margin_detail_YYYY` | 6,079,736 | margin_detail | 2000 起 ✅ |
| 涨跌停价格 | `tushare_stk_limit_YYYY` | 13,390,811 | stk_limit | 2000 起 ✅ |
| 股东人数 | `tushare_stk_holdernumber` | 450,504 | stk_holdernumber | 2000 起 ✅ |
| 龙虎榜明细 | `tushare_top_list` | 151,151 | top_list | 2000 起 ✅ |
| 龙虎榜机构 | `tushare_top_inst` | 2,079,646 | top_inst | 2000 起 ✅ |
| 股权质押明细 | `tushare_pledge_detail` | 133,639 | pledge_detail | 2000 起 ✅ |
| 股权质押统计 | `tushare_pledge_stat` | 1,283,864 | pledge_stat | 2000 起 ✅ |
| 股票回购 | `tushare_repurchase` | 51,293 | repurchase | 2000 起 ✅ |
| 大宗交易 | `tushare_block_trade` | 534,034 | block_trade | 2000 起 ✅ |
| 股东增减持 | `tushare_stk_holdertrade` | 75,655 | stk_holdertrade | 2000 起 ✅ |

### 财务

| 逻辑实体 | 表形态 | 数据量 | 对应接口 | 积分 |
|---------|--------|--------|---------|------|
| 利润表 | `tushare_income` | 201,962 | income | 2000 起 ✅ |
| 资产负债表 | `tushare_balancesheet` | 196,936 | balancesheet | 2000 起 ✅ |
| 现金流量表 | `tushare_cashflow` | 199,117 | cashflow | 2000 起 ✅ |
| 业绩预告 | `tushare_forecast` | 73,491 | forecast | 2000 起 ✅ |
| 业绩快报 | `tushare_express` | 18,486 | express | 2000 起 ✅ |
| 分红送股 | `tushare_dividend` | 305,856 | dividend | 2000 起 ✅ |
| 财务指标 | `tushare_fina_indicator` | 81,639 | fina_indicator | 2000 起 ✅ |
| 财务审计 | `tushare_fina_audit` | 56,292 | fina_audit | 2000 起 ✅ |
| 主营构成 | `tushare_fina_mainbz` | 739,302 | fina_mainbz | 2000 起 ✅ |
| 财报披露计划 | `tushare_disclosure_date` | 277,705 | disclosure_date | 2000 起 ✅ |

### 基础信息 / 宏观 / 本地计算

| 逻辑实体 | 表形态 | 数据量 | 对应接口 | 积分 |
|---------|--------|--------|---------|------|
| IPO 列表 | `tushare_ipo_list` | 2,000 | new_share | 120 ✅ |
| 股票列表 | `tushare_stock_basic` | 5,543 | stock_basic | 120 ✅ |
| 公司信息 | `tushare_stock_company` | 6,291 | stock_company | 120 ✅ |
| 交易日历 | `tushare_trade_cal` | 4,383 | trade_cal | 120 ✅ |
| 更名历史 | `tushare_namechange` | 10,000 | namechange | 120 ✅ |
| SHIBOR 利率 | `tushare_shibor` | 4,120 | shibor | 2000 ✅ |
| 筹码分布 | `tushare_cyq` | 10,351,463 | cyq（**特色数据，需 5000+，超出本次范围**）→ 已改为本地三角形分布法计算（`incremental_cyq.py`，基于 tushare_daily + tushare_daily_basic） | ✅ |

> 注：`tushare_sync_log` 为内部同步日志表，非 Tushare 接口。`tushare_dividend` 原为 0 行空表，已于 2026-08 由 `incremental_fin.py` 补数（305,856 行）；`tushare_cyq` 由本地三角形分布法计算写入（`incremental_cyq.py`，不依赖 Tushare cyq 接口）。已按仓库决策整体移除的表：`tushare_shibor_quote`（2026-08-13 删除，接口限频 1 次/分钟且无消费方）、`tushare_shibor_lpr`/`tushare_libor`/`tushare_hibor`/`tushare_wz_index`/`tushare_gz_index`、`tushare_hk_hold`、`tushare_hs_const`。

## 二、2000 积分「未建表」接口缺口（按模块）

### 1. 股票行情补充（主干已建，缺股票周/月线）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| weekly | 周线行情（股票） | 2000 | ❌ 未建（可由 daily 重采样，可不建；指数周线已建 `tushare_index_weekly`） |
| monthly | 月线行情（股票） | 2000 | ❌ 未建（可由 daily 重采样，可不建；指数月线已建 `tushare_index_monthly`） |
| pro_bar | 复权行情 | 2000 | ✅ 已由 adj_factor + daily 覆盖 |

### 2. 股票衍生 / 情绪 / 博弈类（**已全部建成**）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| moneyflow | 个股资金流向（主/#amp;大单） | 2000 | ✅ 已建（按年分区） |
| margin | 融资融券交易汇总 | 2000 | ✅ 已建 |
| margin_detail | 融资融券交易明细 | 2000 | ✅ 已建（按年分区） |
| stk_holdernumber | 股东人数（筹码集中度） | 2000 | ✅ 已建 |
| top_list | 龙虎榜每日明细 | 2000 | ✅ 已建 |
| top_inst | 龙虎榜机构交易 | 2000 | ✅ 已建 |
| pledge_detail | 股权质押明细 | 2000 | ✅ 已建 |
| pledge_stat | 股权质押统计 | 2000 | ✅ 已建 |
| repurchase | 股票回购 | 2000 | ✅ 已建 |
| block_trade | 大宗交易 | 2000 | ✅ 已建 |
| stk_holdertrade | 股东增减持 | 2000 | ✅ 已建 |
| stk_limit | 每日涨跌停价格 | 2000 起 | ✅ 已建（按年分区） |
| hk_hold | 沪深股通持股明细 | 2000 起 | 🚫 已移除（表已删，不再采集） |

### 3. 财务补充
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| disclosure_date | 财报披露计划 | 2000 起 | ✅ 已建 |

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

### 9. 指数（**已全部建成**）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| index_basic | 指数基本信息 | 2000 | ✅ 已建 |
| index_daily | 指数日线行情 | 2000 起 | ✅ 已建（按年分区） |
| index_weekly | 指数周线 | 2000 起 | ✅ 已建 |
| index_monthly | 指数月线 | 2000 起 | ✅ 已建 |
| index_weight | 指数成分和权重 | 2000 | ✅ 已建 |
| index_classify | 申万行业分类 | 2000 | ✅ 已建 |
| index_member_all | 申万行业成分 | 2000 | ✅ 已建 |

### 10. 港股基础（缺失）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| hk_basic | 港股列表 | 2000 | ❌ 未建 |

### 11. 宏观利率（已建 / 已移除）
| 接口 | 说明 | 最低分 | 状态 |
|------|------|--------|------|
| shibor | SHIBOR 利率 | 2000 | ✅ 已建 |
| shibor_quote | SHIBOR 报价 | 2000 | 🚫 已移除（2026-08-13 删除，接口限频 1 次/分钟且无消费方） |
| shibor_lpr | LPR 贷款基础利率 | 120 | 🚫 已移除 |
| libor | LIBOR 拆借利率 | 120 | 🚫 已移除 |
| hibor | HIBOR 拆借利率 | 120 | 🚫 已移除 |
| wz_index | 温州民间借贷利率 | 2000 | 🚫 已移除 |
| gz_index | 广州民间借贷利率 | 2000 | 🚫 已移除 |

## 三、缺口统计
- 已覆盖 2000 积分接口（含 120 基础）：约 **39 个**（核心行情 + 财务三表及衍生 + 指数 7 接口 + 股票衍生 12 接口 + 基础信息 + SHIBOR + 本地计算筹码）
- **未建表接口：约 20 个**，集中在 基金 / 期货 / 期权 / 可转债 / 外汇 / 港股 六大模块，以及股票周/月线（可由 daily 重采样，可不建）
- 已建但空表：无（`tushare_dividend` 已补数 305,856 行；`tushare_cyq` 由本地三角形分布法写入 10,351,463 行）

## 四、给你的量化项目（daily_stock_analysis）的落地优先级

**P0 — 股票衍生因子（已全部建成 ✅）**
1. ✅ moneyflow 资金流向 → 主力/大单净流入因子
2. ✅ margin / margin_detail 融资融券 → 杠杆情绪因子
3. ✅ stk_holdernumber 股东人数 → 筹码集中度因子
4. ✅ stk_limit 涨跌停 → 流动性/极端情绪
5. ✅ top_list / top_inst 龙虎榜 → 游资跟踪

**P1 — 基准与行业中性（已全部建成 ✅）**
6. ✅ index_basic + index_weight + index_daily → 指数基准
7. ✅ index_classify + index_member_all → 申万行业分类，做行业中性
8. ✅ disclosure_date → 财报披露日历，避开窗口期

**P2 — 多资产扩展（看策略是否需要，未建）**
9. cb_basic / cb_issue / cb_daily 可转债
10. fund_basic / fund_nav / fund_daily 基金
11. fut_basic / fut_daily 期货

**P3 — 宏观 / 海外（辅助）**
12. shibor ✅ 已建；shibor_lpr 已随宏观利率模块移除（不再建）
13. hk_basic 未建；hk_hold 已移除
14. fx_daily 外汇（未建）

**可跳过**：weekly / monthly（用 daily 重采样即可）；期货交易日历（复用 trade_cal）。

## 五、后续建议
- 建表可沿用现有「按年分区」模式，已实际应用于 `tushare_moneyflow`/`tushare_margin_detail`/`tushare_stk_limit`/`tushare_index_daily`（2010–2030+default 共 22 分区），并与 `tushare_sync_log` 同步机制保持风格统一。
- 未建表的 20 个接口已无 P0/P1 缺口；P2/P3（基金/期货/期权/可转债/外汇/港股）按策略需要再补。
- 空表遗留已清零：`tushare_dividend`/`tushare_disclosure_date` 等由 `incremental_fin.py` 每周三/六增量更新，`tushare_cyq` 由 `incremental_cyq.py` 本地计算，均有数据。

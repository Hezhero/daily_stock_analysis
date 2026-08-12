优化 backtest_5y_23strategies.py 提升胜率的分析
一、现状诊断（基于代码与数据库实况）
数据使用情况
load_data()（L380-441）从 PostgreSQL 加载数据，实际只用到了：
来源表	用到的字段
tushare_daily	open/high/low/close/vol→volume/amount/pct_chg
tushare_stock_basic	name、list_date（次新过滤）、exchange/market
tushare_daily_basic	仅 3 个：turnover_rate(turn)、pe_ttm、pb
tushare_adj_factor	前复权
关键发现：数据库里有大量已填充但回测完全没用的增强数据
根据 incremental_factor.py、incremental_index.py、incremental_cyq.py 等脚本，以下表是有增量填充逻辑的（即实际有数据）：
- tushare_daily_basic 还有：volume_ratio(量比)、total_mv/circ_mv(市值)、dv_ratio(股息率)、ps_ttm、turnover_rate_f(自由流通换手) —— 全都没用
- tushare_moneyflow：大小单资金流向、净流入 net_mf_amount —— 主力资金是 A 股短线胜率最强的因子之一，完全没用
- tushare_stk_limit：每日精确涨停价 up_limit/down_limit —— 现在用 9.5%/19.5% 近似阈值（L614-623），不精确
- tushare_index_daily：大盘指数日线（上证/深证/创业板）—— 可做市场环境/牛熊过滤（regime filter），完全没用
- tushare_top_list/top_inst：龙虎榜 —— 游资/机构席位，没用
- tushare_margin_detail：融资融券余额/买入 —— 杠杆资金情绪，没用
- tushare_cyq：筹码分布（获利比例/集中度）—— 筹码因子，没用
- tushare_fina_indicator：ROE/毛利率/净利率/营收增速等 —— 财务质量过滤，没用
- tushare_stk_holdernumber：股东户数 —— 筹码集中趋势，没用
- tushare_stock_basic.industry：行业 —— 可做行业轮动/规避，没用
结论：回测目前是"纯量价 + 3 个估值字段"的骨架，把库里最值钱的资金流、市场环境、筹码、基本面因子都闲置了。提升胜率的最大杠杆就在这里。
二、按"性价比 × 数据可得性"排序的优化建议
🥇 A 类：直接提升信号质量（高收益、低风险、数据现成）
A1. 加入市场环境（Regime）过滤 —— 最推荐
- 数据：tushare_index_daily（上证指数 000001.SH）
- 做法：指数在 MA20/MA60 上方且 MA20 上行时，才允许开仓；指数跌破关键均线（如 5 日线下穿 20 日线）时，全部策略空仓。
- 原理：A 股 80% 的个股收益由 beta 驱动，熊市里再好的技术信号胜率也大幅下降。这是全局胜率提升最显著的一招。
- 实现：在 _backtest_single 里对信号加一个 market_ok 掩码。
A2. 用 tushare_stk_limit 精确涨停判定，替换 9.5%/19.5% 近似
- 数据：tushare_stk_limit.up_limit
- 做法：close >= up_limit * 0.999 才算涨停封板（不可成交）；同时可用 down_limit 识别跌停（避免抄底信号接飞刀）。
- 原理：现在用固定阈值在科创板（20%）/ST（5%）/北交所（30%）上会误判，导致把"真涨停买不进"或"ST 假涨停"混入样本，污染胜率统计。
A3. 主力资金（Moneyflow）确认
- 数据：tushare_moneyflow.net_mf_amount（净流入额）、大单净流入
- 做法：信号触发日要求 net_mf_amount > 0（或近 3 日累计净流入为正），大幅过滤"放量但主力出货"的假突破。
- 原理：量价策略最大的坑是"放量 = 出货"。资金流是区分"放量上涨"与"放量诱多"的最强判别器。
🥈 B 类：加过滤条件压缩样本、提高胜率（中收益、低风险）
B1. 市值/流动性过滤（用 circ_mv）
- 数据：tushare_daily_basic.circ_mv（流通市值，万元）
- 做法：过滤掉过小盘（< 20 亿，易被操纵、滑点大）和超大盘（> 500 亿，弹性差）。A 股短线策略在 50~300 亿流通市值区间胜率通常最高。
- 现有 MIN_DAILY_AMOUNT_K=5000 只过滤了成交额，没过滤市值。
B2. 量比确认（用 volume_ratio）
- 数据：tushare_daily_basic.volume_ratio
- 做法：信号日 volume_ratio >= 1.5（相对近期活跃），替代/补充现在手工算的 vol_ma5 放量判断。量比是 Tushare 官方归一化指标，比自算更稳定。
B3. 财务质量过滤（用 tushare_fina_indicator）
- 数据：roe、grossprofit_margin、or_yoy（营收增速）
- 做法：剔除 ROE 为负、营收大幅下滑的"垃圾股"信号。技术面买点 + 基本面不爆雷，胜率双保险。
- 注意：财务数据有披露滞后，需用 ann_date 对齐避免前视偏差。
🥉 C 类：组合/退出机制优化（策略层面）
C1. 组合策略权重化（替代简单"多策略共振"）
- 现状：sig_ensemble 只要 3 个基础策略命中 ≥2 就触发，没有区分各策略历史胜率。
- 优化：给每个策略按其 5 年回测胜率/盈亏比加权，多策略共振时用加权分而非简单计数。推荐逻辑 get_next_day_recommendations 已在用胜率加权（L1684），但回测信号层没做。
C2. 加入止盈止损（目前完全没有退出机制）
- 现状：回测用固定持有期 ret_{1,3,5,10}d（L595），信号触发后无论涨跌都持有到期，没有任何动态退出。
- 优化：
- ATR 止损：stop = close - 2 × ATR(20)，跌破即平仓（用 low 判断触发）。
- 移动止盈：持有期内最高价回撤 8% 平仓，锁定利润。
- 时间止损：持有 10 日未达预期收益（如 < 3%）提前退出。
- 效果：名义胜率可能略降，但盈亏比（profit_loss_ratio）会显著上升，期望收益和夏普提升。这是"有效胜率"——最终账户增长——的真正来源。当前 calc_metrics 里 win_rate 和 profit_loss_ratio 是分开统计的（L1146-1149），优化目标应该是 期望值 = 胜率 × 均盈 - (1-胜率) × 均亏，而不是单纯刷胜率。
C3. 信号去重 / 冷却期（Cooldown）
- 现状：同一股票连续多日触发同策略信号，会被重复统计（如涨停回调策略可能连续 2-3 天都满足条件），造成样本自相关、虚增交易数。
- 优化：同一股票同一策略 N 日内（如 5 日）只取第一次信号；不同策略同一天命中同一股票，在组合层合并为一次交易。
- 效果：样本更独立，胜率统计更真实；也避免"一支股票贡献几十笔高度相关交易"扭曲排序（get_top_stocks_by_win_rate 里 strategy_count 也会有偏差）。
🏅 D 类：统计口径与评估改进（不直接提胜率，但让优化方向正确）
D1. 分年度/分市场环境评估，避免"幸存者偏差式"优化
- 现状：run_backtests 只输出 5 年整体指标（L1422-1425），无法判断策略是"牛市赚的"还是"全周期都行"。
- 优化：按年度（2021/2022/2023/2024/2025）拆分胜率与收益。只在某一两年表现好、其余年份大幅亏损的策略要警惕——那是过拟合/牛市 beta，不是 alpha。配合 A1 的市场环境过滤，可以验证"策略在熊市是否仍有效"。
D2. 用 tushare_index_daily 替代手工等权基准
- 现状：compute_benchmark_metrics（L1330-1355）注释明确说"本地库无指数日线表，用全股票等权平均"——但库里其实有 tushare_index_daily（incremental_index.py 会填充）。
- 优化：直接用上证指数/沪深300 做基准，超额收益（alpha）的计算才准确。等权全股票平均会被小盘股拉高，基准失真。
D3. 交易成本建模
- 现状：回测完全没有手续费/印花税/滑点。A 股双边成本约 0.1%~0.2%（佣金+印花税+滑点），对 1-5 日短线策略影响巨大——高换手策略的名义胜率会被真实成本吃掉。
- 优化：每笔收益扣 0.15% 成本再算胜率/期望。这一步做完，很多"高胜率"策略会现原形，剩下的才是真 alpha。
三、优先级路线图（建议实施顺序）
阶段	动作	预期效果	风险
P0（立刻做）	A2 精确涨停判定（stk_limit）+ D3 交易成本	数据真实化，避免统计幻觉	低
P0	A1 市场环境过滤（index_daily）	全局胜率最显著提升	低
P1	A3 主力资金确认（moneyflow）	过滤假突破，胜率+盈亏比双升	中（moneyflow 需确认数据覆盖年份）
P1	B1 市值过滤（circ_mv）+ B2 量比（volume_ratio）	压缩低质量样本	低
P2	C2 止盈止损 + C1 加权组合	提升期望收益/夏普	中（需防过拟合）
P2	B3 财务过滤（fina_indicator）+ D1 分年度评估	稳健性验证	中（需处理披露滞后）
P3	C3 信号去重 + D2 真实指数基准	统计口径修正	低
四、关键风险提示
1. 前视偏差（Look-ahead bias）：加入 fina_indicator、top_list、margin_detail 等低频数据时，必须用 ann_date 而非 end_date/trade_date 对齐，否则会偷看未来信息，胜率虚高。这是数据增强优化里最容易犯、后果最严重的错误。
2. 数据覆盖确认：moneyflow/stk_limit/index_daily 等扩展表由 incremental_factor.py/incremental_index.py 填充，建议先跑一条 SQL 确认各表 5 年区间的实际覆盖率和行数，再决定依赖程度——如果只有最近 1-2 年数据，回测前段会大量缺失，反而引入偏差。
3. 胜率不是唯一目标：win_rate 是当前 Top-N 策略排序和 get_top_stocks_by_win_rate 推荐的主要依据，但单纯刷胜率可能牺牲盈亏比。建议排序指标从 total_return 扩展为 期望值或凯利分数，兼顾胜率与盈亏比。
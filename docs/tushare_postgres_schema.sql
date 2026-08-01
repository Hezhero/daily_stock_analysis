-- ============================================================
-- Tushare 2000 积分权限接口 → PostgreSQL 本地缓存表
-- ============================================================
-- 适用版本：PostgreSQL 14+
-- 设计原则：
--   1. 每个 Tushare API 对应一张缓存表，字段与官方返回严格对齐
--   2. 大表（日线/复权/每日指标）使用 RANGE 分区（按 trade_date 按年分区）
--   3. 财务表使用 ts_code + end_date 唯一约束，支持增量更新
--   4. 保留 ts_code（Tushare 格式）与 raw_code（6 位纯数字）双字段
--   5. created_at / updated_at 审计字段
--   6. BRIN 索引用于时序扫描，B-tree 用于点查
-- ============================================================

BEGIN;

-- ============================================================
-- 0. 数据库与 Schema
-- ============================================================
-- CREATE DATABASE stock_cache WITH ENCODING 'UTF8' LC_COLLATE 'zh_CN.UTF-8';
-- \c stock_cache
-- CREATE SCHEMA IF NOT EXISTS tushare;
-- SET search_path TO tushare, public;

-- ============================================================
-- 1. 基础数据层（低频更新，全量覆盖）
-- ============================================================

-- 1.1 股票基础信息
-- API: stock_basic
-- 频次: 50次/分钟
-- 建议: 首次全量拉取后，仅定期刷新（如每日一次检查新增上市）
CREATE TABLE IF NOT EXISTS tushare_stock_basic (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,   -- 如 600519.SH
    symbol          VARCHAR(6)   NOT NULL,   -- 6位纯代码 600519
    name            VARCHAR(50)  NOT NULL,   -- 股票名称
    area            VARCHAR(10),             -- 地区
    industry        VARCHAR(20),             -- 所属行业
    cnspell         VARCHAR(30),             -- 拼音缩写
    market          VARCHAR(10),             -- 市场类型（主板/创业板/科创板/北交所）
    list_status     VARCHAR(2)   DEFAULT 'L',-- 上市状态 L上市 D退市 P暂停
    list_date       DATE,                    -- 上市日期
    delist_date     DATE,                    -- 退市日期
    is_hs           VARCHAR(2),              -- 是否沪深港通标的 N/H/S
    act_name        VARCHAR(200),            -- 实控人名称
    act_ent_type    VARCHAR(100),            -- 实控人企业性质
    fullname        VARCHAR(200),            -- 公司全称
    exchange        VARCHAR(10),             -- 交易所代码 SSE/SZSE/BSE
    curr_type       VARCHAR(10),             -- 交易货币
    enname          VARCHAR(200),            -- 英文全称
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_stock_basic_code UNIQUE (ts_code)
);

CREATE INDEX IF NOT EXISTS ix_ts_stock_basic_symbol ON tushare_stock_basic(symbol);
CREATE INDEX IF NOT EXISTS ix_ts_stock_basic_list_date ON tushare_stock_basic(list_date);
CREATE INDEX IF NOT EXISTS ix_ts_stock_basic_industry ON tushare_stock_basic(industry);
CREATE INDEX IF NOT EXISTS ix_ts_stock_basic_status ON tushare_stock_basic(list_status);
COMMENT ON TABLE tushare_stock_basic IS 'Tushare stock_basic 股票基础信息缓存';

-- 1.2 交易日历
-- API: trade_cal
-- 频次: 基础权限
-- 建议: 每年全量拉取一次，缓存到本地
CREATE TABLE IF NOT EXISTS tushare_trade_cal (
    id              BIGSERIAL PRIMARY KEY,
    exchange        VARCHAR(10)  NOT NULL,   -- 交易所 SSE/SZSE/CFFEX/SHFE/...
    cal_date        DATE         NOT NULL,   -- 日历日期
    is_open         SMALLINT     NOT NULL DEFAULT 0, -- 是否交易 1=开市 0=休市
    pretrade_date   DATE,                    -- 上一个交易日
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_trade_cal_exch_date UNIQUE (exchange, cal_date)
);

CREATE INDEX IF NOT EXISTS ix_ts_trade_cal_date ON tushare_trade_cal(cal_date);
CREATE INDEX IF NOT EXISTS ix_ts_trade_cal_open ON tushare_trade_cal(exchange, is_open, cal_date);
COMMENT ON TABLE tushare_trade_cal IS 'Tushare trade_cal 交易日历缓存';

-- 1.3 上市公司基本信息
-- API: stock_company
-- 频次: 基础权限
CREATE TABLE IF NOT EXISTS tushare_stock_company (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,
    com_name        VARCHAR(200),            -- 公司名称
    chairman        VARCHAR(50),             -- 法人代表
    manager         VARCHAR(50),             -- 总经理
    secretary       VARCHAR(50),             -- 董秘
    reg_capital     NUMERIC(20,4),           -- 注册资本（万元）
    setup_date      DATE,                    -- 注册日期
    province        VARCHAR(30),             -- 所在省份
    city            VARCHAR(30),             -- 所在城市
    introduction    TEXT,                    -- 公司介绍
    website         VARCHAR(200),            -- 公司主页
    email           VARCHAR(100),            -- 电子邮件
    office          VARCHAR(300),            -- 办公地址
    employees       INTEGER,                 -- 员工人数
    main_business   TEXT,                    -- 主要业务及产品
    business_scope  TEXT,                    -- 经营范围
    phone           VARCHAR(50),             -- 联系电话
    fax             VARCHAR(50),             -- 传真
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_stock_company_code UNIQUE (ts_code)
);
COMMENT ON TABLE tushare_stock_company IS 'Tushare stock_company 上市公司基本信息缓存';

-- 1.4 股票曾用名
-- API: namechange
CREATE TABLE IF NOT EXISTS tushare_namechange (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,
    name            VARCHAR(50)  NOT NULL,   -- 股票名称
    start_date      DATE         NOT NULL,   -- 开始日期
    end_date        DATE,                    -- 结束日期
    change_reason   VARCHAR(200),            -- 变更原因
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_namechange_code_date UNIQUE (ts_code, start_date)
);

CREATE INDEX IF NOT EXISTS ix_ts_namechange_code ON tushare_namechange(ts_code);
COMMENT ON TABLE tushare_namechange IS 'Tushare namechange 股票曾用名缓存';

-- 1.5 沪深港通成分股
-- API: hs_const
CREATE TABLE IF NOT EXISTS tushare_hs_const (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,
    hs_type         VARCHAR(2)   NOT NULL,   -- SH沪/SZ深
    in_date         DATE         NOT NULL,   -- 纳入日期
    out_date        DATE,                    -- 剔除日期
    is_new          VARCHAR(1),              -- 是否最新 1是 0否
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_hs_const_code_type_date UNIQUE (ts_code, hs_type, in_date)
);

CREATE INDEX IF NOT EXISTS ix_ts_hs_const_type ON tushare_hs_const(hs_type, in_date);
COMMENT ON TABLE tushare_hs_const IS 'Tushare hs_const 沪深港通成分股缓存';

-- 1.6 IPO 新股列表
-- API: ipo_list
-- 注: 此接口为 2025年新增，具体字段以官方文档为准
CREATE TABLE IF NOT EXISTS tushare_ipo_list (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12),             -- 新股代码
    name            VARCHAR(50)  NOT NULL,   -- 新股名称
    ipo_date        DATE,                    -- 上市日期
    issue_date      DATE,                    -- 申购日期
    amount          NUMERIC(20,4),           -- 发行数量（万股）
    market          VARCHAR(10),             -- 市场类型
    price           NUMERIC(12,4),           -- 发行价格
    pe              NUMERIC(12,4),           -- 发行市盈率
    limit_amount    NUMERIC(20,4),           -- 网上发行量
    funds           NUMERIC(20,4),           -- 募集资金（万元）
    ballot          NUMERIC(12,6),           -- 中签率
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_ipo_list_code_date UNIQUE (ts_code, ipo_date)
);

CREATE INDEX IF NOT EXISTS ix_ts_ipo_list_date ON tushare_ipo_list(ipo_date);
COMMENT ON TABLE tushare_ipo_list IS 'Tushare ipo_list IPO新股列表缓存';

-- ============================================================
-- 2. 行情数据层（高频增量更新，大表分区）
-- ============================================================

-- 2.1 A 股日线行情（分区主表）
-- API: daily
-- 频次: 500次/分钟，单次6000条
-- 策略: 按 trade_date 按年 RANGE 分区；每日增量拉取前一日数据
CREATE TABLE IF NOT EXISTS tushare_daily (
    id              BIGSERIAL,
    ts_code         VARCHAR(12)  NOT NULL,   -- 股票代码（如 600519.SH）
    trade_date      DATE         NOT NULL,   -- 交易日期（分区键）
    open            NUMERIC(12,4),           -- 开盘价
    high            NUMERIC(12,4),           -- 最高价
    low             NUMERIC(12,4),           -- 最低价
    close           NUMERIC(12,4),           -- 收盘价
    pre_close       NUMERIC(12,4),           -- 昨收价
    change_val      NUMERIC(12,4),           -- 涨跌额
    pct_chg         NUMERIC(10,4),           -- 涨跌幅（%）
    vol             NUMERIC(20,4),           -- 成交量（手）
    amount          NUMERIC(22,4),           -- 成交额（千元）
    turnover_ratio  NUMERIC(10,4),           -- 换手率（%）
    volume_ratio    NUMERIC(10,4),           -- 量比
    amplitude       NUMERIC(10,4),           -- 振幅（%）
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_ts_daily PRIMARY KEY (id, trade_date),
    CONSTRAINT uix_ts_daily_code_date UNIQUE (ts_code, trade_date)
) PARTITION BY RANGE (trade_date);

COMMENT ON TABLE tushare_daily IS 'Tushare daily A股日线行情缓存（分区表）';

-- 创建默认分区（兜底）
CREATE TABLE IF NOT EXISTS tushare_daily_default PARTITION OF tushare_daily DEFAULT;

-- 按年创建分区（示例 2010-2030）
-- 实际使用时根据需要调整年份范围
DO $$
DECLARE
    y INTEGER;
BEGIN
    FOR y IN 2010..2030 LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS tushare_daily_%s
            PARTITION OF tushare_daily
            FOR VALUES FROM (%L) TO (%L)',
            y,
            format('%s-01-01', y),
            format('%s-01-01', y + 1)
        );
    END LOOP;
END $$;

-- 索引（在分区表上创建，会自动应用到各子分区）
CREATE INDEX IF NOT EXISTS ix_ts_daily_code ON tushare_daily(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_daily_date ON tushare_daily(trade_date);
-- BRIN 索引适用于时间序列顺序扫描
CREATE INDEX IF NOT EXISTS ix_ts_daily_date_brin ON tushare_daily USING BRIN(trade_date);

-- 2.2 复权因子（分区主表）
-- API: adj_factor
-- 频次: 2000积分可用
-- 策略: 与 daily 对齐，按 trade_date 按年分区
CREATE TABLE IF NOT EXISTS tushare_adj_factor (
    id              BIGSERIAL,
    ts_code         VARCHAR(12)  NOT NULL,
    trade_date      DATE         NOT NULL,   -- 分区键
    adj_factor      NUMERIC(14,6) NOT NULL,  -- 复权因子
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_ts_adj_factor PRIMARY KEY (id, trade_date),
    CONSTRAINT uix_ts_adj_factor_code_date UNIQUE (ts_code, trade_date)
) PARTITION BY RANGE (trade_date);

COMMENT ON TABLE tushare_adj_factor IS 'Tushare adj_factor 复权因子缓存（分区表）';

CREATE TABLE IF NOT EXISTS tushare_adj_factor_default PARTITION OF tushare_adj_factor DEFAULT;

DO $$
DECLARE
    y INTEGER;
BEGIN
    FOR y IN 2010..2030 LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS tushare_adj_factor_%s
            PARTITION OF tushare_adj_factor
            FOR VALUES FROM (%L) TO (%L)',
            y,
            format('%s-01-01', y),
            format('%s-01-01', y + 1)
        );
    END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS ix_ts_adj_factor_code ON tushare_adj_factor(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_adj_factor_date ON tushare_adj_factor(trade_date);
CREATE INDEX IF NOT EXISTS ix_ts_adj_factor_date_brin ON tushare_adj_factor USING BRIN(trade_date);

-- 2.3 每日基本面指标（分区主表）
-- API: daily_basic
-- 频次: 至少2000积分
-- 字段: PE/PB/换手率/总市值/流通市值等
CREATE TABLE IF NOT EXISTS tushare_daily_basic (
    id              BIGSERIAL,
    ts_code         VARCHAR(12)  NOT NULL,
    trade_date      DATE         NOT NULL,   -- 分区键
    close           NUMERIC(14,4),           -- 当日收盘价
    turnover_rate   NUMERIC(12,4),           -- 换手率（%）
    turnover_rate_f NUMERIC(12,4),           -- 换手率（自由流通股）
    volume_ratio    NUMERIC(10,4),           -- 量比
    pe              NUMERIC(16,4),           -- 市盈率（TTM）
    pe_ttm          NUMERIC(16,4),           -- 市盈率（TTM）
    pb              NUMERIC(16,4),           -- 市净率
    ps              NUMERIC(16,4),           -- 市销率
    ps_ttm          NUMERIC(16,4),           -- 市销率（TTM）
    dv_ratio        NUMERIC(12,4),           -- 股息率（%）
    dv_ttm          NUMERIC(12,4),           -- 股息率（TTM）
    total_share     NUMERIC(20,4),           -- 总股本（万股）
    float_share     NUMERIC(20,4),           -- 流通股本（万股）
    free_share      NUMERIC(20,4),           -- 自由流通股本（万股）
    total_mv        NUMERIC(22,4),           -- 总市值（万元）
    circ_mv         NUMERIC(22,4),           -- 流通市值（万元）
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_ts_daily_basic PRIMARY KEY (id, trade_date),
    CONSTRAINT uix_ts_daily_basic_code_date UNIQUE (ts_code, trade_date)
) PARTITION BY RANGE (trade_date);

COMMENT ON TABLE tushare_daily_basic IS 'Tushare daily_basic 每日基本面指标缓存（分区表）';

CREATE TABLE IF NOT EXISTS tushare_daily_basic_default PARTITION OF tushare_daily_basic DEFAULT;

DO $$
DECLARE
    y INTEGER;
BEGIN
    FOR y IN 2010..2030 LOOP
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS tushare_daily_basic_%s
            PARTITION OF tushare_daily_basic
            FOR VALUES FROM (%L) TO (%L)',
            y,
            format('%s-01-01', y),
            format('%s-01-01', y + 1)
        );
    END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS ix_ts_daily_basic_code ON tushare_daily_basic(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_daily_basic_date ON tushare_daily_basic(trade_date);
CREATE INDEX IF NOT EXISTS ix_ts_daily_basic_date_brin ON tushare_daily_basic USING BRIN(trade_date);

-- ============================================================
-- 3. 财务数据层（按报告期更新，可按季/年增量拉取）
-- ============================================================

-- 3.1 利润表
-- API: income
-- 字段: 按 Tushare 官方 income 接口字段对齐（约 100+ 字段）
-- 保留核心字段，扩展字段可用 JSONB 存储
CREATE TABLE IF NOT EXISTS tushare_income (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,
    ann_date        DATE,                    -- 公告日期
    f_ann_date      DATE,                    -- 实际公告日期
    end_date        DATE         NOT NULL,   -- 报告期截止日
    report_type     VARCHAR(5),              -- 报告类型: Q1/Q2半/Q3/年报
    comp_type       VARCHAR(2),              -- 公司类型: 1一般工商业 2银行 3保险 4证券
    end_type        VARCHAR(2),              -- 报告期类型: 1合并 2母公司
    -- 核心利润表字段
    total_revenue           NUMERIC(22,4),   -- 营业总收入
    revenue                 NUMERIC(22,4),   -- 营业收入
    int_income              NUMERIC(22,4),   -- 利息收入
    prem_earned             NUMERIC(22,4),   -- 已赚保费
    comm_income             NUMERIC(22,4),   -- 手续费及佣金收入
    total_cogs              NUMERIC(22,4),   -- 营业总成本
    oper_cost               NUMERIC(22,4),   -- 营业成本
    sell_exp                NUMERIC(22,4),   -- 销售费用
    admin_exp               NUMERIC(22,4),   -- 管理费用
    fin_exp                 NUMERIC(22,4),   -- 财务费用
    assets_impair_loss      NUMERIC(22,4),   -- 资产减值损失
    fair_value_inter_gain   NUMERIC(22,4),   -- 公允价值变动收益
    invest_income           NUMERIC(22,4),   -- 投资收益
    oper_profit             NUMERIC(22,4),   -- 营业利润
    non_oper_income         NUMERIC(22,4),   -- 营业外收入
    non_oper_exp            NUMERIC(22,4),   -- 营业外支出
    total_profit            NUMERIC(22,4),   -- 利润总额
    income_tax              NUMERIC(22,4),   -- 所得税费用
    n_income                NUMERIC(22,4),   -- 净利润
    n_income_attr_p         NUMERIC(22,4),   -- 归母净利润
    minority_gain           NUMERIC(22,4),   -- 少数股东损益
    basic_eps               NUMERIC(16,6),   -- 基本每股收益
    diluted_eps             NUMERIC(16,6),   -- 稀释每股收益
    -- 扩展字段（JSONB 存储 Tushare 返回的其他字段）
    extra_fields            JSONB,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_income_code_end_date_type UNIQUE (ts_code, end_date, report_type, comp_type)
);

CREATE INDEX IF NOT EXISTS ix_ts_income_code ON tushare_income(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_income_end_date ON tushare_income(end_date);
CREATE INDEX IF NOT EXISTS ix_ts_income_ann_date ON tushare_income(ann_date);
COMMENT ON TABLE tushare_income IS 'Tushare income 利润表缓存';

-- 3.2 资产负债表
-- API: balancesheet
CREATE TABLE IF NOT EXISTS tushare_balancesheet (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,
    ann_date        DATE,
    f_ann_date      DATE,
    end_date        DATE         NOT NULL,
    report_type     VARCHAR(5),
    comp_type       VARCHAR(2),
    end_type        VARCHAR(2),
    -- 核心资产负债表字段
    total_assets            NUMERIC(22,4),   -- 资产总计
    total_cur_assets        NUMERIC(22,4),   -- 流动资产合计
    money_cap               NUMERIC(22,4),   -- 货币资金
    trad_asset              NUMERIC(22,4),   -- 交易性金融资产
    notes_receiv            NUMERIC(22,4),   -- 应收票据
    accounts_receiv         NUMERIC(22,4),   -- 应收账款
    prepayment              NUMERIC(22,4),   -- 预付款项
    inventories             NUMERIC(22,4),   -- 存货
    total_non_cur_assets    NUMERIC(22,4),   -- 非流动资产合计
    fix_assets              NUMERIC(22,4),   -- 固定资产
    constru_in_process      NUMERIC(22,4),   -- 在建工程
    intangible_assets       NUMERIC(22,4),   -- 无形资产
    goodwill                NUMERIC(22,4),   -- 商誉
    total_liab              NUMERIC(22,4),   -- 负债合计
    total_cur_liab          NUMERIC(22,4),   -- 流动负债合计
    short_borrow            NUMERIC(22,4),   -- 短期借款
    notes_payable           NUMERIC(22,4),   -- 应付票据
    accounts_payable        NUMERIC(22,4),   -- 应付账款
    total_non_cur_liab      NUMERIC(22,4),   -- 非流动负债合计
    long_borrow             NUMERIC(22,4),   -- 长期借款
    total_hldr_eqy_exc_min  NUMERIC(22,4),   -- 股东权益合计（不含少数股东权益）
    minority_int            NUMERIC(22,4),   -- 少数股东权益
    total_hldr_eqy_inc_min  NUMERIC(22,4),   -- 股东权益合计（含少数股东权益）
    extra_fields            JSONB,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_balancesheet_code_end_date_type UNIQUE (ts_code, end_date, report_type, comp_type)
);

CREATE INDEX IF NOT EXISTS ix_ts_balancesheet_code ON tushare_balancesheet(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_balancesheet_end_date ON tushare_balancesheet(end_date);
COMMENT ON TABLE tushare_balancesheet IS 'Tushare balancesheet 资产负债表缓存';

-- 3.3 现金流量表
-- API: cashflow
CREATE TABLE IF NOT EXISTS tushare_cashflow (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,
    ann_date        DATE,
    f_ann_date      DATE,
    end_date        DATE         NOT NULL,
    report_type     VARCHAR(5),
    comp_type       VARCHAR(2),
    end_type        VARCHAR(2),
    -- 核心现金流量表字段
    c_fr_sale_sg            NUMERIC(22,4),   -- 销售商品、提供劳务收到的现金
    net_cf_oper_act         NUMERIC(22,4),   -- 经营活动产生的现金流量净额
    net_cf_inv_act          NUMERIC(22,4),   -- 投资活动产生的现金流量净额
    net_cf_fin_act          NUMERIC(22,4),   -- 筹资活动产生的现金流量净额
    free_cf                 NUMERIC(22,4),   -- 企业自由现金流
    -- 经营活动明细
    st_cash_out_act         NUMERIC(22,4),   -- 经营活动现金流出小计
    st_cash_in_act          NUMERIC(22,4),   -- 经营活动现金流入小计
    -- 投资活动明细
    st_cash_out_inv         NUMERIC(22,4),   -- 投资活动现金流出小计
    st_cash_in_inv          NUMERIC(22,4),   -- 投资活动现金流入小计
    -- 筹资活动明细
    st_cash_out_fin         NUMERIC(22,4),   -- 筹资活动现金流出小计
    st_cash_in_fin          NUMERIC(22,4),   -- 筹资活动现金流入小计
    n_cashflow_act          NUMERIC(22,4),   -- 汇率变动对现金的影响
    c_change                NUMERIC(22,4),   -- 现金及现金等价物净增加额
    c_bal_end               NUMERIC(22,4),   -- 期末现金及现金等价物余额
    extra_fields            JSONB,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_cashflow_code_end_date_type UNIQUE (ts_code, end_date, report_type, comp_type)
);

CREATE INDEX IF NOT EXISTS ix_ts_cashflow_code ON tushare_cashflow(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_cashflow_end_date ON tushare_cashflow(end_date);
COMMENT ON TABLE tushare_cashflow IS 'Tushare cashflow 现金流量表缓存';

-- 3.4 业绩预告
-- API: forecast
CREATE TABLE IF NOT EXISTS tushare_forecast (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,
    ann_date        DATE,
    end_date        DATE         NOT NULL,   -- 报告期
    type            VARCHAR(10),             -- 预告类型: 预增/预减/扭亏/首亏/续盈/略增/略减/不确定
    p_change_min    NUMERIC(16,4),           -- 净利润变动下限（%）
    p_change_max    NUMERIC(16,4),           -- 净利润变动上限（%）
    net_profit_min  NUMERIC(22,4),           -- 净利润下限（万元）
    net_profit_max  NUMERIC(22,4),           -- 净利润上限（万元）
    last_parent_net NUMERIC(22,4),           -- 上年同期归母净利润
    notice_date     DATE,                    -- 公告日期
    notice_reason   TEXT,                    -- 业绩变动原因
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_forecast_code_end_ann UNIQUE (ts_code, end_date, ann_date)
);

CREATE INDEX IF NOT EXISTS ix_ts_forecast_code ON tushare_forecast(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_forecast_end_date ON tushare_forecast(end_date);
COMMENT ON TABLE tushare_forecast IS 'Tushare forecast 业绩预告缓存';

-- 3.5 业绩快报
-- API: express
CREATE TABLE IF NOT EXISTS tushare_express (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,
    ann_date        DATE,
    end_date        DATE         NOT NULL,   -- 报告期
    revenue         NUMERIC(22,4),           -- 营业收入（元）
    operate_profit  NUMERIC(22,4),           -- 营业利润（元）
    total_profit    NUMERIC(22,4),           -- 利润总额（元）
    n_income        NUMERIC(22,4),           -- 净利润（元）
    total_assets    NUMERIC(22,4),           -- 总资产（元）
    total_hldr_eqy  NUMERIC(22,4),           -- 股东权益合计（不含少数股东权益）
    diluted_eps     NUMERIC(16,6),           -- 稀释每股收益
    weighted_roe    NUMERIC(16,6),           -- 加权平均净资产收益率
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_express_code_end_date UNIQUE (ts_code, end_date)
);

CREATE INDEX IF NOT EXISTS ix_ts_express_code ON tushare_express(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_express_end_date ON tushare_express(end_date);
COMMENT ON TABLE tushare_express IS 'Tushare express 业绩快报缓存';

-- 3.6 分红送股
-- API: dividend
CREATE TABLE IF NOT EXISTS tushare_dividend (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,
    end_date        DATE,                    -- 分红年度
    ann_date        DATE,                    -- 预案公告日
    div_proc        VARCHAR(50),             -- 方案进度: 预案/决案/实施
    stk_div         NUMERIC(16,6),           -- 每股送转（股）
    stk_bo_rate     NUMERIC(16,6),           -- 每股送股
    stk_co_rate     NUMERIC(16,6),           -- 每股转增
    cash_div        NUMERIC(16,6),           -- 每股派息（税前，元）
    cash_div_tax    NUMERIC(16,6),           -- 每股派息（税后，元）
    record_date     DATE,                    -- 股权登记日
    ex_date         DATE,                    -- 除权除息日
    pay_date        DATE,                    -- 派息日
    div_listdate    DATE,                    -- 红股上市日
    imp_ann_date    DATE,                    -- 实施公告日
    base_date       DATE,                    -- 基准日
    base_share      NUMERIC(20,4),           -- 实施基准股本（万股）
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_dividend_code_end_ann_proc UNIQUE (ts_code, end_date, ann_date, div_proc)
);

CREATE INDEX IF NOT EXISTS ix_ts_dividend_code ON tushare_dividend(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_dividend_ex_date ON tushare_dividend(ex_date);
COMMENT ON TABLE tushare_dividend IS 'Tushare dividend 分红送股缓存';

-- 3.7 财务指标
-- API: fina_indicator
-- 重要的综合财务指标表，涵盖盈利能力、偿债能力、营运能力等
CREATE TABLE IF NOT EXISTS tushare_fina_indicator (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,
    ann_date        DATE,
    end_date        DATE         NOT NULL,   -- 报告期
    -- 每股指标
    eps                 NUMERIC(16,6),       -- 基本每股收益
    dt_eps              NUMERIC(16,6),       -- 稀释每股收益
    total_revenue_ps    NUMERIC(16,6),       -- 每股营业收入
    revenue_ps          NUMERIC(16,6),       -- 每股营业收入（TTM）
    capital_rese_ps     NUMERIC(16,6),       -- 每股资本公积
    surplus_rese_ps     NUMERIC(16,6),       -- 每股盈余公积
    undist_profit_ps    NUMERIC(16,6),       -- 每股未分配利润
    -- 盈利能力
    grossprofit_margin  NUMERIC(16,6),       -- 销售毛利率（%）
    netprofit_margin    NUMERIC(16,6),       -- 销售净利率（%）
    roe                 NUMERIC(16,6),       -- 净资产收益率（%）
    roe_dt              NUMERIC(16,6),       -- 净资产收益率（摊薄，%）
    roa                 NUMERIC(16,6),       -- 总资产净利率（%）
    roa_yearly          NUMERIC(16,6),       -- 年化总资产净利率（%）
    roic                NUMERIC(16,6),       -- 投入资本回报率（%）
    -- 成长能力
    or_yoy              NUMERIC(16,6),       -- 营业收入同比增长率（%）
    op_yoy              NUMERIC(16,6),       -- 营业利润同比增长率（%）
    profit_yoy          NUMERIC(16,6),       -- 归属净利润同比增长率（%）
    equity_yoy          NUMERIC(16,6),       -- 净资产同比增长率（%）
    assets_yoy          NUMERIC(16,6),       -- 总资产同比增长率（%）
    -- 偿债能力
    debt_to_assets      NUMERIC(16,6),       -- 资产负债率（%）
    current_ratio       NUMERIC(16,6),       -- 流动比率
    quick_ratio         NUMERIC(16,6),       -- 速动比率
    equity_ratio        NUMERIC(16,6),       -- 产权比率
    -- 营运能力
    inv_turn            NUMERIC(16,6),       -- 存货周转率（次）
    ar_turn             NUMERIC(16,6),       -- 应收账款周转率（次）
    assets_turn         NUMERIC(16,6),       -- 总资产周转率（次）
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_fina_indicator_code_end_date UNIQUE (ts_code, end_date)
);

CREATE INDEX IF NOT EXISTS ix_ts_fina_indicator_code ON tushare_fina_indicator(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_fina_indicator_end_date ON tushare_fina_indicator(end_date);
COMMENT ON TABLE tushare_fina_indicator IS 'Tushare fina_indicator 财务指标缓存';

-- 3.8 财务审计意见
-- API: fina_audit
CREATE TABLE IF NOT EXISTS tushare_fina_audit (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,
    ann_date        DATE,
    end_date        DATE         NOT NULL,   -- 报告期
    audit_result    VARCHAR(100),            -- 审计结果: 标准无保留意见/保留意见/否定意见/无法表示意见
    audit_fees      NUMERIC(20,4),           -- 审计费用（元）
    audit_agency    VARCHAR(200),            -- 审计机构
    audit_sign      VARCHAR(100),            -- 签字会计师
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_fina_audit_code_end_date UNIQUE (ts_code, end_date)
);

CREATE INDEX IF NOT EXISTS ix_ts_fina_audit_code ON tushare_fina_audit(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_fina_audit_end_date ON tushare_fina_audit(end_date);
COMMENT ON TABLE tushare_fina_audit IS 'Tushare fina_audit 财务审计意见缓存';

-- 3.9 主营业务构成
-- API: fina_mainbz
CREATE TABLE IF NOT EXISTS tushare_fina_mainbz (
    id              BIGSERIAL PRIMARY KEY,
    ts_code         VARCHAR(12)  NOT NULL,
    end_date        DATE         NOT NULL,   -- 报告期
    bz_item         VARCHAR(200) NOT NULL,   -- 主营业务项目
    bz_code         VARCHAR(10),             -- 项目代码
    bz_sales        NUMERIC(22,4),           -- 主营业务收入（元）
    bz_profit       NUMERIC(22,4),           -- 主营业务利润（元）
    bz_cost         NUMERIC(22,4),           -- 主营业务成本（元）
    curr_type       VARCHAR(10),             -- 货币代码
    update_date     DATE,                    -- 更新日期
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_fina_mainbz_code_end_item UNIQUE (ts_code, end_date, bz_item, bz_code)
);

CREATE INDEX IF NOT EXISTS ix_ts_fina_mainbz_code ON tushare_fina_mainbz(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_fina_mainbz_end_date ON tushare_fina_mainbz(end_date);
COMMENT ON TABLE tushare_fina_mainbz IS 'Tushare fina_mainbz 主营业务构成缓存';

-- ============================================================
-- 4. 辅助视图：合并日线 + 复权因子 → 前/后复权价格
-- ============================================================

-- 视图: 合并 daily + adj_factor，提供复权价格
CREATE OR REPLACE VIEW v_tushare_daily_adj AS
SELECT
    d.ts_code,
    d.trade_date,
    d.open,
    d.high,
    d.low,
    d.close,
    d.pre_close,
    d.change_val,
    d.pct_chg,
    d.vol,
    d.amount,
    d.turnover_ratio,
    d.volume_ratio,
    d.amplitude,
    a.adj_factor,
    -- 前复权: 乘最新复权因子 / 当日复权因子
    -- 此处仅提供当日因子，前复权计算需在应用层结合最新因子
    (d.open   * a.adj_factor)::NUMERIC(12,4)  AS open_adj,
    (d.high   * a.adj_factor)::NUMERIC(12,4)  AS high_adj,
    (d.low    * a.adj_factor)::NUMERIC(12,4)  AS low_adj,
    (d.close  * a.adj_factor)::NUMERIC(12,4)  AS close_adj
FROM tushare_daily d
LEFT JOIN tushare_adj_factor a
    ON d.ts_code = a.ts_code AND d.trade_date = a.trade_date;

COMMENT ON VIEW v_tushare_daily_adj IS '日线行情 + 复权因子合并视图';

-- 视图: 个股全景（日线 + 每日指标 + 股票基本信息合并）
CREATE OR REPLACE VIEW v_tushare_stock_full AS
SELECT
    d.ts_code,
    b.symbol,
    b.name,
    b.industry,
    b.market,
    b.list_date,
    d.trade_date,
    d.open,
    d.high,
    d.low,
    d.close,
    d.pct_chg,
    d.vol,
    d.amount,
    d.turnover_ratio,
    db.pe,
    db.pb,
    db.ps,
    db.total_mv,
    db.circ_mv,
    db.dv_ratio
FROM tushare_daily d
JOIN tushare_stock_basic b ON d.ts_code = b.ts_code
LEFT JOIN tushare_daily_basic db ON d.ts_code = db.ts_code AND d.trade_date = db.trade_date;

COMMENT ON VIEW v_tushare_stock_full IS '个股日线全景视图（行情+基本面+基础信息）';

-- ============================================================
-- 5. 数据同步元数据表（记录每次缓存的拉取状态）
-- ============================================================

CREATE TABLE IF NOT EXISTS tushare_sync_log (
    id              BIGSERIAL PRIMARY KEY,
    api_name        VARCHAR(50)  NOT NULL,   -- API 名称（如 daily, income）
    sync_type       VARCHAR(20)  NOT NULL,   -- 同步类型: full/incremental
    ts_code         VARCHAR(12),             -- 股票代码（null 表示全市场）
    start_date      DATE,                    -- 拉取起始日期
    end_date        DATE,                    -- 拉取截止日期
    record_count    INTEGER      NOT NULL DEFAULT 0, -- 本次拉取记录数
    status          VARCHAR(20)  NOT NULL DEFAULT 'success', -- success/failed/partial
    error_msg       TEXT,                    -- 错误信息
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_ts_sync_log_api ON tushare_sync_log(api_name, sync_type);
CREATE INDEX IF NOT EXISTS ix_ts_sync_log_started ON tushare_sync_log(started_at);
COMMENT ON TABLE tushare_sync_log IS 'Tushare 数据同步日志';

-- ============================================================
-- 6. updated_at 自动更新触发器
-- ============================================================

CREATE OR REPLACE FUNCTION fn_tushare_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 为所有非分区表添加 updated_at 触发器
DO $$
DECLARE
    tbl TEXT;
BEGIN
    FOR tbl IN
        SELECT unnest(ARRAY[
            'tushare_stock_basic',
            'tushare_trade_cal',
            'tushare_stock_company',
            'tushare_namechange',
            'tushare_hs_const',
            'tushare_ipo_list',
            'tushare_income',
            'tushare_balancesheet',
            'tushare_cashflow',
            'tushare_forecast',
            'tushare_express',
            'tushare_dividend',
            'tushare_fina_indicator',
            'tushare_fina_audit',
            'tushare_fina_mainbz'
        ])
    LOOP
        EXECUTE format('
            DROP TRIGGER IF EXISTS trg_%s_updated_at ON %I;
            CREATE TRIGGER trg_%s_updated_at
                BEFORE UPDATE ON %I
                FOR EACH ROW
                EXECUTE FUNCTION fn_tushare_updated_at();
        ', tbl, tbl, tbl, tbl);
    END LOOP;
END $$;

COMMIT;

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

COMMENT ON COLUMN tushare_stock_basic.id IS '自增主键';
COMMENT ON COLUMN tushare_stock_basic.ts_code IS '股票代码（Tushare 格式，如 600519.SH）';
COMMENT ON COLUMN tushare_stock_basic.symbol IS '6位纯数字代码（如 600519）';
COMMENT ON COLUMN tushare_stock_basic.name IS '股票名称';
COMMENT ON COLUMN tushare_stock_basic.area IS '所在地区';
COMMENT ON COLUMN tushare_stock_basic.industry IS '所属行业';
COMMENT ON COLUMN tushare_stock_basic.cnspell IS '拼音缩写';
COMMENT ON COLUMN tushare_stock_basic.market IS '市场类型（主板/创业板/科创板/北交所）';
COMMENT ON COLUMN tushare_stock_basic.list_status IS '上市状态（L上市 D退市 P暂停）';
COMMENT ON COLUMN tushare_stock_basic.list_date IS '上市日期';
COMMENT ON COLUMN tushare_stock_basic.delist_date IS '退市日期';
COMMENT ON COLUMN tushare_stock_basic.is_hs IS '是否沪深港通标的（H沪股通 S深股通）';
COMMENT ON COLUMN tushare_stock_basic.act_name IS '实控人名称';
COMMENT ON COLUMN tushare_stock_basic.act_ent_type IS '实控人企业性质';
COMMENT ON COLUMN tushare_stock_basic.fullname IS '公司全称';
COMMENT ON COLUMN tushare_stock_basic.exchange IS '交易所代码（SSE上交所 SZSE深交所 BSE北交所）';
COMMENT ON COLUMN tushare_stock_basic.curr_type IS '交易货币';
COMMENT ON COLUMN tushare_stock_basic.enname IS '英文全称';
COMMENT ON COLUMN tushare_stock_basic.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_stock_basic.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_trade_cal.id IS '自增主键';
COMMENT ON COLUMN tushare_trade_cal.exchange IS '交易所代码（SSE上交所 SZSE深交所 CFFEX中金所 SHFE上期所等）';
COMMENT ON COLUMN tushare_trade_cal.cal_date IS '日历日期';
COMMENT ON COLUMN tushare_trade_cal.is_open IS '是否开市（1开市 0休市）';
COMMENT ON COLUMN tushare_trade_cal.pretrade_date IS '上一个交易日';
COMMENT ON COLUMN tushare_trade_cal.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_trade_cal.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_stock_company.id IS '自增主键';
COMMENT ON COLUMN tushare_stock_company.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_stock_company.com_name IS '公司名称';
COMMENT ON COLUMN tushare_stock_company.chairman IS '法人代表';
COMMENT ON COLUMN tushare_stock_company.manager IS '总经理';
COMMENT ON COLUMN tushare_stock_company.secretary IS '董秘';
COMMENT ON COLUMN tushare_stock_company.reg_capital IS '注册资本（万元）';
COMMENT ON COLUMN tushare_stock_company.setup_date IS '注册日期';
COMMENT ON COLUMN tushare_stock_company.province IS '所在省份';
COMMENT ON COLUMN tushare_stock_company.city IS '所在城市';
COMMENT ON COLUMN tushare_stock_company.introduction IS '公司介绍';
COMMENT ON COLUMN tushare_stock_company.website IS '公司主页';
COMMENT ON COLUMN tushare_stock_company.email IS '电子邮件';
COMMENT ON COLUMN tushare_stock_company.office IS '办公地址';
COMMENT ON COLUMN tushare_stock_company.employees IS '员工人数';
COMMENT ON COLUMN tushare_stock_company.main_business IS '主要业务及产品';
COMMENT ON COLUMN tushare_stock_company.business_scope IS '经营范围';
COMMENT ON COLUMN tushare_stock_company.phone IS '联系电话';
COMMENT ON COLUMN tushare_stock_company.fax IS '传真';
COMMENT ON COLUMN tushare_stock_company.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_stock_company.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_namechange.id IS '自增主键';
COMMENT ON COLUMN tushare_namechange.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_namechange.name IS '股票名称';
COMMENT ON COLUMN tushare_namechange.start_date IS '名称开始日期';
COMMENT ON COLUMN tushare_namechange.end_date IS '名称结束日期';
COMMENT ON COLUMN tushare_namechange.change_reason IS '变更原因';
COMMENT ON COLUMN tushare_namechange.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_namechange.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_hs_const.id IS '自增主键';
COMMENT ON COLUMN tushare_hs_const.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_hs_const.hs_type IS '沪深港通类型（SH沪股通 SZ深股通）';
COMMENT ON COLUMN tushare_hs_const.in_date IS '纳入日期';
COMMENT ON COLUMN tushare_hs_const.out_date IS '剔除日期';
COMMENT ON COLUMN tushare_hs_const.is_new IS '是否最新（1是 0否）';
COMMENT ON COLUMN tushare_hs_const.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_hs_const.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_ipo_list.id IS '自增主键';
COMMENT ON COLUMN tushare_ipo_list.ts_code IS '新股代码';
COMMENT ON COLUMN tushare_ipo_list.name IS '新股名称';
COMMENT ON COLUMN tushare_ipo_list.ipo_date IS '上市日期';
COMMENT ON COLUMN tushare_ipo_list.issue_date IS '申购日期';
COMMENT ON COLUMN tushare_ipo_list.amount IS '发行数量（万股）';
COMMENT ON COLUMN tushare_ipo_list.market IS '市场类型';
COMMENT ON COLUMN tushare_ipo_list.price IS '发行价格（元）';
COMMENT ON COLUMN tushare_ipo_list.pe IS '发行市盈率';
COMMENT ON COLUMN tushare_ipo_list.limit_amount IS '网上发行量（万股）';
COMMENT ON COLUMN tushare_ipo_list.funds IS '募集资金（万元）';
COMMENT ON COLUMN tushare_ipo_list.ballot IS '中签率';
COMMENT ON COLUMN tushare_ipo_list.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_ipo_list.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_daily.id IS '自增主键';
COMMENT ON COLUMN tushare_daily.ts_code IS '股票代码（如 600519.SH）';
COMMENT ON COLUMN tushare_daily.trade_date IS '交易日期（分区键）';
COMMENT ON COLUMN tushare_daily.open IS '开盘价';
COMMENT ON COLUMN tushare_daily.high IS '最高价';
COMMENT ON COLUMN tushare_daily.low IS '最低价';
COMMENT ON COLUMN tushare_daily.close IS '收盘价';
COMMENT ON COLUMN tushare_daily.pre_close IS '昨收价';
COMMENT ON COLUMN tushare_daily.change_val IS '涨跌额';
COMMENT ON COLUMN tushare_daily.pct_chg IS '涨跌幅（%）';
COMMENT ON COLUMN tushare_daily.vol IS '成交量（手）';
COMMENT ON COLUMN tushare_daily.amount IS '成交额（千元）';
COMMENT ON COLUMN tushare_daily.turnover_ratio IS '换手率（%）';
COMMENT ON COLUMN tushare_daily.volume_ratio IS '量比';
COMMENT ON COLUMN tushare_daily.amplitude IS '振幅（%）';
COMMENT ON COLUMN tushare_daily.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_daily.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_adj_factor.id IS '自增主键';
COMMENT ON COLUMN tushare_adj_factor.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_adj_factor.trade_date IS '交易日期（分区键）';
COMMENT ON COLUMN tushare_adj_factor.adj_factor IS '复权因子';
COMMENT ON COLUMN tushare_adj_factor.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_adj_factor.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_daily_basic.id IS '自增主键';
COMMENT ON COLUMN tushare_daily_basic.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_daily_basic.trade_date IS '交易日期（分区键）';
COMMENT ON COLUMN tushare_daily_basic.close IS '当日收盘价';
COMMENT ON COLUMN tushare_daily_basic.turnover_rate IS '换手率（%）';
COMMENT ON COLUMN tushare_daily_basic.turnover_rate_f IS '换手率（自由流通股，%）';
COMMENT ON COLUMN tushare_daily_basic.volume_ratio IS '量比';
COMMENT ON COLUMN tushare_daily_basic.pe IS '市盈率';
COMMENT ON COLUMN tushare_daily_basic.pe_ttm IS '市盈率（TTM）';
COMMENT ON COLUMN tushare_daily_basic.pb IS '市净率';
COMMENT ON COLUMN tushare_daily_basic.ps IS '市销率';
COMMENT ON COLUMN tushare_daily_basic.ps_ttm IS '市销率（TTM）';
COMMENT ON COLUMN tushare_daily_basic.dv_ratio IS '股息率（%）';
COMMENT ON COLUMN tushare_daily_basic.dv_ttm IS '股息率（TTM）';
COMMENT ON COLUMN tushare_daily_basic.total_share IS '总股本（万股）';
COMMENT ON COLUMN tushare_daily_basic.float_share IS '流通股本（万股）';
COMMENT ON COLUMN tushare_daily_basic.free_share IS '自由流通股本（万股）';
COMMENT ON COLUMN tushare_daily_basic.total_mv IS '总市值（万元）';
COMMENT ON COLUMN tushare_daily_basic.circ_mv IS '流通市值（万元）';
COMMENT ON COLUMN tushare_daily_basic.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_daily_basic.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_income.id IS '自增主键';
COMMENT ON COLUMN tushare_income.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_income.ann_date IS '公告日期';
COMMENT ON COLUMN tushare_income.f_ann_date IS '实际公告日期';
COMMENT ON COLUMN tushare_income.end_date IS '报告期截止日';
COMMENT ON COLUMN tushare_income.report_type IS '报告类型（Q1/Q2半/Q3/年报）';
COMMENT ON COLUMN tushare_income.comp_type IS '公司类型（1一般工商业 2银行 3保险 4证券）';
COMMENT ON COLUMN tushare_income.end_type IS '报告期类型（1合并 2母公司）';
COMMENT ON COLUMN tushare_income.total_revenue IS '营业总收入';
COMMENT ON COLUMN tushare_income.revenue IS '营业收入';
COMMENT ON COLUMN tushare_income.int_income IS '利息收入';
COMMENT ON COLUMN tushare_income.prem_earned IS '已赚保费';
COMMENT ON COLUMN tushare_income.comm_income IS '手续费及佣金收入';
COMMENT ON COLUMN tushare_income.total_cogs IS '营业总成本';
COMMENT ON COLUMN tushare_income.oper_cost IS '营业成本';
COMMENT ON COLUMN tushare_income.sell_exp IS '销售费用';
COMMENT ON COLUMN tushare_income.admin_exp IS '管理费用';
COMMENT ON COLUMN tushare_income.fin_exp IS '财务费用';
COMMENT ON COLUMN tushare_income.assets_impair_loss IS '资产减值损失';
COMMENT ON COLUMN tushare_income.fair_value_inter_gain IS '公允价值变动收益';
COMMENT ON COLUMN tushare_income.invest_income IS '投资收益';
COMMENT ON COLUMN tushare_income.oper_profit IS '营业利润';
COMMENT ON COLUMN tushare_income.non_oper_income IS '营业外收入';
COMMENT ON COLUMN tushare_income.non_oper_exp IS '营业外支出';
COMMENT ON COLUMN tushare_income.total_profit IS '利润总额';
COMMENT ON COLUMN tushare_income.income_tax IS '所得税费用';
COMMENT ON COLUMN tushare_income.n_income IS '净利润';
COMMENT ON COLUMN tushare_income.n_income_attr_p IS '归母净利润';
COMMENT ON COLUMN tushare_income.minority_gain IS '少数股东损益';
COMMENT ON COLUMN tushare_income.basic_eps IS '基本每股收益';
COMMENT ON COLUMN tushare_income.diluted_eps IS '稀释每股收益';
COMMENT ON COLUMN tushare_income.extra_fields IS 'Tushare 返回的扩展字段（JSONB）';
COMMENT ON COLUMN tushare_income.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_income.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_balancesheet.id IS '自增主键';
COMMENT ON COLUMN tushare_balancesheet.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_balancesheet.ann_date IS '公告日期';
COMMENT ON COLUMN tushare_balancesheet.f_ann_date IS '实际公告日期';
COMMENT ON COLUMN tushare_balancesheet.end_date IS '报告期截止日';
COMMENT ON COLUMN tushare_balancesheet.report_type IS '报告类型（Q1/Q2半/Q3/年报）';
COMMENT ON COLUMN tushare_balancesheet.comp_type IS '公司类型（1一般工商业 2银行 3保险 4证券）';
COMMENT ON COLUMN tushare_balancesheet.end_type IS '报告期类型（1合并 2母公司）';
COMMENT ON COLUMN tushare_balancesheet.total_assets IS '资产总计';
COMMENT ON COLUMN tushare_balancesheet.total_cur_assets IS '流动资产合计';
COMMENT ON COLUMN tushare_balancesheet.money_cap IS '货币资金';
COMMENT ON COLUMN tushare_balancesheet.trad_asset IS '交易性金融资产';
COMMENT ON COLUMN tushare_balancesheet.notes_receiv IS '应收票据';
COMMENT ON COLUMN tushare_balancesheet.accounts_receiv IS '应收账款';
COMMENT ON COLUMN tushare_balancesheet.prepayment IS '预付款项';
COMMENT ON COLUMN tushare_balancesheet.inventories IS '存货';
COMMENT ON COLUMN tushare_balancesheet.total_non_cur_assets IS '非流动资产合计';
COMMENT ON COLUMN tushare_balancesheet.fix_assets IS '固定资产';
COMMENT ON COLUMN tushare_balancesheet.constru_in_process IS '在建工程';
COMMENT ON COLUMN tushare_balancesheet.intangible_assets IS '无形资产';
COMMENT ON COLUMN tushare_balancesheet.goodwill IS '商誉';
COMMENT ON COLUMN tushare_balancesheet.total_liab IS '负债合计';
COMMENT ON COLUMN tushare_balancesheet.total_cur_liab IS '流动负债合计';
COMMENT ON COLUMN tushare_balancesheet.short_borrow IS '短期借款';
COMMENT ON COLUMN tushare_balancesheet.notes_payable IS '应付票据';
COMMENT ON COLUMN tushare_balancesheet.accounts_payable IS '应付账款';
COMMENT ON COLUMN tushare_balancesheet.total_non_cur_liab IS '非流动负债合计';
COMMENT ON COLUMN tushare_balancesheet.long_borrow IS '长期借款';
COMMENT ON COLUMN tushare_balancesheet.total_hldr_eqy_exc_min IS '股东权益合计（不含少数股东权益）';
COMMENT ON COLUMN tushare_balancesheet.minority_int IS '少数股东权益';
COMMENT ON COLUMN tushare_balancesheet.total_hldr_eqy_inc_min IS '股东权益合计（含少数股东权益）';
COMMENT ON COLUMN tushare_balancesheet.extra_fields IS 'Tushare 返回的扩展字段（JSONB）';
COMMENT ON COLUMN tushare_balancesheet.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_balancesheet.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_cashflow.id IS '自增主键';
COMMENT ON COLUMN tushare_cashflow.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_cashflow.ann_date IS '公告日期';
COMMENT ON COLUMN tushare_cashflow.f_ann_date IS '实际公告日期';
COMMENT ON COLUMN tushare_cashflow.end_date IS '报告期截止日';
COMMENT ON COLUMN tushare_cashflow.report_type IS '报告类型（Q1/Q2半/Q3/年报）';
COMMENT ON COLUMN tushare_cashflow.comp_type IS '公司类型（1一般工商业 2银行 3保险 4证券）';
COMMENT ON COLUMN tushare_cashflow.end_type IS '报告期类型（1合并 2母公司）';
COMMENT ON COLUMN tushare_cashflow.c_fr_sale_sg IS '销售商品、提供劳务收到的现金';
COMMENT ON COLUMN tushare_cashflow.net_cf_oper_act IS '经营活动产生的现金流量净额';
COMMENT ON COLUMN tushare_cashflow.net_cf_inv_act IS '投资活动产生的现金流量净额';
COMMENT ON COLUMN tushare_cashflow.net_cf_fin_act IS '筹资活动产生的现金流量净额';
COMMENT ON COLUMN tushare_cashflow.free_cf IS '企业自由现金流';
COMMENT ON COLUMN tushare_cashflow.st_cash_out_act IS '经营活动现金流出小计';
COMMENT ON COLUMN tushare_cashflow.st_cash_in_act IS '经营活动现金流入小计';
COMMENT ON COLUMN tushare_cashflow.st_cash_out_inv IS '投资活动现金流出小计';
COMMENT ON COLUMN tushare_cashflow.st_cash_in_inv IS '投资活动现金流入小计';
COMMENT ON COLUMN tushare_cashflow.st_cash_out_fin IS '筹资活动现金流出小计';
COMMENT ON COLUMN tushare_cashflow.st_cash_in_fin IS '筹资活动现金流入小计';
COMMENT ON COLUMN tushare_cashflow.n_cashflow_act IS '汇率变动对现金的影响';
COMMENT ON COLUMN tushare_cashflow.c_change IS '现金及现金等价物净增加额';
COMMENT ON COLUMN tushare_cashflow.c_bal_end IS '期末现金及现金等价物余额';
COMMENT ON COLUMN tushare_cashflow.extra_fields IS 'Tushare 返回的扩展字段（JSONB）';
COMMENT ON COLUMN tushare_cashflow.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_cashflow.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_forecast.id IS '自增主键';
COMMENT ON COLUMN tushare_forecast.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_forecast.ann_date IS '公告日期';
COMMENT ON COLUMN tushare_forecast.end_date IS '报告期';
COMMENT ON COLUMN tushare_forecast.type IS '预告类型（预增/预减/扭亏/首亏/续盈/略增/略减/不确定）';
COMMENT ON COLUMN tushare_forecast.p_change_min IS '净利润变动下限（%）';
COMMENT ON COLUMN tushare_forecast.p_change_max IS '净利润变动上限（%）';
COMMENT ON COLUMN tushare_forecast.net_profit_min IS '净利润下限（万元）';
COMMENT ON COLUMN tushare_forecast.net_profit_max IS '净利润上限（万元）';
COMMENT ON COLUMN tushare_forecast.last_parent_net IS '上年同期归母净利润';
COMMENT ON COLUMN tushare_forecast.notice_date IS '公告日期';
COMMENT ON COLUMN tushare_forecast.notice_reason IS '业绩变动原因';
COMMENT ON COLUMN tushare_forecast.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_forecast.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_express.id IS '自增主键';
COMMENT ON COLUMN tushare_express.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_express.ann_date IS '公告日期';
COMMENT ON COLUMN tushare_express.end_date IS '报告期';
COMMENT ON COLUMN tushare_express.revenue IS '营业收入（元）';
COMMENT ON COLUMN tushare_express.operate_profit IS '营业利润（元）';
COMMENT ON COLUMN tushare_express.total_profit IS '利润总额（元）';
COMMENT ON COLUMN tushare_express.n_income IS '净利润（元）';
COMMENT ON COLUMN tushare_express.total_assets IS '总资产（元）';
COMMENT ON COLUMN tushare_express.total_hldr_eqy IS '股东权益合计（不含少数股东权益）';
COMMENT ON COLUMN tushare_express.diluted_eps IS '稀释每股收益';
COMMENT ON COLUMN tushare_express.weighted_roe IS '加权平均净资产收益率';
COMMENT ON COLUMN tushare_express.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_express.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_dividend.id IS '自增主键';
COMMENT ON COLUMN tushare_dividend.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_dividend.end_date IS '分红年度';
COMMENT ON COLUMN tushare_dividend.ann_date IS '预案公告日';
COMMENT ON COLUMN tushare_dividend.div_proc IS '方案进度（预案/决案/实施）';
COMMENT ON COLUMN tushare_dividend.stk_div IS '每股送转（股）';
COMMENT ON COLUMN tushare_dividend.stk_bo_rate IS '每股送股';
COMMENT ON COLUMN tushare_dividend.stk_co_rate IS '每股转增';
COMMENT ON COLUMN tushare_dividend.cash_div IS '每股派息（税前，元）';
COMMENT ON COLUMN tushare_dividend.cash_div_tax IS '每股派息（税后，元）';
COMMENT ON COLUMN tushare_dividend.record_date IS '股权登记日';
COMMENT ON COLUMN tushare_dividend.ex_date IS '除权除息日';
COMMENT ON COLUMN tushare_dividend.pay_date IS '派息日';
COMMENT ON COLUMN tushare_dividend.div_listdate IS '红股上市日';
COMMENT ON COLUMN tushare_dividend.imp_ann_date IS '实施公告日';
COMMENT ON COLUMN tushare_dividend.base_date IS '基准日';
COMMENT ON COLUMN tushare_dividend.base_share IS '实施基准股本（万股）';
COMMENT ON COLUMN tushare_dividend.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_dividend.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_fina_indicator.id IS '自增主键';
COMMENT ON COLUMN tushare_fina_indicator.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_fina_indicator.ann_date IS '公告日期';
COMMENT ON COLUMN tushare_fina_indicator.end_date IS '报告期';
COMMENT ON COLUMN tushare_fina_indicator.eps IS '基本每股收益';
COMMENT ON COLUMN tushare_fina_indicator.dt_eps IS '稀释每股收益';
COMMENT ON COLUMN tushare_fina_indicator.total_revenue_ps IS '每股营业收入';
COMMENT ON COLUMN tushare_fina_indicator.revenue_ps IS '每股营业收入（TTM）';
COMMENT ON COLUMN tushare_fina_indicator.capital_rese_ps IS '每股资本公积';
COMMENT ON COLUMN tushare_fina_indicator.surplus_rese_ps IS '每股盈余公积';
COMMENT ON COLUMN tushare_fina_indicator.undist_profit_ps IS '每股未分配利润';
COMMENT ON COLUMN tushare_fina_indicator.grossprofit_margin IS '销售毛利率（%）';
COMMENT ON COLUMN tushare_fina_indicator.netprofit_margin IS '销售净利率（%）';
COMMENT ON COLUMN tushare_fina_indicator.roe IS '净资产收益率（%）';
COMMENT ON COLUMN tushare_fina_indicator.roe_dt IS '净资产收益率（摊薄，%）';
COMMENT ON COLUMN tushare_fina_indicator.roa IS '总资产净利率（%）';
COMMENT ON COLUMN tushare_fina_indicator.roa_yearly IS '年化总资产净利率（%）';
COMMENT ON COLUMN tushare_fina_indicator.roic IS '投入资本回报率（%）';
COMMENT ON COLUMN tushare_fina_indicator.or_yoy IS '营业收入同比增长率（%）';
COMMENT ON COLUMN tushare_fina_indicator.op_yoy IS '营业利润同比增长率（%）';
COMMENT ON COLUMN tushare_fina_indicator.profit_yoy IS '归属净利润同比增长率（%）';
COMMENT ON COLUMN tushare_fina_indicator.equity_yoy IS '净资产同比增长率（%）';
COMMENT ON COLUMN tushare_fina_indicator.assets_yoy IS '总资产同比增长率（%）';
COMMENT ON COLUMN tushare_fina_indicator.debt_to_assets IS '资产负债率（%）';
COMMENT ON COLUMN tushare_fina_indicator.current_ratio IS '流动比率';
COMMENT ON COLUMN tushare_fina_indicator.quick_ratio IS '速动比率';
COMMENT ON COLUMN tushare_fina_indicator.equity_ratio IS '产权比率';
COMMENT ON COLUMN tushare_fina_indicator.inv_turn IS '存货周转率（次）';
COMMENT ON COLUMN tushare_fina_indicator.ar_turn IS '应收账款周转率（次）';
COMMENT ON COLUMN tushare_fina_indicator.assets_turn IS '总资产周转率（次）';
COMMENT ON COLUMN tushare_fina_indicator.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_fina_indicator.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_fina_audit.id IS '自增主键';
COMMENT ON COLUMN tushare_fina_audit.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_fina_audit.ann_date IS '公告日期';
COMMENT ON COLUMN tushare_fina_audit.end_date IS '报告期';
COMMENT ON COLUMN tushare_fina_audit.audit_result IS '审计结果（标准无保留意见/保留意见/否定意见/无法表示意见）';
COMMENT ON COLUMN tushare_fina_audit.audit_fees IS '审计费用（元）';
COMMENT ON COLUMN tushare_fina_audit.audit_agency IS '审计机构';
COMMENT ON COLUMN tushare_fina_audit.audit_sign IS '签字会计师';
COMMENT ON COLUMN tushare_fina_audit.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_fina_audit.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_fina_mainbz.id IS '自增主键';
COMMENT ON COLUMN tushare_fina_mainbz.ts_code IS '股票代码';
COMMENT ON COLUMN tushare_fina_mainbz.end_date IS '报告期';
COMMENT ON COLUMN tushare_fina_mainbz.bz_item IS '主营业务项目';
COMMENT ON COLUMN tushare_fina_mainbz.bz_code IS '项目代码';
COMMENT ON COLUMN tushare_fina_mainbz.bz_sales IS '主营业务收入（元）';
COMMENT ON COLUMN tushare_fina_mainbz.bz_profit IS '主营业务利润（元）';
COMMENT ON COLUMN tushare_fina_mainbz.bz_cost IS '主营业务成本（元）';
COMMENT ON COLUMN tushare_fina_mainbz.curr_type IS '货币代码';
COMMENT ON COLUMN tushare_fina_mainbz.update_date IS '更新日期';
COMMENT ON COLUMN tushare_fina_mainbz.created_at IS '记录创建时间';
COMMENT ON COLUMN tushare_fina_mainbz.updated_at IS '记录最近更新时间';

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

COMMENT ON COLUMN tushare_sync_log.id IS '自增主键';
COMMENT ON COLUMN tushare_sync_log.api_name IS 'API 名称（如 daily、income）';
COMMENT ON COLUMN tushare_sync_log.sync_type IS '同步类型（full 全量 / incremental 增量）';
COMMENT ON COLUMN tushare_sync_log.ts_code IS '股票代码（null 表示全市场）';
COMMENT ON COLUMN tushare_sync_log.start_date IS '拉取起始日期';
COMMENT ON COLUMN tushare_sync_log.end_date IS '拉取截止日期';
COMMENT ON COLUMN tushare_sync_log.record_count IS '本次拉取记录数';
COMMENT ON COLUMN tushare_sync_log.status IS '同步状态（success/failed/partial）';
COMMENT ON COLUMN tushare_sync_log.error_msg IS '错误信息';
COMMENT ON COLUMN tushare_sync_log.started_at IS '同步开始时间';
COMMENT ON COLUMN tushare_sync_log.finished_at IS '同步结束时间';
COMMENT ON COLUMN tushare_sync_log.created_at IS '记录创建时间';

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

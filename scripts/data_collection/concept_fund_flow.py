import akshare as ak
import psycopg2
from psycopg2 import sql
import time
import logging
import os
from psycopg2.extras import execute_values
from datetime import datetime
import random
import pandas as pd

# 配置日志（保持与原项目日志目录一致）
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'result')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'concept_fund_flow.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # 控制台输出
        logging.FileHandler(log_file, encoding='utf-8')  # 日志文件（支持中文）
    ]
)
logger = logging.getLogger(__name__)

# PostgreSQL 数据库连接配置（复用原项目配置，支持环境变量注入）
postgresql_config = {
    'host': os.environ.get('PG_HOST', '127.0.0.1'),
    'user': os.environ.get('PG_USER', 'root'),
    'password': os.environ.get('PG_PASSWORD', '123629He'),
    'database': os.environ.get('PG_DATABASE', 'akshare'),
    'port': os.environ.get('PG_PORT', '5431')
}


def create_concept_flow_table(client):
    """
    创建概念板块资金流表（若不存在），并添加索引和唯一约束
    Args:
        client: 数据库游标对象
    """
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS concept_fund_flow (
            id SERIAL PRIMARY KEY,
            concept_name VARCHAR(100) NOT NULL,  -- 概念名称（对应接口的“行业”字段）
            concept_index NUMERIC(10, 3),        -- 概念指数
            concept_pct_chg NUMERIC(10, 2),      -- 概念涨跌幅（%，即时数据专用）
            inflow NUMERIC(18, 2),               -- 流入资金（单位：亿）
            outflow NUMERIC(18, 2),              -- 流出资金（单位：亿）
            net_flow NUMERIC(18, 2),             -- 资金净额（单位：亿）
            company_count NUMERIC(10, 0),        -- 概念包含公司家数
            leading_stock VARCHAR(100),          -- 领涨股名称（即时数据专用）
            leading_stock_pct_chg NUMERIC(10, 2),-- 领涨股涨跌幅（%，即时数据专用）
            leading_stock_price NUMERIC(10, 2),  -- 领涨股当前价（元，即时数据专用）
            period_pct_chg NUMERIC(10, 2),       -- 阶段涨跌幅（%，3/5/10/20日数据专用）
            flow_type VARCHAR(20) NOT NULL,      -- 数据类型：即时/3日排行/5日排行/10日排行/20日排行
            record_date DATE NOT NULL,           -- 数据记录日期（按获取当天算）
            insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 数据入库时间
            -- 唯一约束：同一概念+同一数据类型+同一日期 不重复
            UNIQUE (concept_name, flow_type, record_date)
        )
    """
    try:
        # 执行建表语句
        client.execute(create_table_sql)
        # 创建索引（提升查询效率）
        index_sqls = [
            "CREATE INDEX IF NOT EXISTS idx_concept_flow_name ON concept_fund_flow (concept_name)",
            "CREATE INDEX IF NOT EXISTS idx_concept_flow_type ON concept_fund_flow (flow_type)",
            "CREATE INDEX IF NOT EXISTS idx_concept_flow_date ON concept_fund_flow (record_date)"
        ]
        for idx_sql in index_sqls:
            client.execute(idx_sql)
        logger.info("概念板块资金流表及索引创建成功")
    except Exception as e:
        logger.error(f"创建概念板块资金流表失败：{str(e)}")
        raise


def add_concept_flow_column_comments(client):
    """
    为概念板块资金流表添加字段注释（提升表可读性）
    Args:
        client: 数据库游标对象
    """
    column_comments = [
        ("concept_name", "概念名称（对应同花顺接口的“行业”字段）"),
        ("concept_index", "概念指数"),
        ("concept_pct_chg", "概念涨跌幅（单位：%，仅即时数据有值）"),
        ("inflow", "流入资金（单位：亿）"),
        ("outflow", "流出资金（单位：亿）"),
        ("net_flow", "资金净额（单位：亿）"),
        ("company_count", "概念包含的公司家数"),
        ("leading_stock", "领涨股名称（仅即时数据有值）"),
        ("leading_stock_pct_chg", "领涨股涨跌幅（单位：%，仅即时数据有值）"),
        ("leading_stock_price", "领涨股当前价（单位：元，仅即时数据有值）"),
        ("period_pct_chg", "阶段涨跌幅（单位：%，仅3/5/10/20日数据有值）"),
        ("flow_type", "数据类型（可选值：即时、3日排行、5日排行、10日排行、20日排行）"),
        ("record_date", "数据记录日期（按数据获取当天计算）"),
        ("insert_time", "数据入库时间（自动生成，无需手动插入）")
    ]
    try:
        for column, comment in column_comments:
            # 用sql.Identifier避免字段名注入风险
            comment_sql = sql.SQL("COMMENT ON COLUMN concept_fund_flow.{} IS %s").format(
                sql.Identifier(column)
            )
            client.execute(comment_sql, [comment])
        logger.info("概念板块资金流表字段注释添加成功")
    except Exception as e:
        logger.error(f"添加概念板块资金流表字段注释失败：{str(e)}")
        raise


def batch_insert_concept_flow_data(client, data_list):
    """
    批量插入概念板块资金流数据（支持冲突跳过）
    Args:
        client: 数据库游标对象
        data_list: 待插入数据列表（每个元素为元组，对应表字段顺序）
    """
    if not data_list:
        logger.warning("无待插入的概念板块资金流数据")
        return
    # 插入SQL（字段顺序与data_list元组顺序一致）
    insert_sql = sql.SQL("""
        INSERT INTO concept_fund_flow (
            concept_name, concept_index, concept_pct_chg, inflow, outflow, net_flow,
            company_count, leading_stock, leading_stock_pct_chg, leading_stock_price,
            period_pct_chg, flow_type, record_date
        ) VALUES %s ON CONFLICT (concept_name, flow_type, record_date) DO NOTHING
    """)
    try:
        # 用execute_values实现批量插入（效率高于循环单条插入）
        execute_values(client, insert_sql, data_list)
        logger.info(f"成功批量插入 {len(data_list)} 条概念板块资金流数据")
    except Exception as e:
        logger.error(f"批量插入概念板块资金流数据失败：{str(e)}")
        raise


def handle_api_rate_limit(last_req_time, req_count):
    """
    处理API请求频率限制（避免触发akshare接口限制）
    Args:
        last_req_time: 上次请求时间戳
        req_count: 当前分钟内请求次数
    Returns:
        tuple: (更新后的上次请求时间戳, 更新后的请求次数)
    """
    current_time = time.time()
    # 随机延迟1.5-2.5秒（避免固定延迟导致的请求峰值）
    random_delay = random.uniform(1.5, 2.5)
    time.sleep(random_delay)
    # 更新请求计数和时间戳
    return current_time, req_count + 1


def fetch_concept_flow_data(flow_type, last_req_time, req_count):
    """
    调用akshare接口获取指定类型的概念板块资金流数据
    Args:
        flow_type: 数据类型（即时/3日排行/5日排行/10日排行/20日排行）
        last_req_time: 上次请求时间戳
        req_count: 当前分钟内请求次数
    Returns:
        tuple: (数据DataFrame, 更新后的上次请求时间戳, 更新后的请求次数)
    """
    try:
        # 先处理频率限制
        last_req_time, req_count = handle_api_rate_limit(last_req_time, req_count)
        logger.info(f"开始获取「{flow_type}」类型的概念板块资金流数据")
        # 调用akshare接口
        df = ak.stock_fund_flow_concept(symbol=flow_type)
        if df.empty:
            logger.warning(f"「{flow_type}」类型的概念板块资金流数据为空")
            return None, last_req_time, req_count
        logger.info(f"成功获取「{flow_type}」类型的概念板块资金流数据，共 {len(df)} 条")
        return df, last_req_time, req_count
    except Exception as e:
        logger.error(f"获取「{flow_type}」类型的概念板块资金流数据失败：{str(e)}")
        # 即使失败也计数（避免无限重试）
        return None, last_req_time, req_count + 1


def process_concept_flow_data(df, flow_type):
    """
    处理概念板块资金流数据（格式转换、字段映射）
    Args:
        df: 原始数据DataFrame
        flow_type: 数据类型（即时/3日排行/5日排行/10日排行/20日排行）
    Returns:
        list: 处理后的数据列表（每个元素为元组，用于入库）
    """
    if df is None or df.empty:
        return []
    
    processed_data = []
    # 数据记录日期（按获取当天算，格式：YYYY-MM-DD）
    record_date = datetime.now().date()
    
    if flow_type == "即时":
        # 处理即时数据（字段：行业、行业指数、行业-涨跌幅、流入资金、流出资金、净额、公司家数、领涨股、领涨股-涨跌幅、当前价）
        for _, row in df.iterrows():
            # 处理空值（用None填充）
            concept_name = row["行业"] if pd.notna(row["行业"]) else None
            concept_index = round(row["行业指数"], 3) if pd.notna(row["行业指数"]) else None
            concept_pct_chg = round(row["行业-涨跌幅"], 2) if pd.notna(row["行业-涨跌幅"]) else None
            inflow = round(row["流入资金"], 2) if pd.notna(row["流入资金"]) else None
            outflow = round(row["流出资金"], 2) if pd.notna(row["流出资金"]) else None
            net_flow = round(row["净额"], 2) if pd.notna(row["净额"]) else None
            company_count = int(row["公司家数"]) if pd.notna(row["公司家数"]) else None
            leading_stock = row["领涨股"] if pd.notna(row["领涨股"]) else None
            leading_stock_pct_chg = round(row["领涨股-涨跌幅"], 2) if pd.notna(row["领涨股-涨跌幅"]) else None
            leading_stock_price = round(row["当前价"], 2) if pd.notna(row["当前价"]) else None
            period_pct_chg = None  # 即时数据无阶段涨跌幅
            
            # 组装数据元组（与入库字段顺序一致）
            data_tuple = (
                concept_name, concept_index, concept_pct_chg, inflow, outflow, net_flow,
                company_count, leading_stock, leading_stock_pct_chg, leading_stock_price,
                period_pct_chg, flow_type, record_date
            )
            processed_data.append(data_tuple)
    else:
        # 处理3/5/10/20日数据（字段：行业、公司家数、行业指数、阶段涨跌幅、流入资金、流出资金、净额）
        for _, row in df.iterrows():
            # 处理阶段涨跌幅（原始数据带%符号，需去除并转数值）
            period_pct_str = row["阶段涨跌幅"] if pd.notna(row["阶段涨跌幅"]) else None
            period_pct_chg = None
            if period_pct_str:
                try:
                    period_pct_chg = round(float(str(period_pct_str).replace("%", "")), 2)
                except ValueError:
                    logger.warning(f"阶段涨跌幅格式异常：{period_pct_str}，跳过该值")
            
            # 处理其他字段
            concept_name = row["行业"] if pd.notna(row["行业"]) else None
            concept_index = round(row["行业指数"], 3) if pd.notna(row["行业指数"]) else None
            inflow = round(row["流入资金"], 2) if pd.notna(row["流入资金"]) else None
            outflow = round(row["流出资金"], 2) if pd.notna(row["流出资金"]) else None
            net_flow = round(row["净额"], 2) if pd.notna(row["净额"]) else None
            company_count = int(row["公司家数"]) if pd.notna(row["公司家数"]) else None
            # 阶段数据无领涨股相关字段，用None填充
            leading_stock = None
            leading_stock_pct_chg = None
            leading_stock_price = None
            concept_pct_chg = None  # 阶段数据无即时涨跌幅
            
            # 组装数据元组（与入库字段顺序一致）
            data_tuple = (
                concept_name, concept_index, concept_pct_chg, inflow, outflow, net_flow,
                company_count, leading_stock, leading_stock_pct_chg, leading_stock_price,
                period_pct_chg, flow_type, record_date
            )
            processed_data.append(data_tuple)
    
    return processed_data


def main_concept_flow_fetch():
    """
    主函数：获取所有类型的概念板块资金流数据并入库
    """
    # 待获取的数据类型列表
    flow_types = ["即时", "3日排行", "5日排行", "10日排行", "20日排行"]
    # 初始化请求计数器和时间戳
    req_count = 0
    last_req_time = time.time()
    # 初始化入库计数器
    total_insert_count = 0
    
    try:
        # 连接数据库（上下文管理器自动管理连接关闭）
        with psycopg2.connect(**postgresql_config) as conn:
            # 创建游标（上下文管理器自动管理游标关闭）
            with conn.cursor() as client:
                # 1. 初始化表结构和字段注释
                create_concept_flow_table(client)
                add_concept_flow_column_comments(client)
                conn.commit()
                logger.info("概念板块资金流表结构初始化完成")
                
                # 2. 遍历所有数据类型，获取并入库
                for flow_type in flow_types:
                    try:
                        # 2.1 获取原始数据
                        df, last_req_time, req_count = fetch_concept_flow_data(
                            flow_type=flow_type,
                            last_req_time=last_req_time,
                            req_count=req_count
                        )
                        if df is None:
                            logger.warning(f"「{flow_type}」类型数据获取失败或为空，跳过入库")
                            continue
                        
                        # 2.2 处理数据
                        processed_data = process_concept_flow_data(df, flow_type)
                        if not processed_data:
                            logger.warning(f"「{flow_type}」类型数据处理后为空，跳过入库")
                            continue
                        
                        # 2.3 批量插入数据
                        batch_insert_concept_flow_data(client, processed_data)
                        total_insert_count += len(processed_data)
                        
                        # 定期提交事务（避免事务过大）
                        if total_insert_count >= 1000:
                            conn.commit()
                            logger.info(f"累计提交 {total_insert_count} 条概念板块资金流数据")
                            
                    except Exception as e:
                        # 单类型数据处理失败，回滚当前事务，不影响其他类型
                        conn.rollback()
                        logger.error(f"处理「{flow_type}」类型数据时异常，已回滚：{str(e)}")
                        continue
                
                # 3. 提交剩余未提交的数据
                if total_insert_count > 0:
                    conn.commit()
                    logger.info(f"所有类型数据处理完成，累计入库 {total_insert_count} 条概念板块资金流数据")
                else:
                    logger.info("所有类型数据处理完成，无新数据入库")
    
    except psycopg2.Error as e:
        logger.error(f"数据库连接或操作异常：{str(e)}")
    except Exception as e:
        logger.error(f"程序主流程异常：{str(e)}")


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("开始执行概念板块资金流数据获取程序")
    logger.info("=" * 50)
    main_concept_flow_fetch()
    logger.info("=" * 50)
    logger.info("概念板块资金流数据获取程序执行结束")
    logger.info("=" * 50)
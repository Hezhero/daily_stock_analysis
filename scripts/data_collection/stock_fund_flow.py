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

# 配置日志
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'result')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'stock_fund_flow.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file)
    ]
)
logger = logging.getLogger(__name__)

# PostgreSQL 数据库连接配置
postgresql_config = {
    'host': os.environ.get('PG_HOST', '127.0.0.1'),
    'user': os.environ.get('PG_USER', 'root'),
    'password': os.environ.get('PG_PASSWORD', '123629He'),
    'database': os.environ.get('PG_DATABASE', 'akshare'),
    'port': os.environ.get('PG_PORT', '5431')
}


def create_table(client):
    """创建表（如果不存在）并添加必要的索引和约束"""
    # 创建表
    create_table_query = """
                     CREATE TABLE IF NOT EXISTS stock_fund_flow (
                         id SERIAL PRIMARY KEY,
                         code VARCHAR(20) NOT NULL,
                         name VARCHAR(100) NOT NULL,
                         latest_price NUMERIC(10, 2),
                         pct_chg NUMERIC(10, 2),
                         turnover_rate NUMERIC(10, 2),
                         inflow NUMERIC(18, 2),
                         outflow NUMERIC(18, 2),
                         net_flow NUMERIC(18, 2),
                         amount NUMERIC(18, 2),
                         period_pct_chg NUMERIC(10, 2),
                         consecutive_turnover NUMERIC(10, 2),
                         flow_type VARCHAR(20) NOT NULL, -- 区分数据类型：即时, 3日排行, 5日排行, 10日排行, 20日排行
                         record_date DATE NOT NULL, -- 记录日期
                         insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         UNIQUE (code, flow_type, record_date)  -- 唯一约束
                     )
                     """
    client.execute(create_table_query)

    # 创建索引
    try:
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_stock_fund_flow_code ON stock_fund_flow (code);
                       """)
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_stock_fund_flow_type ON stock_fund_flow (flow_type);
                       """)
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_stock_fund_flow_date ON stock_fund_flow (record_date);
                       """)
        logger.info("表结构和索引创建成功")
    except Exception as e:
        logger.error(f"创建索引时出错：{e}")


def add_column_comments(client):
    """添加列注释"""
    column_comments = [
        ("code", "股票代码"),
        ("name", "股票名称"),
        ("latest_price", "最新价"),
        ("pct_chg", "涨跌幅（%）"),
        ("turnover_rate", "换手率（%）"),
        ("inflow", "流入资金（元）"),
        ("outflow", "流出资金（元）"),
        ("net_flow", "净额（元）"),
        ("amount", "成交额（元）"),
        ("period_pct_chg", "阶段涨跌幅（%）"),
        ("consecutive_turnover", "连续换手率（%）"),
        ("flow_type", "数据类型（即时, 3日排行, 5日排行, 10日排行, 20日排行）"),
        ("record_date", "记录日期"),
        ("insert_time", "数据插入时间")
    ]
    try:
        for column, comment in column_comments:
            comment_query = sql.SQL("COMMENT ON COLUMN stock_fund_flow.{} IS %s").format(sql.Identifier(column))
            client.execute(comment_query, [comment])
        logger.info("列注释添加成功")
    except Exception as e:
        logger.error(f"添加列注释时出错: {e}")


def insert_data(client, data_to_insert):
    """批量插入数据"""
    try:
        insert_query = sql.SQL("""
                               INSERT INTO stock_fund_flow 
                               (code, name, latest_price, pct_chg, turnover_rate, inflow, outflow, net_flow, 
                                amount, period_pct_chg, consecutive_turnover, flow_type, record_date)
                               VALUES %s ON CONFLICT (code, flow_type, record_date) DO NOTHING
                               """)
        execute_values(client, insert_query, data_to_insert)
    except Exception as e:
        logger.error(f"插入数据时出错: {e}")
        raise


def handle_rate_limit(last_request_time, request_count):
    """处理API请求频率限制"""
    current_time = time.time()
    time_diff = current_time - last_request_time

    # 添加随机延迟以避免请求过于频繁
    delay = random.uniform(1.5, 2.5)
    time.sleep(delay)
    return time.time(), request_count + 1


def fetch_fund_flow_data(flow_type, request_count, last_request_time):
    """获取指定类型的资金流数据"""
    try:
        # 处理频率限制
        last_request_time, request_count = handle_rate_limit(last_request_time, request_count)
        
        logger.info(f"获取 {flow_type} 资金流数据...")
        df = ak.stock_fund_flow_individual(symbol=flow_type)
        return df, request_count, last_request_time
    except Exception as e:
        logger.error(f"获取{flow_type}资金流数据时出错: {e}")
        return None, request_count + 1, last_request_time


def process_fund_flow_data(df, flow_type):
    """处理资金流数据，转换为适合入库的格式"""
    if df is None or df.empty:
        return []
    
    # 记录当前日期作为数据日期
    record_date = datetime.now().date()
    
    processed_data = []
    
    # 处理不同类型的数据
    if flow_type == "即时":
        # 处理即时数据
        for _, row in df.iterrows():
            # 处理百分比和金额字段中的特殊字符
            pct_chg = float(str(row['涨跌幅']).replace('%', '')) if pd.notna(row['涨跌幅']) else None
            turnover_rate = float(str(row['换手率']).replace('%', '')) if pd.notna(row['换手率']) else None
            
            # 处理金额单位（亿、万）
            def parse_amount(amount_str):
                if pd.isna(amount_str):
                    return None
                amount_str = str(amount_str)
                if '亿' in amount_str:
                    return float(amount_str.replace('亿', '')) * 100000000
                elif '万' in amount_str:
                    return float(amount_str.replace('万', '')) * 10000
                else:
                    return float(amount_str)
            
            inflow = parse_amount(row['流入资金'])
            outflow = parse_amount(row['流出资金'])
            net_flow = parse_amount(row['净额'])
            amount = parse_amount(row['成交额'])
            
            processed_data.append((
                str(row['股票代码']),  # code
                row['股票简称'],        # name
                row['最新价'],         # latest_price
                pct_chg,              # pct_chg
                turnover_rate,        # turnover_rate
                inflow,               # inflow
                outflow,              # outflow
                net_flow,             # net_flow
                amount,               # amount
                None,                 # period_pct_chg (即时数据无此字段)
                None,                 # consecutive_turnover (即时数据无此字段)
                flow_type,            # flow_type
                record_date           # record_date
            ))
    else:
        # 处理3日、5日、10日、20日排行数据
        for _, row in df.iterrows():
            period_pct_chg = float(str(row['阶段涨跌幅']).replace('%', '')) if pd.notna(row['阶段涨跌幅']) else None
            consecutive_turnover = float(str(row['连续换手率']).replace('%', '')) if pd.notna(row['连续换手率']) else None
            
            # 处理资金流入净额的单位
            def parse_net_flow(flow_str):
                if pd.isna(flow_str):
                    return None
                flow_str = str(flow_str)
                if '亿' in flow_str:
                    return float(flow_str.replace('亿', '')) * 100000000
                elif '万' in flow_str:
                    return float(flow_str.replace('万', '')) * 10000
                else:
                    return float(flow_str)
            
            net_flow = parse_net_flow(row['资金流入净额'])
            
            processed_data.append((
                str(row['股票代码']),  # code
                row['股票简称'],        # name
                row['最新价'],         # latest_price
                None,                 # pct_chg (阶段数据无此字段)
                None,                 # turnover_rate (阶段数据无此字段)
                None,                 # inflow (阶段数据无此字段)
                None,                 # outflow (阶段数据无此字段)
                net_flow,             # net_flow
                None,                 # amount (阶段数据无此字段)
                period_pct_chg,       # period_pct_chg
                consecutive_turnover, # consecutive_turnover
                flow_type,            # flow_type
                record_date           # record_date
            ))
    
    return processed_data


def fetch_and_save_fund_flow():
    """主函数：获取资金流数据并保存到数据库"""
    try:
        # 连接到 PostgreSQL 数据库
        with psycopg2.connect(**postgresql_config) as conn:
            with conn.cursor() as client:
                # 创建表和添加列注释
                create_table(client)
                add_column_comments(client)
                conn.commit()
                logger.info("数据库表结构初始化完成")

                # 要获取的资金流类型列表
                flow_types = ["即时", "3日排行", "5日排行", "10日排行", "20日排行"]
                
                # 初始化计数器和时间戳
                insert_count = 0
                request_count = 0
                last_request_time = time.time()

                # 遍历每种资金流类型
                for flow_type in flow_types:
                    try:
                        # 获取资金流数据
                        df, request_count, last_request_time = fetch_fund_flow_data(
                            flow_type, request_count, last_request_time)
                        
                        if df is not None and not df.empty:
                            # 处理数据
                            processed_data = process_fund_flow_data(df, flow_type)
                            
                            if processed_data:
                                # 插入数据
                                insert_data(client, processed_data)
                                insert_count += len(processed_data)
                                logger.info(f"{flow_type} 资金流数据处理完成，新增 {len(processed_data)} 条记录")
                                
                                # 定期提交
                                if insert_count >= 1000:
                                    conn.commit()
                                    logger.info(f"已提交 {insert_count} 条数据到数据库")
                                    insert_count = 0
                        else:
                            logger.info(f"{flow_type} 没有获取到数据或数据为空")
                            
                    except Exception as e:
                        logger.error(f"处理 {flow_type} 资金流数据时出错：{e}")
                        conn.rollback()

                # 提交剩余数据
                if insert_count > 0:
                    conn.commit()
                    logger.info(f"已提交最后 {insert_count} 条数据到数据库")

                logger.info("所有类型的资金流数据获取和保存完成！")

    except psycopg2.Error as e:
        logger.error(f"PostgreSQL 数据库错误：{e}")
    except Exception as e:
        logger.error(f"程序运行时出错：{e}")


if __name__ == "__main__":
    fetch_and_save_fund_flow()
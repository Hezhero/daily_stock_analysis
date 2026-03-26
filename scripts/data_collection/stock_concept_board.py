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
log_file = os.path.join(log_dir, 'concept_board_akshare.log')

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
                     CREATE TABLE IF NOT EXISTS concept_board_akshare (
                         id SERIAL PRIMARY KEY,
                         rank INT,
                         board_name VARCHAR(100) NOT NULL,
                         board_code VARCHAR(20) NOT NULL,
                         latest_price NUMERIC(10, 2),
                         change_amount NUMERIC(10, 2),
                         change_percent NUMERIC(10, 2),
                         total_market_value BIGINT,
                         turnover_rate NUMERIC(10, 2),
                         up_stocks INT,
                         down_stocks INT,
                         leading_stock VARCHAR(100),
                         leading_stock_change NUMERIC(10, 2),
                         insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         UNIQUE (board_code, insert_time)  -- 每天的板块数据唯一
                     )
                     """
    client.execute(create_table_query)

    # 创建索引
    try:
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_concept_board_code ON concept_board_akshare (board_code);
                       """)
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_concept_board_insert_time ON concept_board_akshare (insert_time);
                       """)
        logger.info("表结构和索引创建成功")
    except Exception as e:
        logger.error(f"创建索引时出错：{e}")


def add_column_comments(client):
    """添加列注释"""
    column_comments = [
        ("rank", "排名"),
        ("board_name", "板块名称"),
        ("board_code", "板块代码"),
        ("latest_price", "最新价"),
        ("change_amount", "涨跌额"),
        ("change_percent", "涨跌幅（%）"),
        ("total_market_value", "总市值"),
        ("turnover_rate", "换手率（%）"),
        ("up_stocks", "上涨家数"),
        ("down_stocks", "下跌家数"),
        ("leading_stock", "领涨股票"),
        ("leading_stock_change", "领涨股票-涨跌幅（%）"),
        ("insert_time", "数据插入时间")
    ]
    try:
        for column, comment in column_comments:
            comment_query = sql.SQL("COMMENT ON COLUMN concept_board_akshare.{} IS %s").format(sql.Identifier(column))
            client.execute(comment_query, [comment])
        logger.info("列注释添加成功")
    except Exception as e:
        logger.error(f"添加列注释时出错: {e}")


def insert_data(client, data_to_insert):
    """批量插入数据"""
    try:
        insert_query = sql.SQL("""
                               INSERT INTO concept_board_akshare (rank, board_name, board_code, latest_price, 
                                                                change_amount, change_percent, total_market_value,
                                                                turnover_rate, up_stocks, down_stocks, leading_stock,
                                                                leading_stock_change)
                               VALUES %s ON CONFLICT (board_code, insert_time) DO NOTHING
                               """)
        execute_values(client, insert_query, data_to_insert)
    except Exception as e:
        logger.error(f"插入数据时出错: {e}")
        raise


def fetch_concept_board_data(request_count):
    """获取概念板块数据"""
    try:
        df = ak.stock_board_concept_name_em()
        return df, request_count + 1
    except Exception as e:
        logger.error(f"获取概念板块数据时出错: {e}")
        return None, request_count + 1


def fetch_concept_boards():
    """主函数：获取概念板块数据并保存到数据库"""
    try:
        with psycopg2.connect(**postgresql_config) as conn:
            with conn.cursor() as client:
                # 初始化表结构
                create_table(client)
                add_column_comments(client)
                conn.commit()
                logger.info("数据库表结构初始化完成")

                # 初始化计数器
                request_count = 0
                last_request_time = time.time()

                try:
                    # 获取概念板块数据
                    logger.info("开始获取概念板块数据...")
                    df, request_count = fetch_concept_board_data(request_count)

                    if df is not None and not df.empty:
                        # 重命名列名以匹配数据库字段
                        df = df.rename(columns={
                            '排名': 'rank',
                            '板块名称': 'board_name',
                            '板块代码': 'board_code',
                            '最新价': 'latest_price',
                            '涨跌额': 'change_amount',
                            '涨跌幅': 'change_percent',
                            '总市值': 'total_market_value',
                            '换手率': 'turnover_rate',
                            '上涨家数': 'up_stocks',
                            '下跌家数': 'down_stocks',
                            '领涨股票': 'leading_stock',
                            '领涨股票-涨跌幅': 'leading_stock_change'
                        })

                        # 确保包含所有需要的列
                        required_columns = ['rank', 'board_name', 'board_code', 'latest_price', 'change_amount',
                                           'change_percent', 'total_market_value', 'turnover_rate', 'up_stocks',
                                           'down_stocks', 'leading_stock', 'leading_stock_change']
                        df = df[required_columns]

                        # 处理可能的空值
                        df = df.fillna(0)

                        # 转换数据类型
                        df['total_market_value'] = df['total_market_value'].astype(int)
                        df['up_stocks'] = df['up_stocks'].astype(int)
                        df['down_stocks'] = df['down_stocks'].astype(int)

                        # 插入数据库
                        data_to_insert = [tuple(record.values()) for record in df.to_dict('records')]
                        insert_data(client, data_to_insert)
                        conn.commit()
                        
                        logger.info(f"成功保存{len(data_to_insert)}条概念板块数据到数据库")
                    else:
                        logger.info("没有获取到概念板块数据或获取失败")

                except Exception as e:
                    logger.error(f"处理概念板块数据时出错：{e}")
                    conn.rollback()

                logger.info("概念板块数据获取完成！")

    except psycopg2.Error as e:
        logger.error(f"PostgreSQL 数据库错误：{e}")
    except Exception as e:
        logger.error(f"程序运行时出错：{e}")


if __name__ == "__main__":
    fetch_concept_boards()
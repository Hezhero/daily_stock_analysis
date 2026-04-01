import akshare as ak
import psycopg2
from psycopg2 import sql
import time
import logging
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from datetime import datetime
import random
import pandas as pd

# 加载环境变量
load_dotenv()

# 配置日志
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'result')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'stock_institution_participation.log')

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
    'password': os.environ.get('PG_PASSWORD'),
    'database': os.environ.get('PG_DATABASE', 'akshare'),
    'port': os.environ.get('PG_PORT', '5431')
}


def create_table(client):
    """创建表（如果不存在）并添加必要的索引和约束"""
    create_table_query = """
                     CREATE TABLE IF NOT EXISTS stock_institution_participation (
                         id SERIAL PRIMARY KEY,
                         code VARCHAR(20) NOT NULL,
                         name VARCHAR(100) NOT NULL,
                         trade_date DATE NOT NULL,
                         institution_participation NUMERIC(10, 2),
                         insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         UNIQUE (code, trade_date)  -- 确保同一股票同一交易日数据唯一
                     )
                     """
    client.execute(create_table_query)

    # 创建索引
    try:
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_stock_code ON stock_institution_participation (code);
                       """)
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_trade_date ON stock_institution_participation (trade_date);
                       """)
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_insert_time ON stock_institution_participation (insert_time);
                       """)
        logger.info("表结构和索引创建成功")
    except Exception as e:
        logger.error(f"创建索引时出错：{e}")


def add_column_comments(client):
    """添加列注释"""
    column_comments = [
        ("code", "股票代码"),
        ("name", "股票名称"),
        ("trade_date", "交易日"),
        ("institution_participation", "机构参与度（%）"),
        ("insert_time", "数据插入时间")
    ]
    try:
        for column, comment in column_comments:
            comment_query = sql.SQL("COMMENT ON COLUMN stock_institution_participation.{} IS %s").format(sql.Identifier(column))
            client.execute(comment_query, [comment])
        logger.info("列注释添加成功")
    except Exception as e:
        logger.error(f"添加列注释时出错: {e}")


def insert_data(client, data_to_insert):
    """批量插入数据"""
    try:
        insert_query = sql.SQL("""
                               INSERT INTO stock_institution_participation (
                                   code, name, trade_date, institution_participation
                               ) VALUES %s ON CONFLICT (code, trade_date) DO NOTHING
                               """)
        execute_values(client, insert_query, data_to_insert)
    except Exception as e:
        logger.error(f"插入数据时出错: {e}")
        raise


def handle_rate_limit(last_request_time):
    """处理API请求频率限制"""
    current_time = time.time()
    time_diff = current_time - last_request_time
    
    # 控制请求频率，添加随机延迟避免被限制
    # if time_diff < 3:  # 确保至少间隔3秒
    #     delay = random.uniform(3 - time_diff, 5 - time_diff)
    #     time.sleep(delay)
    # 添加随机延迟以避免周期性高峰
    delay = random.uniform(1.5, 2.5)  # 随机延迟1.5-2.5秒
    time.sleep(delay)
    
    return time.time()


def get_all_stock_list():
    """获取所有A股股票列表（代码和名称）"""
    try:
        # 获取A股列表
        df = ak.stock_zh_a_spot_em()
        # 筛选出股票代码和名称
        stock_list = df[['代码', '名称']].rename(columns={
            '代码': 'code',
            '名称': 'name'
        }).to_dict('records')
        logger.info(f"成功获取{len(stock_list)}只A股股票列表")
        return stock_list
    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        return []


def fetch_institution_data(stock_code):
    """获取指定股票的机构参与度数据"""
    try:
        df = ak.stock_comment_detail_zlkp_jgcyd_em(symbol=stock_code)
        return df
    except Exception as e:
        logger.warning(f"获取股票{stock_code}的机构参与度数据失败: {e}")
        return None


def get_latest_trade_date(client, stock_code):
    """获取指定股票在数据库中的最新交易日期"""
    try:
        query = """
                SELECT MAX(trade_date) 
                FROM stock_institution_participation 
                WHERE code = %s
                """
        client.execute(query, (stock_code,))
        result = client.fetchone()[0]
        return result
    except Exception as e:
        logger.error(f"获取股票{stock_code}最新交易日期时出错: {e}")
        return None


def process_and_store_data():
    """主函数：获取并存储所有股票的机构参与度数据"""
    try:
        with psycopg2.connect(**postgresql_config) as conn:
            with conn.cursor() as client:
                # 初始化表结构
                create_table(client)
                add_column_comments(client)
                conn.commit()
                logger.info("数据库表初始化完成")

                # 获取所有股票列表
                stocks = get_all_stock_list()
                if not stocks:
                    logger.error("没有获取到股票列表，程序退出")
                    return

                last_request_time = time.time()
                total_stocks = len(stocks)
                success_count = 0
                total_records = 0

                # 遍历每个股票获取机构参与度数据
                for i, stock in enumerate(stocks, 1):
                    stock_code = stock['code']
                    stock_name = stock['name']
                    logger.info(f"处理股票 {i}/{total_stocks}: {stock_code}({stock_name})")

                    try:
                        # 获取最新交易日期，实现增量更新
                        latest_date = get_latest_trade_date(client, stock_code)

                        # 控制请求频率
                        last_request_time = handle_rate_limit(last_request_time)

                        # 获取机构参与度数据
                        df = fetch_institution_data(stock_code)
                        if df is None or df.empty:
                            logger.info(f"股票 {stock_code} 没有机构参与度数据")
                            continue

                        # 数据处理
                        df = df.rename(columns={
                            '交易日': 'trade_date',
                            '机构参与度': 'institution_participation'
                        })

                        # 转换日期格式
                        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

                        # 筛选出最新日期之后的数据（增量更新）
                        if latest_date:
                            df = df[df['trade_date'] > latest_date]
                            if df.empty:
                                logger.info(f"股票 {stock_code} 没有新的机构参与度数据")
                                continue

                        # 添加股票代码和名称
                        df['code'] = stock_code
                        df['name'] = stock_name

                        # 确保列顺序正确
                        required_columns = ['code', 'name', 'trade_date', 'institution_participation']
                        df = df.reindex(columns=required_columns)

                        # 处理空值和数据类型
                        df = df.fillna(0)
                        df['institution_participation'] = pd.to_numeric(
                            df['institution_participation'], errors='coerce').fillna(0)

                        # 准备插入数据
                        data_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]
                        insert_data(client, data_to_insert)

                        # 统计信息
                        record_count = len(data_to_insert)
                        total_records += record_count
                        success_count += 1
                        logger.info(f"成功插入 {record_count} 条机构参与度数据")

                        # 每处理20个股票提交一次
                        if i % 20 == 0:
                            conn.commit()
                            logger.info(f"已提交前 {i} 个股票的数据")

                    except Exception as e:
                        logger.error(f"处理股票 {stock_code} 时出错: {e}")
                        conn.rollback()

                # 提交剩余数据
                conn.commit()
                logger.info(f"所有股票处理完成，成功处理 {success_count}/{total_stocks} 只股票，共插入 {total_records} 条机构参与度数据")

    except psycopg2.Error as e:
        logger.error(f"数据库错误: {e}")
    except Exception as e:
        logger.error(f"程序运行错误: {e}")


if __name__ == "__main__":
    process_and_store_data()
    
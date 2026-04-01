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
log_file = os.path.join(log_dir, 'concept_board_stocks_akshare.log')

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
                     CREATE TABLE IF NOT EXISTS concept_board_stocks_akshare (
                         id SERIAL PRIMARY KEY,
                         board_code VARCHAR(20) NOT NULL,
                         board_name VARCHAR(100) NOT NULL,
                         serial_number INT,
                         stock_code VARCHAR(20) NOT NULL,
                         stock_name VARCHAR(100) NOT NULL,
                         latest_price NUMERIC(10, 2),
                         change_percent NUMERIC(10, 2),
                         change_amount NUMERIC(10, 2),
                         volume NUMERIC(18, 2),
                         amount NUMERIC(18, 2),
                         amplitude NUMERIC(10, 2),
                         high NUMERIC(10, 2),
                         low NUMERIC(10, 2),
                         open NUMERIC(10, 2),
                         prev_close NUMERIC(10, 2),
                         turnover_rate NUMERIC(10, 2),
                         pe_dynamic NUMERIC(10, 2),
                         pb NUMERIC(10, 2),
                         insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         UNIQUE (board_code, stock_code, insert_time)  -- 每天每个板块的个股数据唯一
                     )
                     """
    client.execute(create_table_query)

    # 创建索引
    try:
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_concept_board_stocks_board_code ON concept_board_stocks_akshare (board_code);
                       """)
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_concept_board_stocks_stock_code ON concept_board_stocks_akshare (stock_code);
                       """)
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_concept_board_stocks_insert_time ON concept_board_stocks_akshare (insert_time);
                       """)
        logger.info("表结构和索引创建成功")
    except Exception as e:
        logger.error(f"创建索引时出错：{e}")


def add_column_comments(client):
    """添加列注释"""
    column_comments = [
        ("board_code", "板块代码"),
        ("board_name", "板块名称"),
        ("serial_number", "序号"),
        ("stock_code", "股票代码"),
        ("stock_name", "股票名称"),
        ("latest_price", "最新价"),
        ("change_percent", "涨跌幅（%）"),
        ("change_amount", "涨跌额"),
        ("volume", "成交量（手）"),
        ("amount", "成交额"),
        ("amplitude", "振幅（%）"),
        ("high", "最高价"),
        ("low", "最低价"),
        ("open", "今开"),
        ("prev_close", "昨收"),
        ("turnover_rate", "换手率（%）"),
        ("pe_dynamic", "市盈率-动态"),
        ("pb", "市净率"),
        ("insert_time", "数据插入时间")
    ]
    try:
        for column, comment in column_comments:
            comment_query = sql.SQL("COMMENT ON COLUMN concept_board_stocks_akshare.{} IS %s").format(sql.Identifier(column))
            client.execute(comment_query, [comment])
        logger.info("列注释添加成功")
    except Exception as e:
        logger.error(f"添加列注释时出错: {e}")


def insert_data(client, data_to_insert):
    """批量插入数据"""
    try:
        insert_query = sql.SQL("""
                               INSERT INTO concept_board_stocks_akshare (board_code, board_name, serial_number, stock_code,
                                                                         stock_name, latest_price, change_percent,
                                                                         change_amount, volume, amount, amplitude, high,
                                                                         low, open, prev_close, turnover_rate,
                                                                         pe_dynamic, pb)
                               VALUES %s ON CONFLICT (board_code, stock_code, insert_time) DO NOTHING
                               """)
        execute_values(client, insert_query, data_to_insert)
    except Exception as e:
        logger.error(f"插入数据时出错: {e}")
        raise


def handle_rate_limit(last_request_time, request_count):
    """处理API请求频率限制"""
    current_time = time.time()
    time_diff = current_time - last_request_time

    # 添加随机延迟避免请求过于频繁
    delay = random.uniform(3, 4)
    time.sleep(delay)
    return time.time(), request_count


def get_concept_board_list():
    """获取所有概念板块列表"""
    try:
        df = ak.stock_board_concept_name_em()
        # 只保留板块代码和名称
        board_list = df[['板块代码', '板块名称']].rename(columns={
            '板块代码': 'board_code',
            '板块名称': 'board_name'
        })
        logger.info(f"成功获取{len(board_list)}个概念板块列表")
        return board_list
    except Exception as e:
        logger.error(f"获取概念板块列表时出错：{e}")
        return None


def fetch_board_stocks(symbol, request_count):
    """获取指定概念板块的成分股数据"""
    try:
        df = ak.stock_board_concept_cons_em(symbol=symbol)
        return df, request_count + 1
    except Exception as e:
        logger.error(f"获取板块{symbol}成分股数据时出错: {e}")
        return None, request_count + 1


def fetch_concept_board_stocks():
    """主函数：获取所有概念板块的成分股数据并保存到数据库"""
    try:
        with psycopg2.connect(**postgresql_config) as conn:
            with conn.cursor() as client:
                # 初始化表结构
                create_table(client)
                add_column_comments(client)
                conn.commit()
                logger.info("数据库表结构初始化完成")

                # 获取所有概念板块列表
                board_list = get_concept_board_list()
                if board_list is None or board_list.empty:
                    logger.error("无法获取概念板块列表，程序终止")
                    return

                # 初始化计数器
                request_count = 1  # 获取板块列表算一次请求
                last_request_time = time.time()
                processed_count = 0
                total_boards = len(board_list)
                insert_count = 0

                # 遍历每个概念板块获取成分股
                for index, board in board_list.iterrows():
                    board_code = board['board_code']
                    board_name = board['board_name']
                    processed_count += 1

                    try:
                        # 处理频率限制
                        last_request_time, request_count = handle_rate_limit(last_request_time, request_count)

                        # 获取成分股数据
                        logger.info(f"[{processed_count}/{total_boards}] 获取板块 {board_code}({board_name}) 的成分股数据")
                        df, request_count = fetch_board_stocks(board_code, request_count)

                        if df is not None and not df.empty:
                            # 重命名列名以匹配数据库字段
                            df = df.rename(columns={
                                '序号': 'serial_number',
                                '代码': 'stock_code',
                                '名称': 'stock_name',
                                '最新价': 'latest_price',
                                '涨跌幅': 'change_percent',
                                '涨跌额': 'change_amount',
                                '成交量': 'volume',
                                '成交额': 'amount',
                                '振幅': 'amplitude',
                                '最高': 'high',
                                '最低': 'low',
                                '今开': 'open',
                                '昨收': 'prev_close',
                                '换手率': 'turnover_rate',
                                '市盈率-动态': 'pe_dynamic',
                                '市净率': 'pb'
                            })

                            # 添加板块代码和名称
                            df['board_code'] = board_code
                            df['board_name'] = board_name

                            # 确保包含所有需要的列
                            required_columns = ['board_code', 'board_name', 'serial_number', 'stock_code', 'stock_name',
                                               'latest_price', 'change_percent', 'change_amount', 'volume', 'amount',
                                               'amplitude', 'high', 'low', 'open', 'prev_close', 'turnover_rate',
                                               'pe_dynamic', 'pb']
                            df = df[required_columns]

                            # 处理可能的空值
                            df = df.fillna(0)

                            # 转换数据类型
                            numeric_cols = ['serial_number', 'latest_price', 'change_percent', 'change_amount',
                                           'volume', 'amount', 'amplitude', 'high', 'low', 'open', 'prev_close',
                                           'turnover_rate', 'pe_dynamic', 'pb']
                            for col in numeric_cols:
                                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

                            # 准备插入数据
                            data_to_insert = [tuple(record.values()) for record in df.to_dict('records')]
                            insert_data(client, data_to_insert)
                            
                            # 更新插入计数器
                            insert_count += len(data_to_insert)
                            logger.info(f"板块 {board_code}({board_name}) 成功插入{len(data_to_insert)}条成分股数据")

                            # 定期提交事务
                            if insert_count >= 1000:
                                conn.commit()
                                logger.info(f"已提交{insert_count}条成分股数据到数据库")
                                insert_count = 0

                        else:
                            logger.info(f"板块 {board_code}({board_name}) 没有获取到成分股数据或获取失败")

                    except Exception as e:
                        logger.error(f"处理板块 {board_code}({board_name}) 时出错：{e}")
                        conn.rollback()

                # 提交剩余数据
                if insert_count > 0:
                    conn.commit()
                    logger.info(f"已提交最后{insert_count}条成分股数据到数据库")

                logger.info("所有概念板块的成分股数据获取完成！")

    except psycopg2.Error as e:
        logger.error(f"PostgreSQL 数据库错误：{e}")
    except Exception as e:
        logger.error(f"程序运行时出错：{e}")


if __name__ == "__main__":
    fetch_concept_board_stocks()
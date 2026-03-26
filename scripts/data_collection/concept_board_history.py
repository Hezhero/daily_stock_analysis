import akshare as ak
import psycopg2
from psycopg2 import sql
import time
import logging
import os
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import random
import pandas as pd

# 配置日志
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'result')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'concept_board_history_hfq_akshare.log')

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
    create_table_query = """
                     CREATE TABLE IF NOT EXISTS concept_board_history_hfq_akshare (
                         id SERIAL PRIMARY KEY,
                         board_code VARCHAR(20) NOT NULL,
                         board_name VARCHAR(100) NOT NULL,
                         trade_date DATE NOT NULL,
                         open NUMERIC(10, 2),
                         close NUMERIC(10, 2),
                         high NUMERIC(10, 2),
                         low NUMERIC(10, 2),
                         change_percent NUMERIC(10, 2),
                         change_amount NUMERIC(10, 2),
                         volume BIGINT,
                         amount NUMERIC(18, 2),
                         amplitude NUMERIC(10, 2),
                         turnover_rate NUMERIC(10, 2),
                         adjust_type VARCHAR(10) NOT NULL,  -- 复权类型: hfq-后复权
                         insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         UNIQUE (board_code, trade_date, adjust_type)  -- 唯一约束
                     )
                     """
    client.execute(create_table_query)

    # 创建索引
    try:
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_concept_hist_board_code ON concept_board_history_hfq_akshare (board_code);
                       """)
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_concept_hist_trade_date ON concept_board_history_hfq_akshare (trade_date);
                       """)
        client.execute("""
                       CREATE INDEX IF NOT EXISTS idx_concept_hist_insert_time ON concept_board_history_hfq_akshare (insert_time);
                       """)
        logger.info("表结构和索引创建成功")
    except Exception as e:
        logger.error(f"创建索引时出错：{e}")


def add_column_comments(client):
    """添加列注释"""
    column_comments = [
        ("board_code", "板块代码"),
        ("board_name", "板块名称"),
        ("trade_date", "交易日期"),
        ("open", "开盘价"),
        ("close", "收盘价"),
        ("high", "最高价"),
        ("low", "最低价"),
        ("change_percent", "涨跌幅（%）"),
        ("change_amount", "涨跌额"),
        ("volume", "成交量"),
        ("amount", "成交额"),
        ("amplitude", "振幅（%）"),
        ("turnover_rate", "换手率（%）"),
        ("adjust_type", "复权类型"),
        ("insert_time", "数据插入时间")
    ]
    try:
        for column, comment in column_comments:
            comment_query = sql.SQL("COMMENT ON COLUMN concept_board_history_hfq_akshare.{} IS %s").format(sql.Identifier(column))
            client.execute(comment_query, [comment])
        logger.info("列注释添加成功")
    except Exception as e:
        logger.error(f"添加列注释时出错: {e}")


def insert_data(client, data_to_insert):
    """批量插入数据"""
    try:
        insert_query = sql.SQL("""
                               INSERT INTO concept_board_history_hfq_akshare (board_code, board_name, trade_date, open, close,
                                                                          high, low, change_percent, change_amount,
                                                                          volume, amount, amplitude, turnover_rate,
                                                                          adjust_type)
                               VALUES %s ON CONFLICT (board_code, trade_date, adjust_type) DO NOTHING
                               """)
        execute_values(client, insert_query, data_to_insert)
    except Exception as e:
        logger.error(f"插入数据时出错: {e}")
        raise


def get_latest_trade_date(client, board_code, adjust_type):
    """获取指定板块在数据库中的最新交易日期"""
    try:
        query = """
                SELECT MAX(trade_date) 
                FROM concept_board_history_hfq_akshare 
                WHERE board_code = %s AND adjust_type = %s
                """
        client.execute(query, (board_code, adjust_type))
        result = client.fetchone()[0]
        return result
    except Exception as e:
        logger.error(f"获取{board_code}最新交易日期时出错: {e}")
        return None


def handle_rate_limit(last_request_time, request_count):
    """处理API请求频率限制"""
    current_time = time.time()
    time_diff = current_time - last_request_time

    # 添加随机延迟避免请求过于频繁
    delay = random.uniform(2, 3)
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


def fetch_board_hist_data(board_code, start_date, end_date, adjust_type, request_count):
    """获取指定概念板块的历史行情数据"""
    try:
        df = ak.stock_board_concept_hist_em(
            symbol=board_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=adjust_type
        )
        return df, request_count + 1
    except Exception as e:
        logger.error(f"获取板块{board_code}历史数据时出错: {e}")
        return None, request_count + 1


def fetch_concept_board_hist_data():
    """主函数：获取所有概念板块的历史行情后复权数据并保存到数据库"""
    adjust_type = "hfq"  # 后复权
    start_date_default = "20200101"  # 起始日期从2008年开始

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

                # 遍历每个概念板块获取历史数据
                for index, board in board_list.iterrows():
                    board_code = board['board_code']
                    board_name = board['board_name']
                    processed_count += 1

                    try:
                        # 获取当前日期作为结束日期
                        end_date = datetime.now().strftime('%Y%m%d')

                        # 获取最新交易日期
                        latest_date = get_latest_trade_date(client, board_code, adjust_type)

                        # 确定起始日期
                        if latest_date:
                            # 从最新日期的下一天开始获取
                            next_date = (latest_date + timedelta(days=1)).strftime('%Y%m%d')
                            start_date = next_date
                        else:
                            # 首次获取从2008年开始
                            start_date = start_date_default

                        # 检查是否需要更新数据
                        if start_date > end_date or (start_date == end_date and datetime.now().strftime('%H') < '15'):
                            logger.info(f"[{processed_count}/{total_boards}] 板块 {board_code}({board_name}) 的数据已是最新的，无需更新")
                            continue

                        # 处理频率限制
                        last_request_time, request_count = handle_rate_limit(last_request_time, request_count)

                        # 获取历史数据
                        logger.info(f"[{processed_count}/{total_boards}] 获取板块 {board_code}({board_name}) 的历史数据，起始日期: {start_date}，结束日期: {end_date}")
                        df, request_count = fetch_board_hist_data(
                            board_name, start_date, end_date, adjust_type, request_count)

                        if df is not None and not df.empty:
                            # 检查是否包含预期的列名，避免在空DataFrame上重命名导致错误
                            expected_columns = ['日期', '开盘', '收盘', '最高', '最低', '涨跌幅', '涨跌额', '成交量',
                                                '成交额', '振幅', '换手率']
                            if not all(col in df.columns for col in expected_columns):
                                logger.warning(f"板块 {board_code}({board_name}) 返回的数据列不完整，跳过处理")
                                continue

                            # 重命名列名以匹配数据库字段
                            df = df.rename(columns={
                                '日期': 'trade_date',
                                '开盘': 'open',
                                '收盘': 'close',
                                '最高': 'high',
                                '最低': 'low',
                                '涨跌幅': 'change_percent',
                                '涨跌额': 'change_amount',
                                '成交量': 'volume',
                                '成交额': 'amount',
                                '振幅': 'amplitude',
                                '换手率': 'turnover_rate'
                            })

                            # 添加板块信息和复权类型
                            df['board_code'] = board_code
                            df['board_name'] = board_name
                            df['adjust_type'] = adjust_type

                            # 确保包含所有需要的列
                            required_columns = ['board_code', 'board_name', 'trade_date', 'open', 'close', 'high',
                                                'low',
                                                'change_percent', 'change_amount', 'volume', 'amount', 'amplitude',
                                                'turnover_rate', 'adjust_type']
                            df = df[required_columns]

                            # 处理日期格式
                            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

                            # 处理空值
                            df = df.fillna(0)

                            # 转换数据类型
                            numeric_cols = ['open', 'close', 'high', 'low', 'change_percent', 'change_amount',
                                            'volume', 'amount', 'amplitude', 'turnover_rate']
                            for col in numeric_cols:
                                if col == 'volume':
                                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                                else:
                                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

                            # 准备插入数据
                            data_to_insert = [tuple(record.values()) for record in df.to_dict('records')]
                            insert_data(client, data_to_insert)

                            # 更新插入计数器
                            insert_count += len(data_to_insert)
                            logger.info(f"板块 {board_code}({board_name}) 成功插入{len(data_to_insert)}条历史数据")

                            # 定期提交事务
                            if insert_count >= 1000:
                                conn.commit()
                                logger.info(f"已提交{insert_count}条历史数据到数据库")
                                insert_count = 0

                        else:
                            logger.info(f"板块 {board_code}({board_name}) 在指定时间范围内没有新的历史数据或获取失败")

                    except Exception as e:
                        logger.error(f"处理板块 {board_code}({board_name}) 时出错：{e}")
                        conn.rollback()

                # 提交剩余数据
                if insert_count > 0:
                    conn.commit()
                    logger.info(f"已提交最后{insert_count}条历史数据到数据库")

                logger.info("所有概念板块的历史行情后复权数据获取完成！")

    except psycopg2.Error as e:
        logger.error(f"PostgreSQL 数据库错误：{e}")
    except Exception as e:
        logger.error(f"程序运行时出错：{e}")


if __name__ == "__main__":
    fetch_concept_board_hist_data()
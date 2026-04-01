import argparse
import io
import logging
# 配置日志
import os
import sys
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2.sql as sql
import backoff
import pandas as pd
import psycopg2
import warnings

# 加载环境变量
load_dotenv()

# 抑制 numpy/pandas 警告
warnings.filterwarnings('ignore')

import baostock as bs

# 每日最新数据更新时间：
# 当前交易日17:30，完成日K线数据入库；
# 当前交易日18:00，完成复权因子数据入库；
# 第二自然日11:00，完成分钟K线数据入库；
# 第二自然日1:30，完成前交易日“其它财务报告数据”入库；
# 周六17:30，完成周线数据入库；
# 每周数据更新时间：
# 每周一下午，完成上证50成份股、沪深300成份股、中证500成份股信息数据入库；
# conda activate tushare && python baostock_history_postgresql.py  --start_date 2005-01-01
# python baostock_history_postgresql.py --max_workers 10 --stock_codes sh.000001 sh.600000 --start_date 2023-01-01 --end_date 2023-12-31
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'result')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'baostock_history.log')

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
    'database': os.environ.get('PG_DATABASE', 'baostock'),
    'port': os.environ.get('PG_PORT', '5431')
}

# 重试配置
RETRY_CONFIG = {
    'db_connection': {'max_tries': 5, 'base': 2, 'factor': 1, 'max_value': 30},
    'stock_data': {'max_tries': 3, 'base': 2, 'factor': 1, 'max_value': 10}
}

# 性能配置
PERF_CONFIG = {
    'batch_size': 5000,
    'commit_size': 10000
}


class DatabaseManager:
    """数据库连接管理器"""

    @staticmethod
    @contextmanager
    def get_connection():
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            conn = psycopg2.connect(**postgresql_config)
            yield conn
        except psycopg2.Error as e:
            logger.error(f"数据库连接失败: {e}")
            raise
        finally:
            if conn:
                conn.close()

    @staticmethod
    @backoff.on_exception(backoff.expo, Exception, **RETRY_CONFIG['db_connection'])
    def check_connection():
        """检查数据库连接"""
        with DatabaseManager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                if cursor.fetchone()[0] != 1:
                    raise Exception("数据库连接测试失败")


class BaoStockManager:
    """BaoStock API 管理器"""

    _is_logged_in = False

    @classmethod
    @contextmanager
    def session(cls):
        """BaoStock 登录会话上下文"""
        if not cls._is_logged_in:
            max_retries = 3
            retry_count = 0

            while retry_count < max_retries:
                try:
                    logger.info("尝试登录 BaoStock")
                    login_result = bs.login()
                    if login_result.error_code != '0':
                        logger.error(f"BaoStock 登录失败: {login_result.error_msg}")
                        retry_count += 1
                        if retry_count < max_retries:
                            time.sleep(2 ** retry_count)
                            continue
                        raise Exception(f"BaoStock 登录失败超过最大重试次数: {login_result.error_msg}")

                    logger.info("BaoStock 登录成功")
                    cls._is_logged_in = True
                    break

                except Exception as e:
                    logger.error(f"BaoStock 登录失败: {e}")
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(2 ** retry_count)
                        continue
                    raise Exception(f"BaoStock 登录失败超过最大重试次数: {e}")
        else:
            logger.info("BaoStock 已经登录，复用现有会话")

        try:
            yield
        except Exception as e:
            logger.error(f"BaoStock会话中发生错误: {e}")
            raise
        finally:
            # 程序结束时才登出
            pass


class DataFormatter:
    """数据格式化工具类"""

    @staticmethod
    def clean_data(df):
        """数据清洗和类型转换"""
        if df.empty:
            return df

        # 处理空值
        df = df.replace('', None)

        # 类型转换
        numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume',
                        'amount', 'turn', 'pctChg', 'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 日期格式化
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date

        return df


class DatabaseOperations:
    """数据库操作类"""

    @staticmethod
    def init_schema(conn):
        """初始化数据库表结构"""
        with conn.cursor() as cursor:
            # 创建表
            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS baostock_daily_history
                           (   code VARCHAR(10) NOT NULL,
                               name VARCHAR(100),
                               date DATE NOT NULL,
                               open NUMERIC(10,4),
                               close NUMERIC(10,4),
                               high NUMERIC(10,4),
                               low NUMERIC(10,4),
                               pre_close NUMERIC(10,4),
                               volume NUMERIC(20),
                               amount NUMERIC(20,4),
                               adjust_flag VARCHAR(1),
                               turn NUMERIC(10,6),
                               trade_status VARCHAR(1),
                               pct_chg NUMERIC(10,6),
                               pe_ttm NUMERIC(20,6),
                               pb_mrq NUMERIC(20,6),
                               ps_ttm NUMERIC(20,6),
                               pcf_ncf_ttm NUMERIC(20,6),
                               is_st VARCHAR(1),
                               ipo_date DATE,
                               out_date DATE,
                               type VARCHAR(2),
                               status VARCHAR(1),
                               insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                               UNIQUE(code,date)
                           )
                           """)

            # 创建索引
            cursor.execute("""
                           CREATE INDEX IF NOT EXISTS idx_baostock_daily_history_code
                               ON baostock_daily_history (code)
                           """)

            cursor.execute("""
                           CREATE INDEX IF NOT EXISTS idx_baostock_daily_history_date
                               ON baostock_daily_history (date)
                           """)

            # 添加列注释
            DatabaseOperations.add_column_comments(cursor)

            conn.commit()
    @staticmethod
    def add_column_comments(client):
        column_comments = [
            ("code", "股票代码"),
            ("name", "股票名称"),
            ("date", "交易日期"),
            ("open", "开盘价"),
            ("close", "收盘价"),
            ("high", "最高价"),
            ("low", "最低价"),
            ("pre_close", "前收盘价"),
            ("volume", "成交量 (累计 单位：股)"),
            ("amount", "成交额 (单位：人民币元)"),
            ("adjust_flag", "复权状态(1：后复权，2：前复权，3：不复权)"),
            ("turn", "换手率[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%"),
            ("trade_status", "交易状态(1：正常交易0：停牌)"),
            ("pct_chg", "涨跌幅 (百分比)日涨跌幅=[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%"),
            ("pe_ttm", "滚动市盈率(指定交易日的股票收盘价/指定交易日的每股盈余TTM)=(指定交易日的股票收盘价*截至当日公司总股本)/归属母公司股东净利润TTM"),
            ("pb_mrq", "市净率(指定交易日的股票收盘价/指定交易日的每股净资产)=总市值/(最近披露的归属母公司股东的权益-其他权益工具)"),
            ("ps_ttm", "滚动市销率(指定交易日的股票收盘价/指定交易日的每股销售额)=(指定交易日的股票收盘价*截至当日公司总股本)/营业总收入TTM"),
            ("pcf_ncf_ttm", "滚动市现率(指定交易日的股票收盘价/指定交易日的每股现金流TTM)=(指定交易日的股票收盘价*截至当日公司总股本)/现金以及现金等价物净增加额TTM"),
            ("is_st", "是否ST股，1是，0否"),
            ("ipo_date", "上市日期"),
            ("out_date", "退市日期"),
            ("type", "证券类型，其中1：股票，2：指数，3：其它，4：可转债，5：ETF"),
            ("status", "上市状态，其中1：上市，0：退市"),
            ("insert_time", "数据插入时间")
        ]
        """添加列注释

        Args:
            client: 数据库游标对象
        """
        try:
            for column, comment in column_comments:
                comment_query = sql.SQL("COMMENT ON COLUMN baostock_daily_history.{} IS %s").format(
                    sql.Identifier(column))
                client.execute(comment_query, [comment])
            logger.info("列注释添加成功")
        except Exception as e:
            logger.error(f"添加列注释时出错: {e}")

    @staticmethod
    def get_latest_date(conn, code):
        """获取指定股票在数据库中的最新交易日期

        Args:
            conn: 数据库连接
            code: 股票代码

        Returns:
            date: 最新交易日期，如果没有数据则返回None
        """
        try:
            with conn.cursor() as cursor:
                query = """
                        SELECT MAX(date) \
                        FROM baostock_daily_history \
                        WHERE code = %s \
                        """
                cursor.execute(query, (code,))
                result = cursor.fetchone()[0]
                return result
        except Exception as e:
            logger.error(f"获取{code}最新交易日期时出错: {e}")
            return None

    @staticmethod
    def bulk_insert(conn, data, batch_size):
        """高性能批量插入

        Args:
            conn: 数据库连接
            data: 要插入的数据，可以是DataFrame或列表
            batch_size: 批量大小

        Returns:
            int: 成功插入的记录数量
        """
        if data is None or (isinstance(data, pd.DataFrame) and data.empty):
            return 0

        buffer = io.StringIO()
        df = pd.DataFrame(data)
        df = DataFormatter.clean_data(df)

        # 检查DataFrame是否为空
        if df.empty:
            return 0

        # 列名映射：从BaoStock API列名到数据库列名
        column_mapping = {
            'code': 'code',
            'name': 'name',
            'date': 'date',
            'open': 'open',
            'close': 'close',
            'high': 'high',
            'low': 'low',
            'preclose': 'pre_close',
            'volume': 'volume',
            'amount': 'amount',
            'adjustflag': 'adjust_flag',
            'turn': 'turn',
            'tradestatus': 'trade_status',
            'pctChg': 'pct_chg',
            'peTTM': 'pe_ttm',
            'pbMRQ': 'pb_mrq',
            'psTTM': 'ps_ttm',
            'pcfNcfTTM': 'pcf_ncf_ttm',
            'isST': 'is_st',
            'ipo_date': 'ipo_date',
            'out_date': 'out_date',
            'type': 'type',
            'status': 'status'
        }

        # 重命名列以匹配数据库表结构
        df = df.rename(columns=column_mapping)

        # 确保DataFrame的列顺序与数据库表结构一致
        expected_columns = [
            'code', 'name', 'date', 'open', 'close', 'high', 'low', 'pre_close', 'volume',
            'amount', 'adjust_flag', 'turn', 'trade_status', 'pct_chg', 'pe_ttm',
            'pb_mrq', 'ps_ttm', 'pcf_ncf_ttm', 'is_st', 'ipo_date', 'out_date',
            'type', 'status'
        ]

        # 重新排列列
        df = df.reindex(columns=expected_columns)

        # 创建临时表并插入数据
        temp_table_name = f"temp_baostock_data_{int(time.time())}"

        try:
            # 创建临时表
            with conn.cursor() as cursor:
                cursor.execute(f"""
                CREATE TEMPORARY TABLE {temp_table_name} (
                    LIKE baostock_daily_history INCLUDING ALL
                ) ON COMMIT DROP
                """)

                # 将数据写入临时表
                buffer = io.StringIO()
                df.to_csv(buffer, sep='\t', na_rep='nan', header=False, index=False)
                buffer.seek(0)

                cursor.copy_from(
                    buffer,
                    temp_table_name,
                    sep='\t',
                    null='nan',
                    columns=expected_columns
                )

                # 使用INSERT ON CONFLICT将数据从临时表插入到目标表，避免重复
                cursor.execute(f"""
                INSERT INTO baostock_daily_history ({', '.join(expected_columns)})
                SELECT {', '.join(expected_columns)} FROM {temp_table_name}
                ON CONFLICT (code, date) DO NOTHING
                RETURNING code
                """)

                # 获取实际插入的行数
                inserted_rows = cursor.rowcount

                # 提交事务
                conn.commit()

                return inserted_rows

        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"批量插入失败: {e}")
            raise


class DateManager:
    """日期管理类"""

    @staticmethod
    def get_date_range(conn, code, start_date, end_date):
        """获取有效的日期范围

        Args:
            conn: 数据库连接
            code: 股票代码
            start_date: 用户指定的开始日期，可以为None
            end_date: 用户指定的结束日期，可以为None

        Returns:
            tuple: (start_date, end_date) 有效的日期范围，如果没有有效范围则返回 (None, None)
        """
        # 快速检查股票是否存在于数据库
        with conn.cursor() as cursor:
            cursor.execute("SELECT EXISTS(SELECT 1 FROM baostock_daily_history WHERE code = %s LIMIT 1)", (code,))
            stock_exists = cursor.fetchone()[0]

        # 设置结束日期
        end = end_date or datetime.now().date()

        if stock_exists:
            # 如果股票存在于数据库，获取最新日期
            latest_date = DatabaseOperations.get_latest_date(conn, code)
            if latest_date:
                # 从最新日期的下一天开始
                start = latest_date + timedelta(days=1)
                logger.info(f"股票 {code} 已存在于数据库，最新日期为 {latest_date}，开始日期设置为 {start}")

                # 检查是否需要更新
                if start > end:
                    logger.info(f"股票 {code} 的数据已是最新的，无需更新。")
                    return None, None
                return start, end

        # 如果股票不存在于数据库或没有最新日期
        if start_date:
            # 使用用户指定的开始日期
            start = start_date
            logger.info(f"股票 {code} 使用用户指定的开始日期: {start}")
        else:
            # 默认从2005年开始
            start = datetime(2005, 1, 1).date()
            logger.info(f"股票 {code} 在数据库中没有数据，开始日期设置为 {start}")

        logger.info(f"股票 {code} 结束日期设置为 {end}")
        return start, end








class BaoStockAPI:
    """BaoStock API 接口封装（优化版）"""

    @staticmethod
    @backoff.on_exception(backoff.expo, (Exception, RuntimeError), **RETRY_CONFIG['stock_data'])
    def get_stock_list():
        """获取股票列表"""
        logger.info("获取股票列表")
        try:
            rs = bs.query_stock_basic()
            if rs.error_code != '0':
                logger.error(f"获取股票列表失败: {rs.error_msg}")
                return pd.DataFrame()

            data_list = []
            while (rs.next()):
                row = rs.get_row_data()
                if row:
                    # 将每一行转换为字符串列表，避免 numpy 类型问题
                    data_list.append([str(x) if x is not None else None for x in row])

            if not data_list:
                logger.warning("获取到的股票列表为空")
                return pd.DataFrame()

            # 获取列名并转换为字符串
            columns = [str(c) for c in rs.fields]

            # 使用字典方式创建 DataFrame，避免 pandas 内部的 numpy 转换问题
            data_dict = {col: [row[i] for row in data_list] for i, col in enumerate(columns)}
            df = pd.DataFrame(data_dict)
            logger.info(f"获取到 {len(df)} 只股票")
            return df
        except Exception as e:
            logger.error(f"获取股票列表异常: {e}")
            logger.error(traceback.format_exc())
            raise

    @staticmethod
    @backoff.on_exception(backoff.expo, (Exception, RuntimeError), **RETRY_CONFIG['stock_data'])
    def get_daily_data(code, name, ipo_date, out_date, type, status, start_date, end_date):
        """一次性获取整个日期范围的日线数据"""
        fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"

        # 计算日期范围的总天数
        total_days = (end_date - start_date).days + 1

        if total_days <= 0:
            logger.info(f"股票 {code} 的数据已是最新的，无需更新。开始日期 {start_date} > 结束日期 {end_date}")
            return pd.DataFrame()

        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                logger.info(f"一次性获取 {code} 从 {start_date} 到 {end_date} 的所有数据")

                # 调用 BaoStock API
                rs = bs.query_history_k_data_plus(
                    code=code,
                    fields=fields,
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d'),
                    # 数据类型，默认为d，日k线；d=日k线、w=周、m=月、5=5分钟、15=15分钟、30=30分钟、60=60分钟k线数据，不区分大小写；指数没有分钟线数据；周线每周最后一个交易日才可以获取，月线每月最后一个交易日才可以获取。
                    frequency="d",
                    # 复权类型，默认不复权：3；1：后复权；2：前复权。已支持分钟线、日线、周线、月线前后复权。 BaoStock提供的是涨跌幅复权算法复权因子，具体介绍见：复权因子简介或者BaoStock复权因子简介。
                    adjustflag="1"
                )

                if rs.error_code != '0':
                    logger.error(f"{code} 请求失败: {rs.error_msg}")
                    retry_count += 1
                    time.sleep(1 * retry_count)
                    continue

                # 处理结果
                data_list = []
                while (rs.next()):
                    row = rs.get_row_data()
                    # 将每一行转换为字符串列表，避免 numpy 类型问题
                    data_list.append([str(x) if x is not None else None for x in row])

                # 转换为 DataFrame
                if data_list:
                    columns = [str(c) for c in rs.fields]
                    # 使用字典方式创建 DataFrame
                    data_dict = {col: [row[i] for row in data_list] for i, col in enumerate(columns)}
                    df = pd.DataFrame(data_dict)
                    # 添加股票名称列
                    df['name'] = name
                    df['ipo_date'] = ipo_date
                    df['out_date'] = out_date
                    df['type'] = type
                    df['status'] = status
                    logger.info(f"成功获取 {code} 从 {start_date} 到 {end_date} 的 {len(data_list)} 条数据")
                    return df
                else:
                    logger.info(f"{code} 在指定日期范围内没有数据")
                    # 返回包含name列的空DataFrame
                    empty_columns = [str(c) for c in rs.fields] + ['name', 'ipo_date', 'out_date', 'type', 'status']
                    empty_df = pd.DataFrame(columns=empty_columns, dtype=object)
                    return empty_df

            except Exception as e:
                logger.error(f"API调用出错: {e}")
                retry_count += 1
                time.sleep(1 * retry_count)

        # 如果所有重试都失败，返回空 DataFrame
        logger.error(f"{code} 获取数据失败，超过最大重试次数")
        return pd.DataFrame()





def main(args):
    """简化版主流程"""
    try:

        # 判断是否为交易日
        if not is_trading_day(datetime.now()):
            logger.error("非交易日，程序退出")
            return

        # 初始化
        DatabaseManager.check_connection()

        with DatabaseManager.get_connection() as conn:
            DatabaseOperations.init_schema(conn)

            with BaoStockManager.session():
                # 获取股票列表
                if args.stock_codes:
                    stock_codes = args.stock_codes
                    # 当指定了特定股票代码时，需要获取这些股票的详细信息
                    stock_list = BaoStockAPI.get_stock_list()
                    # 筛选出指定的股票
                    stock_list = stock_list[stock_list['code'].isin(args.stock_codes)]
                    stock_codes = stock_list['code'].tolist()
                else:
                    stock_list = BaoStockAPI.get_stock_list()
                    stock_codes = stock_list['code'].tolist()

                # 记录开始时间
                start_time = time.time()
                total_records = 0

                logger.info(f"开始处理 {len(stock_codes)} 只股票")

                # 顺序处理每只股票
                for i, row in stock_list.iterrows():
                    code = row['code']
                    name = row.get('code_name', '')  # 安全获取股票名称
                    ipo_date = row.get('ipoDate', '')
                    out_date = row.get('outDate', '')
                    type = row.get('type', '')
                    status = row.get('status', '')
                    try:
                        logger.info(f"[{i}/{len(stock_list)}] 处理股票 {code}")

                        # 获取有效的日期范围
                        start, end = DateManager.get_date_range(conn, code, args.start_date, args.end_date)
                        if not start:
                            logger.info(f"股票 {code} 无需获取新数据，跳过")
                            continue

                        # 获取日线数据
                        logger.info(f"获取股票 {code} 的日线数据，日期范围: {start} 至 {end}")
                        data = BaoStockAPI.get_daily_data(code, name, ipo_date, out_date, type, status, start, end)

                        if data.empty:
                            logger.info(f"股票 {code} 在指定日期范围内没有数据")
                            continue

                        # 将数据插入数据库
                        logger.info(f"将股票 {code} 的 {len(data)} 条数据插入数据库")
                        inserted = DatabaseOperations.bulk_insert(conn, data, PERF_CONFIG['batch_size'])
                        logger.info(f"股票 {code} 成功插入 {inserted} 条数据")
                        total_records += inserted

                    except Exception as e:
                        logger.error(f"处理股票 {code} 时出错: {e}")
                        continue

                elapsed = time.time() - start_time
                logger.info(f"处理完成! 共插入 {total_records} 条数据, 耗时 {elapsed:.2f} 秒")

    except Exception as e:
        logger.error(f"主流程异常: {e}")
        sys.exit(1)
    finally:
        if BaoStockManager._is_logged_in:
            bs.logout()


def is_trading_day(date):
    """
    判断指定日期是否为交易日

    Args:
        date (str or datetime): 日期，格式为'YYYY-MM-DD'

    Returns:
        bool: True表示是交易日，False表示非交易日
    """
    # 初始化baostock连接
    lg = bs.login()
    if lg.error_code != '0':
        logger.warning("baostock未正确初始化，无法判断交易日")
        return True  # 出错时默认返回True以保证程序继续运行

    # 格式化日期
    if isinstance(date, datetime):
        date_str = date.strftime('%Y-%m-%d')
    else:
        date_str = str(date)

    # 查询交易日
    rs = bs.query_trade_dates(start_date=date_str, end_date=date_str)

    # 处理查询结果
    if rs.error_code != '0':
        logger.warning(f"查询交易日失败: {rs.error_msg}")
        return True  # 出错时默认返回True

    # 获取查询结果
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())

    if not data_list:
        logger.warning(f"未获取到交易日数据: {date_str}")
        return True  # 出错时默认返回True

    # 判断是否为交易日
    is_trading = data_list[0][1] == '1'  # 第二列是is_trading_day字段，'1'表示交易日
    return is_trading



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="从 BaoStock 获取股票历史数据并保存到 PostgreSQL 数据库")
    parser.add_argument('--start_date', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                        help='开始日期，格式：YYYY-MM-DD，默认为数据库中最新日期的下一天')
    parser.add_argument('--end_date', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                        help='结束日期，格式：YYYY-MM-DD，默认为当前日期')
    parser.add_argument('--stock_codes', nargs='+', help='指定股票代码列表，如果不指定则获取所有股票')

    args = parser.parse_args()

    try:
        main(args)
    except KeyboardInterrupt:
        logger.info("用户中断执行")
        sys.exit(0)
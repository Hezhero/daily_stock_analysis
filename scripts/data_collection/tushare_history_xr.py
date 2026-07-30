import argparse
import io
import json as _json
import logging
import os
import queue
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta

import backoff
import pandas as pd
import psycopg2
import psycopg2.pool
import psycopg2.sql as sql
import requests
import warnings
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 抑制 numpy/pandas 警告
warnings.filterwarnings('ignore')

# 日志配置
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'result')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'tushare_history_xr.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file)
    ]
)
logger = logging.getLogger(__name__)

# ── PostgreSQL 数据库连接配置 ──
postgresql_config = {
    'host': os.environ.get('PG_HOST', '127.0.0.1'),
    'user': os.environ.get('PG_USER', 'root'),
    'password': os.environ.get('PG_PASSWORD'),
    'database': os.environ.get('PG_DATABASE', 'baostock'),
    'port': os.environ.get('PG_PORT', '5431')
}

# ── 重试配置 ──
RETRY_CONFIG = {
    'db_connection': {'max_tries': 5, 'base': 2, 'factor': 1, 'max_value': 30},
    'stock_data': {'max_tries': 3, 'base': 2, 'factor': 2, 'max_value': 30}
}

# ── 性能配置 ──
PERF_CONFIG = {
    'batch_size': 5000,
    'commit_size': 10000,
    'max_workers': 10
}

# ── Tushare 配置 ──
TUSHARE_TOKEN = os.environ.get('TUSHARE_TOKEN', '')
TUSHARE_API_URL = os.environ.get('TUSHARE_API_URL', 'http://api.tushare.pro')
# Tushare 每分钟请求次数限制（可通过环境变量覆盖）
TUSHARE_RATE_LIMIT = int(os.environ.get('TUSHARE_RATE_LIMIT', '200'))
# 速率限制触发时最大等待间隔（秒）
TUSHARE_RATE_MAX_WAIT = int(os.environ.get('TUSHARE_RATE_MAX_WAIT', '10'))

# ── 目标表名 ──
TABLE_NAME = 'baostock_daily_history_xr'


class TushareHttpClient:
    """轻量级 Tushare Pro HTTP 客户端，不依赖 tushare SDK。

    参照 data_provider/tushare_fetcher.py 中的 _TushareHttpClient 实现。
    """

    def __init__(self, token: str, timeout: int = 30):
        self._token = token
        self._timeout = timeout
        self._api_url = TUSHARE_API_URL

    def query(self, api_name: str, fields: str = "", **kwargs) -> pd.DataFrame:
        """调用 Tushare Pro HTTP API。

        Args:
            api_name: 接口名称（如 daily / stock_basic）
            fields: 逗号分隔的字段列表
            **kwargs: 接口参数

        Returns:
            DataFrame，失败抛出异常
        """
        req_params = {
            "api_name": api_name,
            "token": self._token,
            "params": kwargs,
            "fields": fields,
        }
        res = requests.post(self._api_url, json=req_params, timeout=self._timeout)
        if res.status_code != 200:
            raise RuntimeError(f"Tushare API HTTP {res.status_code}")

        result = _json.loads(res.text)
        if result.get("code") != 0:
            raise RuntimeError(
                result.get("msg") or f"Tushare API error code {result.get('code')}"
            )

        data = result.get("data") or {}
        columns = data.get("fields") or []
        items = data.get("items") or []
        return pd.DataFrame(items, columns=columns)


class RateLimiter:
    """Tushare API 速率限制器。

    默认配额：300 次/分钟（可通过 TUSHARE_RATE_LIMIT 环境变量配置）。
    速率触发时最大等待间隔：10 秒（可通过 TUSHARE_RATE_MAX_WAIT 配置）。
    """

    def __init__(
        self,
        max_calls_per_minute: int = TUSHARE_RATE_LIMIT,
        max_wait_sec: int = TUSHARE_RATE_MAX_WAIT,
    ):
        self._max_calls = max_calls_per_minute
        self._max_wait = max_wait_sec
        self._call_count = 0
        self._minute_start: float | None = None
        self._lock = threading.Lock()

    def acquire(self):
        """获取一次调用许可，均匀间隔请求以避免触发服务端限流。

        策略：
        - 300 次/分钟 = 每 0.2 秒一次调用，多线程通过全局锁串行化。
        - 若本地计数器已达上限，等待到下一分钟窗口（最长 max_wait 秒）。
        """
        with self._lock:
            now = time.time()
            if self._minute_start is None or now - self._minute_start >= 60:
                self._minute_start = now
                self._call_count = 0

            # 当本地计数器达到上限时，等待下一个 60 秒窗口
            if self._call_count >= self._max_calls:
                need_wait = max(0, 60 - (now - self._minute_start))
                sleep_sec = min(need_wait, self._max_wait)
                logger.warning(
                    f"Tushare 速率限制触发 ({self._call_count}/{self._max_calls})，"
                    f"等待 {sleep_sec:.1f} 秒（上限 {self._max_wait} 秒）…"
                )
                time.sleep(sleep_sec)
                self._minute_start = time.time()
                self._call_count = 0

            # 最小间隔：将 300 次均匀分布在 60 秒内
            min_interval = 60.0 / self._max_calls
            if self._call_count > 0:
                expected_next = self._minute_start + (self._call_count * min_interval)
                if now < expected_next:
                    wait = expected_next - now
                    if wait > 0:
                        time.sleep(wait)

            self._call_count += 1


# 全局速率限制器（所有线程共享）
_rate_limiter = RateLimiter()


def _ts_code_to_baostock(ts_code: str) -> str:
    """将 Tushare ts_code 格式转为 BaoStock 格式。

    Examples:
        '600519.SH'  -> 'sh.600519'
        '000001.SZ'  -> 'sz.000001'
        '688xxx.SH'  -> 'sh.688xxx'
        '8xxxxx.BJ'  -> 'bj.8xxxxx'
    """
    ts_code = ts_code.strip().upper()
    if '.' in ts_code:
        code, exchange = ts_code.split('.', 1)
        exchange = exchange.lower()
        if exchange in ('sh', 'ss'):
            return f"sh.{code}"
        elif exchange == 'sz':
            return f"sz.{code}"
        elif exchange == 'bj':
            return f"bj.{code}"
        else:
            return f"{exchange}.{code}"
    return ts_code.lower()


class DatabaseManager:
    """数据库连接管理器（支持连接池）。"""

    _pool = None

    @classmethod
    def init_pool(cls, min_connections=1, max_connections=20):
        if cls._pool is None:
            cls._pool = psycopg2.pool.ThreadedConnectionPool(
                min_connections,
                max_connections,
                **postgresql_config
            )
            logger.info(f"数据库连接池初始化成功 (min={min_connections}, max={max_connections})")

    @classmethod
    @contextmanager
    def get_connection(cls):
        if cls._pool is None:
            cls.init_pool()

        conn = None
        try:
            conn = cls._pool.getconn()
            yield conn
        except psycopg2.Error as e:
            logger.error(f"数据库连接失败: {e}")
            raise
        finally:
            if conn:
                cls._pool.putconn(conn)

    @classmethod
    def close_pool(cls):
        if cls._pool:
            try:
                cls._pool.closeall()
                logger.debug("数据库连接池已关闭")
            except Exception as e:
                logger.warning(f"关闭连接池时出错: {e}")
            finally:
                cls._pool = None

    @staticmethod
    @backoff.on_exception(backoff.expo, Exception, **RETRY_CONFIG['db_connection'])
    def check_connection():
        with DatabaseManager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                if cursor.fetchone()[0] != 1:
                    raise RuntimeError("数据库连接测试失败")


class DataFormatter:
    """数据格式化工具类。"""

    @staticmethod
    def clean_data(df):
        if df.empty:
            return df

        df = df.replace('', None)

        numeric_cols = ['open', 'high', 'low', 'close', 'pre_close', 'volume',
                        'amount', 'pct_chg']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date

        return df


class DatabaseOperations:
    """数据库操作类。

    与 baostock_history_xr.py 共用同一张表 baostock_daily_history_xr，
    使用 ON CONFLICT (code, date) DO NOTHING 避免覆盖已有数据。
    """

    @staticmethod
    def init_schema(conn):
        with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME}
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

            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_code
                    ON {TABLE_NAME} (code)
            """)

            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_date
                    ON {TABLE_NAME} (date)
            """)

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
            ("turn", "换手率"),
            ("trade_status", "交易状态(1：正常交易 0：停牌)"),
            ("pct_chg", "涨跌幅 (百分比)"),
            ("pe_ttm", "滚动市盈率"),
            ("pb_mrq", "市净率"),
            ("ps_ttm", "滚动市销率"),
            ("pcf_ncf_ttm", "滚动市现率"),
            ("is_st", "是否ST股，1是，0否"),
            ("ipo_date", "上市日期"),
            ("out_date", "退市日期"),
            ("type", "证券类型，其中1：股票，2：指数，3：其它，4：可转债，5：ETF"),
            ("status", "上市状态，其中1：上市，0：退市"),
            ("insert_time", "数据插入时间")
        ]
        try:
            for column, comment in column_comments:
                comment_query = sql.SQL(
                    f"COMMENT ON COLUMN {TABLE_NAME}.{{}} IS %s"
                ).format(sql.Identifier(column))
                client.execute(comment_query, [comment])
            logger.info("列注释添加成功")
        except Exception as e:
            logger.error(f"添加列注释时出错: {e}")

    @staticmethod
    def get_latest_date(conn, code):
        try:
            with conn.cursor() as cursor:
                query = f"""
                    SELECT MAX(date)
                    FROM {TABLE_NAME}
                    WHERE code = %s
                """
                cursor.execute(query, (code,))
                result = cursor.fetchone()[0]
                return result
        except Exception as e:
            logger.error(f"获取{code}最新交易日期时出错: {e}")
            return None

    @staticmethod
    def bulk_insert(conn, data, batch_size):
        if data is None or (isinstance(data, pd.DataFrame) and data.empty):
            return 0

        df = pd.DataFrame(data)
        df = DataFormatter.clean_data(df)

        if df.empty:
            return 0

        # Tushare API 字段 → 数据库列名映射
        column_mapping = {
            'code': 'code',
            'name': 'name',
            'date': 'date',
            'open': 'open',
            'close': 'close',
            'high': 'high',
            'low': 'low',
            'pre_close': 'pre_close',
            'volume': 'volume',
            'amount': 'amount',
            'adjust_flag': 'adjust_flag',
            'pct_chg': 'pct_chg',
            'is_st': 'is_st',
            'ipo_date': 'ipo_date',
            'out_date': 'out_date',
            'type': 'type',
            'status': 'status',
        }

        df = df.rename(columns=column_mapping)

        expected_columns = [
            'code', 'name', 'date', 'open', 'close', 'high', 'low', 'pre_close',
            'volume', 'amount', 'adjust_flag', 'turn', 'trade_status', 'pct_chg',
            'pe_ttm', 'pb_mrq', 'ps_ttm', 'pcf_ncf_ttm', 'is_st', 'ipo_date',
            'out_date', 'type', 'status'
        ]

        # 仅保留存在的列，缺失列在 INSERT 时设为 DEFAULT / NULL
        existing_cols = [c for c in expected_columns if c in df.columns]
        df = df.reindex(columns=existing_cols)

        temp_table_name = f"temp_tushare_data_{int(time.time())}"

        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table_name} (
                        LIKE {TABLE_NAME} INCLUDING ALL
                    ) ON COMMIT DROP
                """)

                buffer = io.StringIO()
                df.to_csv(buffer, sep='\t', na_rep='nan', header=False, index=False)
                buffer.seek(0)

                cursor.copy_from(
                    buffer,
                    temp_table_name,
                    sep='\t',
                    null='nan',
                    columns=existing_cols
                )

                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME} ({', '.join(existing_cols)})
                    SELECT {', '.join(existing_cols)} FROM {temp_table_name}
                    ON CONFLICT (code, date) DO NOTHING
                    RETURNING code
                """)

                inserted_rows = cursor.rowcount
                conn.commit()
                return inserted_rows

        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"批量插入失败: {e}")
            raise


class DateManager:
    """日期管理类。"""

    @staticmethod
    def batch_get_latest_dates(conn, codes):
        if not codes:
            return {}

        placeholders = ','.join(['%s'] * len(codes))
        query = f"""
            SELECT code, MAX(date) as latest_date
            FROM {TABLE_NAME}
            WHERE code IN ({placeholders})
            GROUP BY code
        """
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, tuple(codes))
                return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"批量获取股票最新日期失败: {e}")
            return {}

    @staticmethod
    def get_date_range_fast(conn, code, start_date, end_date, stock_latest_dates=None):
        end = end_date or datetime.now().date()

        if stock_latest_dates and code in stock_latest_dates:
            latest_date = stock_latest_dates[code]
            if latest_date:
                start = latest_date + timedelta(days=1)
                if start > end:
                    return None, None
                return start, end

        return DateManager.get_date_range(conn, code, start_date, end_date)

    @staticmethod
    def get_date_range(conn, code, start_date, end_date):
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT EXISTS(SELECT 1 FROM {TABLE_NAME} WHERE code = %s LIMIT 1)",
                (code,)
            )
            stock_exists = cursor.fetchone()[0]

        end = end_date or datetime.now().date()

        if stock_exists:
            latest_date = DatabaseOperations.get_latest_date(conn, code)
            if latest_date:
                start = latest_date + timedelta(days=1)
                logger.info(f"股票 {code} 已存在于数据库，最新日期为 {latest_date}，开始日期设置为 {start}")
                if start > end:
                    logger.info(f"股票 {code} 的数据已是最新的，无需更新。")
                    return None, None
                return start, end

        if start_date:
            start = start_date
            logger.info(f"股票 {code} 使用用户指定的开始日期: {start}")
        else:
            start = datetime(2005, 1, 1).date()
            logger.info(f"股票 {code} 在数据库中没有数据，开始日期设置为 {start}")

        logger.info(f"股票 {code} 结束日期设置为 {end}")
        return start, end


class TushareAPI:
    """Tushare Pro API 接口封装。

    提供：
    - get_stock_list(): 从 stock_basic 获取 A 股列表
    - get_daily_data(): 从 daily 接口获取日线数据
    """

    _api_client: TushareHttpClient | None = None

    @classmethod
    def _get_client(cls) -> TushareHttpClient:
        if cls._api_client is None:
            if not TUSHARE_TOKEN:
                raise RuntimeError(
                    "TUSHARE_TOKEN 未配置，请在 .env 中设置 TUSHARE_TOKEN=你的token"
                )
            cls._api_client = TushareHttpClient(TUSHARE_TOKEN)
        return cls._api_client

    @staticmethod
    @backoff.on_exception(
        backoff.expo,
        (RuntimeError, requests.RequestException),
        **RETRY_CONFIG['stock_data']
    )
    def get_stock_list():
        """从 Tushare stock_basic 获取 A 股股票列表。

        Returns:
            DataFrame，包含字段：
            ts_code, symbol, name, area, industry, market, exchange,
            list_status, list_date, delist_date, is_hs
        """
        logger.info("从 Tushare 获取股票列表…")
        _rate_limiter.acquire()
        client = TushareAPI._get_client()

        fields = (
            "ts_code,symbol,name,area,industry,market,exchange,"
            "list_status,list_date,delist_date,is_hs"
        )

        df = client.query(
            "stock_basic",
            exchange='',
            list_status='L',
            fields=fields
        )

        if df.empty:
            logger.warning("Tushare stock_basic 返回空列表")
            return pd.DataFrame()

        logger.info(f"Tushare 获取到 {len(df)} 只股票")
        return df

    @staticmethod
    def get_daily_data(
        ts_code: str,
        name: str,
        ipo_date: str,
        out_date: str,
        stock_type: str,
        stock_status: str,
        is_st: str,
        start_date,
        end_date,
    ) -> pd.DataFrame:
        """通过 Tushare daily 接口获取日线数据。

        Tushare daily API (doc_id=27) 返回字段：
        ts_code, trade_date, open, high, low, close, pre_close,
        change, pct_chg, vol, amount

        Args:
            ts_code: Tushare 格式代码（如 600519.SH）
            name: 股票名称
            ipo_date: 上市日期
            out_date: 退市日期
            stock_type: 证券类型
            stock_status: 上市状态
            is_st: 是否 ST
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame，已清洗且包含所有表字段
        """
        total_days = (end_date - start_date).days + 1
        if total_days <= 0:
            logger.debug(f"股票 {ts_code} 日期范围无效，跳过")
            return pd.DataFrame()

        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                _rate_limiter.acquire()
                client = TushareAPI._get_client()

                ts_start = start_date.strftime('%Y%m%d')
                ts_end = end_date.strftime('%Y%m%d')

                logger.debug(
                    f"Tushare daily({ts_code}, {ts_start}–{ts_end})"
                )

                df = client.query(
                    "daily",
                    ts_code=ts_code,
                    start_date=ts_start,
                    end_date=ts_end,
                    fields=(
                        "ts_code,trade_date,open,high,low,close,"
                        "pre_close,change,pct_chg,vol,amount"
                    )
                )

                if df.empty:
                    logger.debug(f"股票 {ts_code} 在指定日期范围内没有数据")
                    return pd.DataFrame()

                # ── 字段转换 ──
                # 将 Tushare ts_code 转为 BaoStock 格式（如 600519.SH → sh.600519）
                df['code'] = df['ts_code'].apply(_ts_code_to_baostock)

                # 日期：YYYYMMDD → date
                df['date'] = pd.to_datetime(
                    df['trade_date'], format='%Y%m%d', errors='coerce'
                ).dt.date

                # 成交量：手 → 股（×100）
                df['volume'] = pd.to_numeric(df['vol'], errors='coerce') * 100

                # 成交额：千元 → 元（×1000）
                df['amount'] = pd.to_numeric(df['amount'], errors='coerce') * 1000

                # 前收盘价
                df['pre_close'] = pd.to_numeric(df['pre_close'], errors='coerce')

                # 涨跌幅（已经是百分比形式）
                df['pct_chg'] = pd.to_numeric(df['pct_chg'], errors='coerce')

                # OHLC
                for col in ('open', 'high', 'low', 'close'):
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                # ── 补充静态字段 ──
                df['name'] = name
                df['adjust_flag'] = '3'  # Tushare daily 默认输出前复权数据 此处获取除权数据
                # 以下字段 Tushare daily 不提供，留空
                # (turn, trade_status, pe_ttm, pb_mrq, ps_ttm, pcf_ncf_ttm)
                df['is_st'] = is_st
                if ipo_date:
                    df['ipo_date'] = pd.to_datetime(ipo_date, format='%Y%m%d',
                                                     errors='coerce').date()
                else:
                    df['ipo_date'] = None
                if out_date:
                    df['out_date'] = pd.to_datetime(out_date, format='%Y%m%d',
                                                     errors='coerce').date()
                else:
                    df['out_date'] = None
                df['type'] = stock_type
                df['status'] = stock_status

                logger.info(
                    f"Tushare 获取 {ts_code} 从 {start_date} 到 {end_date} "
                    f"的 {len(df)} 条日线数据"
                )
                return df

            except Exception as e:
                error_msg = str(e)
                is_rate_limit = '频率超限' in error_msg or '频率' in error_msg

                if is_rate_limit:
                    # Tushare 服务端限流：等待 60 秒让计数器重置后再重试
                    logger.warning(
                        f"Tushare daily 服务端限流 ({ts_code})，等待 60 秒后重试…"
                    )
                    retry_count += 1
                    if retry_count >= max_retries:
                        raise RuntimeError(
                            f"{ts_code} Tushare daily 获取数据失败（限流重试耗尽）: {e}"
                        ) from e
                    time.sleep(60)
                    # 重置本地计数器以与服务端同步
                    with _rate_limiter._lock:
                        _rate_limiter._minute_start = time.time()
                        _rate_limiter._call_count = 0
                else:
                    logger.error(f"Tushare daily 调用失败 ({ts_code}): {e}")
                    retry_count += 1
                    if retry_count >= max_retries:
                        raise RuntimeError(
                            f"{ts_code} Tushare daily 获取数据失败，重试耗尽: {e}"
                        ) from e
                    time.sleep(2 ** retry_count)

        raise RuntimeError(f"{ts_code} Tushare daily 获取数据失败，重试耗尽")


def _derive_type(row) -> str:
    """从 Tushare stock_basic 行推导证券类型。

    Tushare stock_basic.market 取值：
    主板 / 创业板 / 科创板 / CDR / 北交所

    返回：
        1：股票，2：指数，3：其它，4：可转债，5：ETF
    """
    # Tushare stock_basic 只返回股票，无指数/可转债/ETF
    # 统一标记为股票类型
    return '1'


def _derive_status(row) -> str:
    """从 Tushare stock_basic 行推导上市状态。

    list_status: L=上市, D=退市, P=暂停上市
    """
    ls = str(row.get('list_status', '')).strip().upper()
    if ls == 'L':
        return '1'
    elif ls == 'D':
        return '0'
    else:
        return '1'  # 默认上市


def _derive_is_st(row) -> str:
    """从 Tushare stock_basic 行推导是否 ST。

    is_hs 表示是否沪深港通标的，不直接表示 ST。
    Tushare stock_basic 没有直接的 is_st 字段。
    这里先留空，后续可通过 name_st 接口补充。
    """
    return None


def process_single_stock(args, row, stock_latest_dates=None):
    """处理单只股票的完整流程（由 worker 调用）。

    Args:
        args: 命令行参数
        row: Tushare stock_basic 返回的股票信息行
        stock_latest_dates: 预查询的股票最新日期字典

    Returns:
        tuple: (code, inserted_count, error_msg)
    """
    ts_code = row['ts_code']
    # 转为 BaoStock 格式用于数据库查询
    code = _ts_code_to_baostock(ts_code)
    name = row.get('name', '')
    ipo_date = row.get('list_date', '')
    out_date = row.get('delist_date', '')
    stock_type = _derive_type(row)
    stock_status = _derive_status(row)
    is_st = _derive_is_st(row)

    try:
        with DatabaseManager.get_connection() as conn:
            start, end = DateManager.get_date_range_fast(
                conn, code, args.start_date, args.end_date, stock_latest_dates
            )
            if not start:
                logger.debug(f"股票 {code} ({name}) 无需获取新数据，跳过")
                return code, 0, None

        logger.debug(f"处理股票 {code} ({name})")
        data = TushareAPI.get_daily_data(
            ts_code, name, ipo_date, out_date,
            stock_type, stock_status, is_st,
            start, end
        )

        if data.empty:
            logger.debug(f"股票 {code} ({name}) 在指定日期范围内没有数据")
            return code, 0, None

        with DatabaseManager.get_connection() as conn:
            inserted = DatabaseOperations.bulk_insert(
                conn, data, PERF_CONFIG['batch_size']
            )
            logger.debug(f"股票 {code} ({name}) 成功插入 {inserted} 条数据")
            return code, inserted, None

    except Exception as e:
        logger.error(f"处理股票 {code} ({name}) 时出错: {e}")
        return code, 0, str(e)


_WORKER_SENTINEL = object()


def worker_process_stocks(worker_id, args, task_queue, result_queue, stock_latest_dates=None):
    """worker 线程：串行消费股票任务。"""
    logger.debug(f"Worker {worker_id} 已启动")
    try:
        while True:
            row = task_queue.get()
            try:
                if row is _WORKER_SENTINEL:
                    logger.debug(f"Worker {worker_id} 收到停止信号")
                    return
                result = process_single_stock(args, row, stock_latest_dates)
                result_queue.put(result)
            finally:
                task_queue.task_done()
    except Exception as e:
        logger.error(f"Worker {worker_id} 发生错误: {e}")
    finally:
        logger.debug(f"Worker {worker_id} 已退出")


def run_stock_workers(args, stock_list, stock_latest_dates=None):
    """启动 worker 线程处理股票列表。"""
    task_queue = queue.Queue()
    result_queue = queue.Queue()

    for _, row in stock_list.iterrows():
        task_queue.put(row)

    worker_count = min(args.max_workers, len(stock_list)) if len(stock_list) else 0
    for _ in range(worker_count):
        task_queue.put(_WORKER_SENTINEL)

    workers = []
    for worker_id in range(worker_count):
        worker = threading.Thread(
            target=worker_process_stocks,
            args=(worker_id, args, task_queue, result_queue, stock_latest_dates),
            name=f"tushare-worker-{worker_id}",
        )
        worker.start()
        workers.append(worker)

    # 等待所有任务完成
    all_tasks_done = False
    wait_start = time.time()
    max_wait_time = 3600

    while not all_tasks_done and time.time() - wait_start < max_wait_time:
        try:
            task_queue.join(timeout=5)
            all_tasks_done = True
        except Exception:
            pass

    if not all_tasks_done:
        logger.warning("任务队列未能在1小时内完成，强制继续")

    for worker in workers:
        worker.join(timeout=60)
        if worker.is_alive():
            logger.warning(f"Worker {worker.name} 未能在60秒内结束")

    results = []
    try:
        while True:
            results.append(result_queue.get(timeout=5))
    except queue.Empty:
        pass

    return results


def main(args):
    """主流程。"""
    try:
        DatabaseManager.check_connection()

        max_workers = args.max_workers
        DatabaseManager.init_pool(min_connections=1, max_connections=max_workers + 5)

        with DatabaseManager.get_connection() as conn:
            DatabaseOperations.init_schema(conn)

        # 获取股票列表
        if args.stock_codes:
            stock_list = TushareAPI.get_stock_list()
            # 支持 Tushare ts_code 格式（如 600519.SH）和 BaoStock 格式（如 sh.600519）
            ts_codes_to_match = set()
            for sc in args.stock_codes:
                sc_upper = sc.strip().upper()
                if '.' in sc_upper:
                    ts_codes_to_match.add(sc_upper)
                else:
                    # 纯数字代码，匹配所有后缀
                    ts_codes_to_match.add(sc_upper)
            stock_list = stock_list[
                stock_list['ts_code'].apply(
                    lambda x: any(
                        x.upper() == m or x.split('.')[0] == m
                        for m in ts_codes_to_match
                    )
                )
            ]
        else:
            stock_list = TushareAPI.get_stock_list()

        if stock_list.empty:
            logger.warning("未获取到任何股票，退出")
            return

        # 统一转为 BaoStock 格式 code 用于数据库查询
        stock_list['_db_code'] = stock_list['ts_code'].apply(_ts_code_to_baostock)
        stock_codes = stock_list['_db_code'].tolist()

        logger.info(f"共 {len(stock_codes)} 只股票待处理")

        with DatabaseManager.get_connection() as conn:
            logger.info("批量预查询股票最新日期…")
            stock_latest_dates = DateManager.batch_get_latest_dates(conn, stock_codes)
            logger.info(f"预查询完成，{len(stock_latest_dates)} 只股票已有数据")

        start_time = time.time()
        total_records = 0
        completed_count = 0
        failed_stocks = []

        logger.info(f"开始处理 {len(stock_codes)} 只股票，并发数: {max_workers}")

        for code, inserted, error in run_stock_workers(
            args, stock_list, stock_latest_dates
        ):
            completed_count += 1
            if error:
                failed_stocks.append((code, error))
            else:
                total_records += inserted

            if completed_count % 100 == 0 or completed_count == len(stock_list):
                logger.info(
                    f"进度: {completed_count}/{len(stock_list)} "
                    f"({100 * completed_count / len(stock_list):.1f}%)"
                )

        elapsed = time.time() - start_time

        if failed_stocks:
            logger.warning(
                f"处理完成! 共插入 {total_records} 条数据, "
                f"耗时 {elapsed:.2f} 秒, 失败 {len(failed_stocks)} 只股票"
            )
            for code, err in failed_stocks[:10]:
                logger.warning(f"  失败股票: {code}, 错误: {err}")
            raise RuntimeError(f"共有 {len(failed_stocks)} 只股票处理失败")
        else:
            logger.info(
                f"处理完成! 共插入 {total_records} 条数据, "
                f"耗时 {elapsed:.2f} 秒"
            )

    except Exception as e:
        logger.error(f"主流程异常: {e}")
        sys.exit(1)
    finally:
        DatabaseManager.close_pool()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="从 Tushare Pro 获取 A 股日线数据并保存到 PostgreSQL 数据库 "
                    f"({TABLE_NAME} 表)"
    )
    parser.add_argument(
        '--start_date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help='开始日期，格式：YYYY-MM-DD，默认为数据库中最新日期的下一天'
    )
    parser.add_argument(
        '--end_date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help='结束日期，格式：YYYY-MM-DD，默认为当前日期'
    )
    parser.add_argument(
        '--stock_codes',
        nargs='+',
        help='指定股票代码列表（Tushare 格式如 600519.SH 000001.SZ，'
             '或纯数字如 600519 000001），不指定则获取全部 A 股'
    )
    parser.add_argument(
        '--max_workers',
        type=int,
        default=PERF_CONFIG['max_workers'],
        help=f'并发线程数，默认为{PERF_CONFIG["max_workers"]}'
    )

    args = parser.parse_args()

    try:
        main(args)
    except KeyboardInterrupt:
        logger.info("用户中断执行")
        sys.exit(0)

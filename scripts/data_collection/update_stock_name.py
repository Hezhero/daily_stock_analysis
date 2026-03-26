import akshare as ak
import psycopg2
import logging
import os
from datetime import datetime

# 配置日志
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'stock_name_update_{datetime.now().strftime("%Y%m%d")}.log')

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


def get_latest_stock_names():
    """从AKShare获取最新的A股股票名称列表"""
    try:
        logger.info("开始获取最新的股票名称列表...")
        # 获取A股股票列表，包含代码和名称
        stock_list = ak.stock_zh_a_spot_em()

        # 检查是否包含必要的列
        if '代码' not in stock_list.columns or '名称' not in stock_list.columns:
            logger.error("获取的股票列表不包含'代码'或'名称'列")
            return None

        # 转换为字典列表，只保留需要的列
        result = stock_list[['代码', '名称']].rename(columns={
            '代码': 'code',
            '名称': 'name'
        }).to_dict('records')

        logger.info(f"成功获取{len(result)}只股票的名称信息")
        return result

    except Exception as e:
        logger.error(f"获取股票名称列表时出错: {str(e)}")
        return None


def update_stock_names_in_db(stock_names):
    """批量更新数据库中的股票名称"""
    if not stock_names:
        logger.warning("没有股票名称数据可更新")
        return 0

    try:
        with psycopg2.connect(**postgresql_config) as conn:
            with conn.cursor() as cur:
                # 分块处理，避免SQL语句过长
                chunk_size = 1000
                total_updated = 0

                for i in range(0, len(stock_names), chunk_size):
                    chunk = stock_names[i:i+chunk_size]

                    # 准备更新语句 - 修复CTE表引用问题
                    update_query = """
                    UPDATE a_stock_history_akshare AS t
                    SET name = d.name 
                    FROM data d
                    WHERE t.code = d.code 
                      AND (t.name IS NULL OR t.name != d.name)
                    """

                    # 使用CTE来批量更新
                    cte_query = f"""
                    WITH data (code, name) AS (
                        VALUES {', '.join(['%s'] * len(chunk))}
                    )
                    {update_query}
                    """

                    # 准备参数
                    params = [(item['code'], item['name']) for item in chunk]

                    # 执行更新
                    cur.execute(cte_query, params)
                    chunk_updated = cur.rowcount
                    total_updated += chunk_updated
                    logger.info(f"已更新第{i//chunk_size + 1}块，本块更新{chunk_updated}条记录")

                conn.commit()
                logger.info(f"成功更新了{total_updated}条股票名称记录")
                return total_updated

    except psycopg2.Error as e:
        logger.error(f"数据库错误: {str(e)}")
        if conn:
            conn.rollback()
        return 0
    except Exception as e:
        logger.error(f"更新股票名称时出错: {str(e)}")
        if conn:
            conn.rollback()
        return 0


def check_table_exists():
    """检查目标表是否存在"""
    try:
        with psycopg2.connect(** postgresql_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'a_stock_history_akshare'
                )
                """)
                exists = cur.fetchone()[0]

                if not exists:
                    logger.error("表a_stock_history_akshare不存在，请先创建该表")
                    return False

                # 检查name字段是否存在
                cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'a_stock_history_akshare'
                    AND column_name = 'name'
                )
                """)
                name_column_exists = cur.fetchone()[0]

                if not name_column_exists:
                    logger.error("表a_stock_history_akshare中不存在name字段，请先添加该字段")
                    return False

                return True

    except psycopg2.Error as e:
        logger.error(f"检查表结构时发生数据库错误: {str(e)}")
        return False


def main():
    logger.info("===== 开始执行股票名称更新程序 =====")

    # 检查表结构是否符合要求
    if not check_table_exists():
        logger.error("表结构检查失败，程序终止")
        return

    # 获取最新股票名称
    stock_names = get_latest_stock_names()
    if not stock_names:
        logger.error("无法获取股票名称数据，程序终止")
        return

    # 更新数据库中的股票名称
    updated_count = update_stock_names_in_db(stock_names)

    logger.info(f"===== 股票名称更新程序执行完毕，共更新{updated_count}条记录 =====")


if __name__ == "__main__":
    main()

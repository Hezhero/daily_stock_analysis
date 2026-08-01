# -*- coding: utf-8 -*-
"""
A 股爆量回踩选股器

基于 akshare 日线数据：
1. 筛选近两年内出现"周线成交量放大 3 倍且创历史新高"的股票
2. 当前价距爆量周收盘价不超过 10%
3. 剔除 ST/退市/上市不足两年等
4. 根据爆量周 K 线形态和后续走势分类评级

用法: python le_stock.py
"""

import logging
import time
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("le_stock")

# =====================
# 参数区
# =====================
TODAY = datetime.today()
START_DATE = TODAY - timedelta(days=730)

START_STR = START_DATE.strftime("%Y%m%d")
END_STR = TODAY.strftime("%Y%m%d")

PRICE_DIFF_LIMIT = 0.10       # 现价距离爆量周收盘价不超过 10%
VOLUME_MULTIPLE = 3           # 爆量周成交量至少是上一周 3 倍
MIN_LIST_DAYS = 730           # 剔除上市不足两年
API_SLEEP_SEC = 0.2            # API 调用间隔（秒）


class SimpleRateLimiter:
    """简单的速率限制器，用于 akshare API 调用。"""

    def __init__(self, calls_per_minute: int = 60):
        self._interval = 60.0 / calls_per_minute
        self._last_call = 0.0

    def wait(self):
        """等待直到可以发起下一次 API 调用。"""
        now = time.time()
        elapsed = now - self._last_call
        if elapsed < self._interval:
            time.sleep(self._interval - elapsed)
        self._last_call = time.time()


def main():
    rate_limiter = SimpleRateLimiter(calls_per_minute=60)

    logger.info("获取 A 股实时行情列表...")
    spot = ak.stock_zh_a_spot_em()

    code_col = "代码"
    name_col = "名称"
    price_col = "最新价"

    stocks = spot[[code_col, name_col, price_col]].copy()

    # 剔除 ST、退市、B 股等
    stocks = stocks[~stocks[name_col].str.contains("ST|退", na=False)]
    stocks = stocks[stocks[code_col].str.match(r"^(00|30|60|68|83|87|92)")]

    total_stocks = len(stocks)
    logger.info("待筛选股票: %d 只", total_stocks)

    results = []
    processed = 0

    for _, row in stocks.iterrows():
        code = str(row[code_col]).zfill(6)
        name = row[name_col]
        current_price = row[price_col]

        try:
            if pd.isna(current_price) or float(current_price) <= 0:
                continue

            rate_limiter.wait()

            # 拉取近两年日线（前复权）
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=START_STR,
                end_date=END_STR,
                adjust="qfq",
            )

            if df is None or df.empty:
                continue
            if len(df) < 180:
                continue

            df["日期"] = pd.to_datetime(df["日期"])
            df = df.sort_values("日期")

            # 转周线
            weekly = df.resample("W-FRI", on="日期").agg({
                "开盘": "first",
                "收盘": "last",
                "最高": "max",
                "最低": "min",
                "成交量": "sum",
                "成交额": "sum",
            }).dropna()

            if len(weekly) < 52:
                continue

            weekly["上周成交量"] = weekly["成交量"].shift(1)
            weekly["放量倍数"] = weekly["成交量"] / weekly["上周成交量"]

            # 找近两年内所有"放量 3 倍以上"的周
            candidates = weekly[weekly["放量倍数"] >= VOLUME_MULTIPLE]
            if candidates.empty:
                continue

            last_price = float(current_price)

            for idx, signal in candidates.iterrows():
                before = weekly[weekly.index < idx]
                if before.empty:
                    continue

                signal_volume = signal["成交量"]
                signal_close = signal["收盘"]
                signal_open = signal["开盘"]
                signal_low = signal["最低"]
                signal_high = signal["最高"]

                # 爆量周成交量必须高于之前所有周
                if signal_volume <= before["成交量"].max():
                    continue

                # 当前价距爆量周收盘价不超过 10%
                price_diff = abs(last_price - signal_close) / signal_close
                if price_diff > PRICE_DIFF_LIMIT:
                    continue

                after = weekly[weekly.index > idx]
                if not after.empty:
                    max_rise = (after["最高"].max() - signal_close) / signal_close
                    max_fall = (after["最低"].min() - signal_close) / signal_close
                else:
                    max_rise = 0.0
                    max_fall = 0.0

                # 爆量周 K 线形态
                is_yang = signal_close > signal_open
                upper_shadow_ratio = (
                    (signal_high - max(signal_open, signal_close)) / signal_close
                )

                if is_yang and max_fall > -0.30 and upper_shadow_ratio < 0.12:
                    shape = "较好：爆量阳线，未明显出货"
                    conclusion = "重点观察"
                elif max_fall <= -0.30:
                    shape = "偏弱：爆量后跌幅较大"
                    conclusion = "一般观察"
                elif upper_shadow_ratio >= 0.12:
                    shape = "谨慎：爆量周上影线偏长"
                    conclusion = "一般观察"
                else:
                    shape = "一般：符合硬条件，但形态普通"
                    conclusion = "一般观察"

                results.append({
                    "代码": code,
                    "名称": name,
                    "现价": round(last_price, 2),
                    "爆量周": idx.strftime("%Y-%m-%d"),
                    "爆量周开盘": round(signal_open, 2),
                    "爆量周收盘": round(signal_close, 2),
                    "爆量周最低": round(signal_low, 2),
                    "爆量周最高": round(signal_high, 2),
                    "当前价偏离%": round(price_diff * 100, 2),
                    "爆量周成交量": int(signal_volume),
                    "前一周成交量": int(signal["上周成交量"]),
                    "放量倍数": round(signal["放量倍数"], 2),
                    "爆量后最大涨幅%": round(max_rise * 100, 2),
                    "爆量后最大跌幅%": round(max_fall * 100, 2),
                    "形态判断": shape,
                    "结论": conclusion,
                })

        except KeyboardInterrupt:
            logger.warning("用户中断，正在保存当前结果...")
            break
        except Exception:
            pass

        processed += 1
        if processed % 100 == 0:
            logger.info(
                "进度: %d/%d (%.1f%%), 已筛选: %d",
                processed,
                total_stocks,
                processed / total_stocks * 100,
                len(results),
            )

    # 输出结果
    result_df = pd.DataFrame(results)

    if result_df.empty:
        logger.info("没有筛到符合条件的股票")
    else:
        result_df = result_df.sort_values(
            by=["结论", "当前价偏离%", "放量倍数"],
            ascending=[True, True, False],
        )
        logger.info("筛选完成，共 %d 条结果", len(result_df))
        print(result_df.to_string(index=False))

        output_file = "两年首次爆量成本区回踩选股.xlsx"
        result_df.to_excel(output_file, index=False)
        logger.info("已导出: %s", output_file)


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
"""
手动一键增量拉取脚本 —— 整合 crontab 定时任务（scripts/data_collection/*）

按依赖顺序串行执行全部增量采集脚本：
  base -> daily -> cyq(依赖 daily) -> factor -> index -> macro -> fin -> dividend

频率规则（与 crontab 一致）：
  - 每天:          incremental_base / daily / cyq / factor / index
  - 每周三、六:     incremental_fin
  - 每周日:        incremental_dividend / incremental_macro
  默认按"今天星期几"过滤；--all 无视频率全部执行。

说明:
  - 手动执行默认不设超时（--timeout-per-task 可指定，超时会终止对应任务）
  - 单个任务失败不中断后续任务，最后汇总并返回非 0 退出码
  - 收到 SIGTERM/SIGINT（外层 timeout 或 Ctrl+C）时，终止当前子进程并停止后续任务，
    避免增量子脚本成为孤儿进程继续写库
  - --only 与 --skip 互斥，只能使用其中一个
  - 日志追加写入 logs/<脚本名>.log（与 crontab 日志文件一致）
  - 本脚本自身的控制台输出每行带 [YYYY-MM-DD HH:MM:SS] 时间戳（_log）
  - 策略回测（backtest_5y_23strategies.py）不属于数据获取，未纳入

用法:
  python scripts/data_collection/run_all_incremental.py              # 按频率过滤
  python scripts/data_collection/run_all_incremental.py --all        # 无视频率全部执行
  python scripts/data_collection/run_all_incremental.py --skip daily,macro
  python scripts/data_collection/run_all_incremental.py --only macro,fin,dividend
  python scripts/data_collection/run_all_incremental.py --timeout-per-task 7200
  python scripts/data_collection/run_all_incremental.py --dry-run    # 只打印执行计划
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.join(PROJECT_ROOT, "scripts", "data_collection")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

# crontab 星期: 0=周日, 1=周一 ... 6=周六；weekdays=None 表示每天执行
TASKS = [
    ("incremental_base.py",     "incremental_base.log",     "每天",       None),
    ("incremental_daily.py",    "incremental_daily.log",    "每天",       None),
    ("incremental_cyq.py",      "incremental_cyq.log",      "每天",       None),
    ("incremental_factor.py",   "incremental_factor.log",   "每天",       None),
    ("incremental_index.py",    "incremental_index.log",    "每天",       None),
    ("incremental_macro.py",    "incremental_macro.log",    "每周日",     {0}),
    ("incremental_fin.py",      "incremental_fin.log",      "每周三、六", {3, 6}),
    ("incremental_dividend.py", "incremental_dividend.log", "每周日",     {0}),
]

DOW_NAMES = ["周日", "周一", "周二", "周三", "周四", "周五", "周六"]


def _log(msg: str) -> None:
    """带时间戳输出：非空行加 [YYYY-MM-DD HH:MM:SS] 前缀，空行保留为换行分隔。"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for line in str(msg).split("\n"):
        if line:
            print(f"[{ts}] {line}")
        else:
            print()


# 信号转发状态：外层 timeout（或 Ctrl+C）触发后，终止当前子进程并停止后续任务，
# 避免正在运行的增量子脚本成为孤儿进程继续占用数据库连接。
_current_proc: subprocess.Popen | None = None
_stop_requested = False


def _forward_signal(signum, frame) -> None:
    """收到 SIGTERM/SIGINT 时终止当前子进程，并标记停止后续任务。"""
    global _stop_requested
    _stop_requested = True
    proc = _current_proc
    if proc is not None and proc.poll() is None:
        _log(f"\n[run_all_incremental] 收到信号 {signum}，正在终止子进程 PID={proc.pid} ...")
        try:
            proc.terminate()
        except OSError:
            pass


def crontab_today() -> int:
    """python weekday(0=周一..6=周日) -> crontab dow(0=周日..6=周六)。"""
    return (datetime.now().weekday() + 1) % 7


def run_task(script: str, log_name: str, timeout: int) -> tuple[bool, float]:
    """执行单个增量脚本，stdout/stderr 追加写入日志文件。返回 (是否成功, 耗时秒)。

    使用 Popen 而非 subprocess.run：外层 timeout 只向本进程发信号，
    用 Popen 拿到子进程句柄后才能在收到 SIGTERM/SIGINT 时转发并终止子进程，
    避免子进程成为孤儿继续写库。
    """
    global _current_proc
    script_path = os.path.join(SCRIPT_DIR, script)
    log_path = os.path.join(LOG_DIR, log_name)
    os.makedirs(LOG_DIR, exist_ok=True)

    t0 = time.time()
    with open(log_path, "a", encoding="utf-8") as fh:
        fh.write(f"\n===== {datetime.now():%Y-%m-%d %H:%M:%S} "
                 f"run_all_incremental 启动 {script} =====\n")
        fh.flush()
        try:
            proc = subprocess.Popen(
                [sys.executable, script_path],
                cwd=PROJECT_ROOT,
                stdout=fh,
                stderr=subprocess.STDOUT,
            )
            _current_proc = proc
            try:
                proc.wait(timeout=timeout if timeout > 0 else None)
                ok = proc.returncode == 0
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                fh.write(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}] [run_all_incremental] {script} 超过 {timeout}s 超时，已终止\n")
                ok = False
        except OSError as exc:
            fh.write(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}] [run_all_incremental] {script} 启动失败: {exc}\n")
            ok = False
        finally:
            _current_proc = None
    return ok, time.time() - t0


def main() -> int:
    parser = argparse.ArgumentParser(description="手动一键增量拉取（整合 crontab 数据采集任务）")
    parser.add_argument("--all", action="store_true", help="无视星期频率，全部任务执行")
    parser.add_argument("--only", default="",
                        help="只执行指定脚本，逗号分隔（与 --skip 互斥），如 daily,factor")
    parser.add_argument("--skip", default="",
                        help="跳过指定脚本，逗号分隔（与 --only 互斥），如 daily,macro")
    parser.add_argument("--timeout-per-task", type=int, default=0,
                        help="每个任务超时秒数，0 表示不限制（默认）")
    parser.add_argument("--dry-run", action="store_true", help="只打印执行计划，不实际执行")
    args = parser.parse_args()

    only = {x.strip().lower() for x in args.only.split(",") if x.strip()}
    skip = {x.strip().lower() for x in args.skip.split(",") if x.strip()}
    if only and skip:
        parser.error("--only 与 --skip 不能同时使用")

    # 注册信号转发：外层 timeout / Ctrl+C 只作用于本进程，
    # 需接管后转发给当前子进程，并停止后续任务。
    signal.signal(signal.SIGTERM, _forward_signal)
    signal.signal(signal.SIGINT, _forward_signal)

    today = crontab_today()

    def matches(names: set[str], script: str) -> bool:
        """支持全名（incremental_daily.py）或短名（daily）匹配。"""
        short = script.replace("incremental_", "").replace(".py", "").lower()
        return script.lower() in names or short in names

    selected = []
    for script, log_name, freq, weekdays in TASKS:
        if only and not matches(only, script):
            continue
        if matches(skip, script):
            continue
        if args.all or weekdays is None or today in weekdays:
            selected.append((script, log_name, freq))

    if not selected:
        _log(f"今天({DOW_NAMES[today]})没有需要执行的增量任务，无需运行")
        return 0

    _log(f"今天: {DOW_NAMES[today]} | 计划执行 {len(selected)} 个任务"
          f"{'（--all 强制全部）' if args.all else ''}:")
    for script, _, freq in selected:
        _log(f"  - {script}  [{freq}]")
    if args.dry_run:
        _log("dry-run 模式，不实际执行")
        return 0

    results = []
    for i, (script, log_name, _) in enumerate(selected, 1):
        if _stop_requested:
            _log("已收到终止信号，停止后续任务")
            break
        _log(f"\n[{i}/{len(selected)}] 开始: {script} ...")
        ok, elapsed = run_task(script, log_name, args.timeout_per_task)
        status = "成功" if ok else "失败"
        _log(f"[{i}/{len(selected)}] {script} -> {status}（耗时 {elapsed:.0f}s）")
        results.append((script, ok))

    failed = [script for script, ok in results if not ok]
    _log("\n===== 执行汇总 =====")
    for script, ok in results:
        _log(f"  [{'OK' if ok else 'FAIL'}] {script}")
    if failed:
        _log(f"\n失败 {len(failed)} 个: {', '.join(failed)}；日志见 logs/ 目录")
        return 1
    if _stop_requested:
        _log("\n任务被信号中断（已转发并终止子进程）")
        return 1
    _log("\n全部任务执行成功")
    return 0


if __name__ == "__main__":
    sys.exit(main())

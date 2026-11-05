# 回测运行手册（backtest_5y_23strategies）

本手册只回答“怎么跑、怎么读、跑完怎么判断”。指标定义与验收口径见
`result/指标契约_metric_contract_v1_20261017.md`；策略设计背景见
`docs/backtest-optimization-design.md`；实验卡片见 `docs/backtest-experiment-cards.md`。

## 0. 先确认三件事

```powershell
# 1) 数据是否跟上最近交易日（退出码 1 = 有表落后）
conda run --live-stream -n daily_stock_analysis python scripts/check_data_freshness.py

# 2) 解释器必须是项目环境（3.10.x），不是 base
conda run --live-stream -n daily_stock_analysis python -V

# 3) 从仓库根目录运行：脚本里的 load_dotenv() 不带路径
Set-Location <repo-root>
```

`conda activate` 在非交互 PowerShell 里不会真正切换解释器；要么用上面的
`conda run --live-stream`，要么直接用项目解释器的绝对路径（把
`<envs>\daily_stock_analysis\python.exe` 换成你机器上的实际位置，或用
`pwsh -File scripts/pytest.ps1` 的解析逻辑）。

数据落后时先补：

```powershell
conda run --live-stream -n daily_stock_analysis python scripts/data_collection/run_all_incremental.py
```

日常表（`daily/cyq/factor/index`）每个交易日跑；`fin` 是周三/周六；`macro/dividend` 是周日。

## 1. 三种运行口径

| 目的 | 命令 | 看什么 |
| --- | --- | --- |
| 决策级组合指标 | `--portfolio-sim --no-handoff --force` | 资金约束下的 CAGR / maxDD / Sharpe / 成交笔数 |
| 旧信号级表 | `--no-handoff --force` | 23 策略逐持有期胜率、期望、基准超额 |
| 复现冻结的 L-only 基线 | `--portfolio-sim --no-handoff --force --universe l-only` | 与历史 L-only 数字对齐 |

```powershell
# 决策级：组合模拟 + 跳过 handoff/主程序
conda run --live-stream -n daily_stock_analysis python .\scripts\backtest_5y_23strategies.py `
  --portfolio-sim --no-handoff --force

# 信号级：只出旧结果表
conda run --live-stream -n daily_stock_analysis python .\scripts\backtest_5y_23strategies.py `
  --no-handoff --force

# 冻结 L-only 口径（仅用于对齐历史基线，不是默认推荐）
conda run --live-stream -n daily_stock_analysis python .\scripts\backtest_5y_23strategies.py `
  --portfolio-sim --no-handoff --force --universe l-only
```

## 2. 参数语义（容易误解的部分）

- `--force`：只跳过“非交易日直接退出”的检查，不跳过验证窗口。
- `--no-handoff`：结果表输出后退出，跳过 handoff 评分、5 日选股与 `main.py` 子进程；
  5 日验证本身仍会跑，约 12 分钟。
- `--portfolio-sim`：T+1、最多 10 仓位、0.35% 成本、ADV 滑点、日频盯市；
  要求 `--entry-timing next_open`（默认），且与 `--per-strategy-exit` 互斥。
- `--universe`：默认 `pit`（按 `list_date` / `delist_date` 的可交易窗口纳入退市股；数据不足时
  显式告警并退化为 L-only）；`l-only` 仅用于复现冻结的 L-only 基线，存在生存者偏差。
- `--no-cache`：一般不需要。缓存 key 已包含结束日期，换日期会自然失效；
  只有怀疑缓存损坏或改了数据加载 SQL 时才用。
- 不要加 `--per-strategy-exit` 配合 `--portfolio-sim`：两者互斥，argparse 会直接报错。

`.env` 里的 `BACKTEST_CONFLUENCE=1` 和 `BACKTEST_REGIME_SOFT=1` 会被读取并以 `(env)` 记录；
CLI 的 `--confluence-only` / `--regime-soft` 只能打开，不能关闭。要跑裸默认口径，必须临时改 `.env`。

## 3. 时长与产物

参考运行：`logs/backtest_5y_20261102.log`（06:52:39 → 07:51:07，58m28s，21 个策略，
验证窗口 0 个 `market_ok_enh2` 日 → 0 只股票 → `main.py` 跳过）。

- 全量回测：约 55–60 分钟（机器与缓存命中影响较大）。
- `--portfolio-sim`：额外约 3 分钟。
- 5 日验证：约 12 分钟；`--no-handoff` 不会省掉这一段。
- 组合模拟产物写到 `result/`（可用 `--portfolio-sim-out` 改目录），不覆盖旧指标文件。

日志里先确认这两行，再读收益：

```text
市场环境口径: ... (env)
组合模拟 config_hash: ...
```

`config_hash` 用于确认口径没有漂移；数据窗口变化会改变收益数字，但不应该改变同一口径的 hash。

## 4. 结果怎么读

- 决策级只看 `--portfolio-sim` 的资金约束指标；旧信号级表用于归因，不用于对外结论。
- 5 日验证是观测项：metric contract §9 明确它接近市场基准率（27.9%），不能拿来选策略。
- 验证窗口出现 0 个 `market_ok_enh2` 日是正常现象（2026-11-02 即如此），
  此时 `main.py` 不会执行，退出码仍是 0。
- 非交易日运行只打印一行 `ERROR` 日志并以 0 退出；需要强制跑就加 `--force`。
- 对比两次运行：先对齐 `config_hash`、`universe`、数据截止日，再比 CAGR / maxDD / Sharpe / 成交笔数；
  只比总收益会被行情区间带偏。

2026-10-19 冻结 PIT 基线（数据截至 2026-10-16，仅作口径对照）：
`CAGR(OOS) 4.20%`、`maxDD 13.91%`、`Sharpe 0.444`、`560 trades`、`avg cash 77.69%`。
今天的绝对数字会不同，不要把“数字不等”直接当成回归。

## 5. 出问题时的最小排查

| 现象 | 先查 |
| --- | --- |
| 启动即退出，只有一行 `ERROR` | 今天是否交易日；需要跑就加 `--force` |
| `PG_PASSWORD 未设置` / 连接失败 | 仓库根目录 `.env` 的 `PG_*`；用 `check_data_freshness.py` 验证连通性 |
| 验证窗口 0 只股票 | `market_ok_enh2` 天数是否为 0；这是观测结果，不是崩溃 |
| 结果与上次差异很大 | 数据截止日、`config_hash`、`universe`、`.env` 的 confluence/regime 开关 |
| 组合模拟报参数错误 | 是否同时用了 `--per-strategy-exit`，或 `--entry-timing close` |
| 跑得异常慢 | 是否在 base 环境；缓存是否命中；是否误开了 `--no-cache` |

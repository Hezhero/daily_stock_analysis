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
- `--cache-keep N`：准备态缓存保留个数（默认 1，`0` = 不删除）。写完新缓存后删除较旧的
  `backtest_*.parquet`；**与 `--no-cache` 无关**（后者指本次运行既不读也不写缓存，不动历史缓存）。
  细节见 §3.1。
- `--regime-mode`：默认 `auto`（不传参即逐日按市况自动选可开仓判定列；仅
  `regime=bull` 且 `breadth≥0.45` 且 `vol_pct≤0.85` 的当日放宽到 `enh`，其余回退 `enh2`；
  显式传 `enh2` / `enh` / `strict` 为覆盖并在启动日志标注「覆盖(档位)」）。
  `enh2` 是 `auto` 的基准与冲突回退档，`strict` 永不自动选中。复现 2026-10-19 冻结 PIT
  基线、10 月四张实验卡与 2026-10-29 起的 0/5 日志时须显式传 `--regime-mode enh2`（这些
  历史结果基于「默认 `enh2`」口径）。规则版本 `auto_v1` 单值计入 `config_hash`，默认 `auto`
  与显式 `enh2` 的哈希可区分。设计取舍见 `docs/adr/0006-auto-regime-mode-default.md`。
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
市场环境: regime-mode=auto (market_ok_auto) | 口径标记: 自动(auto) | 规则版本: auto_v1 | ...
组合模拟 config_hash: ...（--portfolio-sim 时由 print_portfolio_report 打印）
```

此外 `auto` 运行会另打一行逐日市况摘要（票 01 观测 seam）：

```text
逐日市况: 规则版本=auto_v1 | 口径=自动(auto) | market_ok=... | 逐日选档序列=enh:N,enh2:M | 产物: result/daily_regime.csv
```

`config_hash` 用于确认口径没有漂移；数据窗口变化会改变收益数字，但不应该改变同一口径的 hash。
`auto` 默认会把规则版本 `auto_v1` 计入 `config_hash`，与显式 `--regime-mode enh2` 的冻结基线
哈希可区分。

`auto` 口径落地后，启动日志表达「规则版本 + 逐日选档序列 + 显式覆盖标记」，逐日市况表
（`result/daily_regime.csv`，含 `date` / 三档布尔列 / `regime` / `breadth` / `vol_pct`，
auto 口径下另含 `auto_mode` / `market_ok_auto`）落盘到既有 result/ 出口，供离线复算「哪天
被判成哪一档」。排障对齐清单（数据截止日 / `config_hash` / `universe` / `.env` 静态开关）
之外，比对 `auto` 结果时还须对齐该逐日市况表。

### 3.1 准备态缓存（`data/cache/`）

回测会把「复权 + 增强信号合并后、指标计算前」的中间数据写成
`data/cache/backtest_{起始日}_{结束日}_{口径版本}.parquet`（单份约 800MB），命中时跳过
PG 加载、复权与增强信号合并（约 8 分钟）。它**只是加速手段**：指标与出场收益每次仍实时重算。

命名里内嵌**运行日期**，读取又只按精确路径匹配，所以：

- **换一个运行日期必然未命中**，必然新增一份；
- 历史份除「同一天重跑」外**永不再被读取**（2026-11-18 实测已累积 16 份 / 13GB）；
- 目录默认只增不减，磁盘会被慢慢吃满。

保留策略（2026-11-18 起）：

| 参数 | 行为 |
| --- | --- |
| 默认 `--cache-keep 1` | 写完新缓存后删除较旧的 `backtest_*.parquet`，按**文件 mtime** 保留最新 1 个 |
| `--cache-keep N` | 同上，保留最新 N 个 |
| `--cache-keep 0` | 不删除，保留全部历史缓存 |
| `--no-cache` | 本次运行既不读也不写缓存；**不触发清理**（它只管本次运行，不动历史缓存） |

- 清理只删本 driver 的 `backtest_*.parquet`；同目录的 `akshare_name_map.json`（股票名称解析）
  与 `stocks.index.json`（前端股票索引）**不受影响**。
- 删除失败只打 WARNING，不中断回测（清理是纯磁盘回收）。
- **仅在缓存写入成功后清理**：写失败（磁盘满最常见）时旧缓存正是还能用的那份，不会被删。
- **缓存命中（同日重跑）时不清理、不打印状态行**——那次运行没有新缓存可写。
- 每次**写完**缓存会打一行现状（保留/删除/释放）：

```text
准备态缓存: 保留 1 个 | 删除 15 个（释放 12.2 GB）
```

- `data/` 整体被 gitignore，缓存与产物都不入库；换机/换 clone 后需重新生成。

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
| `server closed the connection unexpectedly` / 连接超时 | PG 若跑在 WSL/Docker 里：先 `wsl -d Ubuntu-24.04`，再用 `docker ps` 确认 `postgresql` 容器 Up；长回测期间保持 WSL 存活，否则容器会随 WSL 停止 |
| 验证窗口 0 只股票 | `market_ok_enh2` 天数是否为 0；这是观测结果，不是崩溃 |
| 结果与上次差异很大 | 数据截止日、`config_hash`、`universe`、`.env` 的 confluence/regime 开关 |
| 组合模拟报参数错误 | 是否同时用了 `--per-strategy-exit`，或 `--entry-timing close` |
| 跑得异常慢 | 是否在 base 环境；缓存是否命中；是否误开了 `--no-cache` |

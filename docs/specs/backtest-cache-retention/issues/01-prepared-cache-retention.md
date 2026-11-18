# 01: 准备态缓存保留策略（`--cache-keep`）

**What to build:** 给 `scripts/backtest_5y_23strategies.py` 的准备态 parquet 缓存加**保留策略**：
缓存文件名内嵌运行日期、读取又只按精确路径匹配，故换一天运行必然未命中并新增一份 ~800MB，
`data/cache/` 只增不减（2026-11-18 实测 16 份 / 13GB）。写完新缓存后按文件 mtime 保留最近
N 个（新增 `--cache-keep N`，默认 1，`0` = 不删除），只删本 driver 的 `backtest_*.parquet`。
不改任何信号、不改回测口径、不改缓存内容本身。

**Blocked by:** None (can start immediately)

**Gate:** `none`
**Signal set:** `none`

**Status:** done

- [x] 新增 `cleanup_prepared_cache(cache_dir, keep)`：按**文件 mtime** 降序保留最新 N 个，
  删除其余；只匹配 `backtest_*.parquet`，同目录 `akshare_name_map.json`（股票名称解析）与
  `stocks.index.json`（前端索引）不受影响。
- [x] `--cache-keep N`（默认 1，`0` = 保留全部历史缓存）；默认值的**单一来源**与文档一致
  （CLI default / 函数默认 / help 文本 / runbook / CHANGELOG 五处同源，测试锁定）。
- [x] 清理在**写入成功之后**执行；写失败（磁盘满最常见）时**不删旧缓存**——
  `write_prepared_cache` 改为返回成功布尔，调用方据此决定。
- [x] 删除失败只计入 `failed` 并 WARNING，绝不中断回测；文件在扫描期间消失不中断整轮清理，
  且 `kept` 计数不谎报。
- [x] `--no-cache` 运行**不触发清理**（它只管本次运行既不读也不写，不动历史缓存）；
  缓存**命中**（同日重跑）路径**不清理、不打印状态行**（2026-11-18 用户裁决）。
- [x] 有定向测试覆盖：保留策略、keep=0、只删 parquet、删除失败降级、文件消失不中断、
  写成功才清理、写失败不清理。
- [x] 文档同步：GLOSSARY「准备态缓存」词条、`docs/backtest-runbook.md` §2 与 §3.1、
  `docs/CHANGELOG.md` 条目；`scripts/check_ai_assets.py` 与 `check_spec_tickets.py` 通过。
- [x] `py_compile` 与定向 `pwsh -File scripts/pytest.ps1 <files>` 通过。

**Note:** 本票由用户 2026-11-18 `/grill-with-docs` 设计树（Q1–Q10）产生。术语遵循
`GLOSSARY.md`「回测运行产物」段；缓存目录与用法的权威说明在 `docs/backtest-runbook.md` §3.1。

## Comments

- 事实基线（2026-11-18 实测）：`data/cache/` 16 个 parquet / 13.0GB / 平均 812MB；
  缓存读取只按 `_prepared_cache_path(start, end, universe)` 精确命中，历史份除「同一天重跑」
  外永不再被读取。清理后 785MB。
- 「保留数按 mtime 而非文件名日期」是用户裁决（Q4）：mtime 反映真实写入时间，文件名只是
  命名约定，可被手工改名破坏；同 mtime 时用文件名兜底保证确定性。
- 本票属**破坏性默认值变更**（默认删旧缓存），按 `AGENTS.md` §1.3 需人类拍板——
  用户 2026-11-18 逐题裁决 Q1–Q10 后授权。
- 未写 ADR：`domain-modeling` 三条标准不满足（清理策略易改、不令人意外、无实质权衡的不可逆性）。
- 两轴 code-review（2026-11-18）发现并已修复：写失败仍删缓存（Q2 保证未兑现）、
  排序键 `p.stat()` 在文件消失时中断整轮清理且计数说谎、runbook 两处 `--no-cache` 表述互相矛盾、
  GLOSSARY「回测结果表」词条事实错误（逐策略表只 print 到 stdout，从不落盘 `result/`）。
  复核后：`--cache-keep` 默认值保留 1（保留最近 1 个），未改「保留 3 个」。
- review 后追加的用户裁决（2026-11-18）：**开票**（本票）、**写成功才清理**（Q2 兑现方式：
  `write_prepared_cache` 改返回 `bool`）、**缓存命中路径不清理也不打印状态行**（与 Q9「总是打印」
  存在张力，按用户裁决以命中路径例外为准，已在 runbook §3.1 与本票验收框写明）。
- 单一来源收敛（review HARD #1）：`CACHE_KEEP_DEFAULT` 是保留个数的唯一来源，CLI 默认、
  两个函数默认、help 文本与文档都引它；测试锁定「CLI 默认 == 函数默认 == 常量 == help 文本」。
- 真实调用点收敛：`write_and_retain_prepared_cache()` 是「写 → 清理 → 打印」的唯一入口，
  `main()` 只调它；测试直接驱动该入口，避免「只测纯函数、接线无覆盖」。
- 复核未采纳的 review 建议：保留数按 mtime（用户裁决 Q4，review 建议改用文件名日期）、
  日志额外输出「失败 N 个」（必要的实现细化：Q5 要求失败可见）、日志附当前磁盘占用
  （review 建议；未采纳——那需要额外 stat 目录，且本轮 Q9 只要求 kept/deleted/freed）。
- **已知局限（review J1，2026-11-18 用户裁决 C：不改行为、仅文档提醒）**：`keep=N` 按 mtime
  全局保留，**不区分股票池口径**。`l-only` 的缓存名为 `backtest_{start}_{end}_v2.parquet`，
  `pit` 为 `..._v2upit.parquet`，两者是不同文件，但清理用 `glob("backtest_*.parquet")` 一网打尽，
  所以 `keep=1` 下用 `l-only` 复现冻结基线的缓存可能被随后的 `pit` 跑删掉。
  缓解：`docs/backtest-runbook.md` §3.1 已加醒目提醒（需要跨口径保留时传更大 `--cache-keep`，
  或跑 `l-only` 前先备份其缓存）。**未实现按口径分组保留**——那是行为变更且属破坏性默认值，
  按 `CODING_STANDARDS.md`「破坏性默认行为必须落票」需另开票重新裁决。

# 10: native Windows 全量失败白名单（显式、有理由、可判读）

**What to build:** 让 native Windows 上的 `python -m pytest -m "not network"` 从"一片红"变成可判读的信号：既有的、已归因的失败登记进一份显式白名单；**新增失败、白名单里已经不再失败的条目、以及任何 collection error 都必须立即显性**，而不是混在同一堆红字里。

**Blocked by:** None（票 09 已把 collection error 清零，并更正了 `test_local_cli_backend.py` 的归因表）。

**Gate:** `none`
**Signal set:** `none`

**Status:** ready-for-human（机制已落地，见「实现」；白名单 52 条 `TODO` 属票 09 剩余项，不新增 Windows CI job 属人工决策）

## 背景

票 09 于 2026-10-25 实测 native Windows：`69 failed / 7709 passed / 275 skipped / 0 errors`。其中只有 Codex App Server 那组（14 个）有明确平台契约理由（该后端在 Windows 由产品排除，同文件 `:238` / `:266` 已有 `skipif` 先例）；其余约 55 个为零散失败，尚未逐项分类。

票 09 的验收标准 1 要求"native Windows 下失败集合必须与本文档的白名单一致，且新增失败立即显性"，但当时**没有任何脚本或 CI 在核对**——只有 `docs/agents/validation-matrix.md` 里的散文。结果是：本机变红时，无法区分"我引入的回归"与"既有失败"，贡献者只能退化为跑定向测试。

## 设计（MVP）

1. **独立 checker，不塞进 `ci_gate`。** `scripts/check_windows_test_baseline.py` 自己跑一次 pytest 并比对白名单。
   理由：`ci_gate.sh` 支持 `PYTEST_SPLITS` / `PYTEST_GROUP` 分片（CI 三分片），任何 session 级的 hook 方案都只看到分片内的子集，无法做全集比对。
2. **白名单是 markdown 表格**（`docs/agents/windows-known-failures.md`），每行必须带 `test` / `类别` / `理由`：
   - 类别只能取 `validation-matrix.md` Platform boundary 定义的三类之一：
     `product-excluded` / `stdlib-missing` / `posix-mode-bits`；
   - 理由写作 `TODO` 视为**未填**，checker 必须因此失败——不许把未分类失败当成已归因。
3. **白名单只描述失败，不改变测试结果。** 把某条登记进白名单不等于 skip；真正的平台契约排除仍应走 `skipif`（票 09 剩余项 3）。
4. **POSIX 上白名单必须为空。** 在 POSIX 跑 checker 时，任何非空白名单条目直接报错退出，防止有人把 Linux 上的失败也塞进 Windows 白名单。

### 退出码语义

| 码 | 含义 |
| --- | --- |
| `0` | 实际失败集合 == 白名单，且 0 collection error，且无 `TODO` 理由 |
| `1` | 存在新增失败 / 白名单过期条目 / `TODO` 理由 / collection error |
| `2` | 用法错误或环境不满足（例如 POSIX 上白名单非空、pytest 未安装） |

## 验收标准

1. checker 能区分四类问题并分别列出：`NEW`（失败但未登记）、`STALE`（登记了但已不再失败）、`TODO`（登记了但没写理由）、`COLLECTION_ERROR`。
2. `--update` 可按本次结果重写白名单：已有条目的理由保持不变，新条目写 `TODO`，不会静默生成"看起来合法"的理由。
3. POSIX 下白名单非空时 checker 以退出码 2 拒绝运行。
4. 白名单每行的类别都落在 `validation-matrix.md` 的三类之内（checker 校验，不是靠人自觉）。
5. 有覆盖上述分支的单测，不需要真跑全量。

## 实现（2026-10-25，native Windows 实测）

- `scripts/check_windows_test_baseline.py`：读 JUnit（`--junitxml`）而不是解析 `-rf` 文本，
  避免参数化 id 里的空格把 nodeid 切坏；`--update` 保留已有理由、新条目写 `TODO`；
  存在 collection error 时拒绝更新。
- `docs/agents/windows-known-failures.md`：由 `--update` 生成，66 行。其中 14 行（Codex App
  Server 状态用例）已填 `product-excluded` + 理由，52 行仍为 `TODO`——它们是票 09 剩余项 1-3 的
  待办清单，**不是**已归因条目。
- `tests/test_check_windows_test_baseline.py`：12 例，覆盖表格解析与 round-trip、junit 里
  failure/error/collection error 的分类、`NEW`/`STALE`/`TODO`/类别越界/collection error 各自失败、
  `--update` 保留已归因条目、POSIX 拒绝非空白名单、collection error 时拒绝更新。

实测：

```
python scripts/check_windows_test_baseline.py --update    → 写入 66 行，exit 1（52 条 TODO）
python scripts/check_windows_test_baseline.py --junit …   → 66 registered / 66 failing
                                                            0 NEW / 0 STALE / 52 UNRESOLVED → exit 1
pytest tests/test_check_windows_test_baseline.py          → 12 passed
```

**实测得出的一条规则**（已写进白名单文件头）：全量失败数在 66~71 之间波动（`test_screening_api`
的截止时间用例、`test_output_stat_error_*` 的 spawn 期偶发 `OSError` 等）。**偶发失败不是已知
失败**，不能靠白名单吸收，否则 `NEW` / `STALE` 会随机抖动；登记前提是"在目标平台上确定性失败"。

**未验证**：POSIX 分支（`_is_native_windows()` 为 `False` 时拒绝非空白名单）只有单测覆盖，没有在
真实 POSIX 机器上跑过。也因此本票没有把脚本接进 `ci_gate`——接入等于给非 Windows 开发者也加一次
全量跑，收益未证明。

**已知限制已补测（2026-10-26）**：checker 从 `--junitxml` 读结果，而 **junit 不记录子测试级失败**。
此前这条只是记录在案，现在已实现：捕获 pytest 摘要行的 `N failed`，与 junit 计数比对，差额以
`UNREPRESENTED (n)` 报出。

刻意把它处理成**信息性**而非致命：当前 5 条差额来自 `test_notification` 的长期子测试失败，若设为
致命，基线永远红，工具就失去"基线一致即通过"的意义。差额变大时能在输出里直接看到，比完全不可见强。

实测：一次真实全量 `17 failed`（pytest）vs `12`（junit 可见）→ 报 `UNREPRESENTED (5)`，
同时 `12 registered / 12 failing`、退出码 0。


## 不在本票范围

- **新增 Windows CI job**：仓库目前只在 `ubuntu-latest` 跑 pytest 全量；Windows 侧只有 `desktop-release` 打包。加 Windows 全量 job 的成本（依赖 + 时长）需要单独评估。
- **给 14 个 Codex 用例补 `skipif`**：属票 09 剩余项 3，是"把契约排除表达成 skip"的独立决定，与本票的"登记已知失败"是两件事。
- **约 55 个零散失败的逐项分类**：属票 09 剩余项 1-2。本票落地后它们会以 `TODO` 形式显性存在，成为该项的待办清单。

## 扩展（2026-10-26）：第 4 类 `known-unattributed` 与软 STALE

票 09/12 的收尾阶段剩余失败已经**不是**平台契约问题（时序、渲染桩、未定性），若坚持只允许
`product-excluded` / `stdlib-missing` / `posix-mode-bits` 三类，这些失败会永远以 `NEW` 形式出现，
白名单反而失去意义。因此新增第 4 类：

- `known-unattributed`：**既有失败，根因已记录但不属于任何平台契约**。它只用于把已知失败固定住，
  让新增失败显性；不得用它掩盖平台契约问题。
- 该类条目的 **STALE 不阻断**：登记项这次通过属好消息，报告为 `STALE (tracked, not gating)` 并
  建议清理，但退出码仍为 0。`product-excluded` 等三类仍保持"登记了却通过 = 失败"（那是契约漂移）。
- `NEW`、`TODO`、collection error 依旧一律失败。

落地结果：基线登记 **12 条**（16 个文件里的 12 个 junit 可见失败），checker 对同一次运行
`12 registered / 12 failing`、退出码 0。子测试级失败（`test_notification` 的 6 条 subtests）仍
不可见，属既有已知限制。

## Notes

- 白名单的 id 用 JUnit 的 `classname::name`（checker 从 `--junitxml` 读取），因此和 `pytest -rf` 打印的 `tests/x.py::test_y` 不逐字相同；新增条目走 `--update` 而不是手抄。
- 不再扩展"白名单之外还能豁免什么"。豁免种类越多，这份文件越容易变成掩盖回归的地方。

# 14: 三条与平台无关的失败（推翻"POSIX 全绿"前提）

**What to build:** 处理三条**在任何平台都会失败**的断言，它们不是 native Windows 特性，却一直混在
Windows 失败清单里。修好它们才能让"POSIX 全绿、Windows 走白名单"这个前提成立。

**Blocked by:** None。

**Gate:** `none`
**Signal set:** `none`

**Status:** ready-for-human（两处都需要产品口径，见「待定决策」）

## 背景（2026-10-26 实测）

票 09 的验收标准 1 写着"POSIX 环境全绿"。这三条说明该前提**从未被验证**：

| 用例 | 断言 | 实际 | 性质 |
| --- | --- | --- | --- |
| `test_check_env_encoding::test_requirements_file_is_ascii_decodable` | `requirements.txt` 可用 ASCII 解码 | 文件有 **22 行中文注释**（如 `# 时间支持`），第 4560 字节处即失败 | 测试与数据不同步；纯 `read_bytes().decode("ascii")`，无平台差异 |
| `test_config_env_compat::test_load_from_env_keeps_default_behavior_without_tickflow_api_key` | `realtime_source_priority == "tencent,akshare_sina,efinance,akshare_em"` | `Config._resolve_realtime_source_priority()` 的**代码默认值**是 `tencent,efinance,tushare` | 断言落后于代码；用例已 `patch.dict(clear=True)`，与环境无关 |
| `test_tickflow_manager_routing::test_tickflow_api_key_does_not_auto_inject_realtime_priority` | 同上 | 同上 | 同上 |

三条都不依赖 `.env`、不依赖 `os.environ`、不依赖测试顺序——去掉 Windows 前缀后依然是红的。

## 待定决策

### D1. `requirements.txt` 的 ASCII 约束怎么处理

- **(a) 把 22 行中文注释翻成 ASCII**：保留"requirements 必须 ASCII 可解码"的护栏（这条护栏是为
  Windows 首次运行编码问题加的，见提交 5cd968b4）。
- **(b) 放宽测试为 UTF-8 可解码**：护栏变弱，但允许中文注释。

➡️ 推荐 **(a)**：护栏有明确的 Windows 首次运行动机，而注释翻成英文成本极低。

### D2. 实时源默认值以谁为准

- **(a) 以代码为准**，把两条断言改成 `tencent,efinance,tushare`。
- **(b) 以断言为准**，把代码默认值改回/改成 `tencent,akshare_sina,efinance,akshare_em`。

➡️ 推荐 **(a)**，但**必须核对文档**：`docs/full-guide.md` / `docs/FAQ.md` 里 `REALTIME_SOURCE_PRIORITY`
的示例值仍是 `tencent,akshare_sina,efinance,akshare_em`。若最终确认代码默认值是对的，文档要一起改；
若确认文档才是产品意图，则选 (b) 并补一条"默认值变更"的记录。**这是产品决策，不由实现者单方面定。**

### D3. 如何防止再次出现"混在 Windows 白名单里的跨平台失败"

- 票 10 的白名单要求每条都填 `product-excluded` / `stdlib-missing` / `posix-mode-bits` 之一；
  这三条**一个都不属于**，因此当初就不该被登记。
- ➡️ 推荐：在票 10 的 checker 文档里补一句"无法归入这三类的失败不得登记，必须先修"。

## 验收标准

1. 三条用例在 POSIX 与 native Windows 上都通过。
2. `requirements.txt` 与实时源默认值，**文档、代码、断言三者一致**（`docs/full-guide.md`、
   `docs/FAQ.md`、`Config._resolve_realtime_source_priority`、两条用例）。
3. 若改动涉及默认值语义，`docs/CHANGELOG.md` 有对应条目。
4. native Windows 全量失败集合减少 3 条。

## 不在本票范围

- 其余 34 条 native Windows 失败（票 09/12 剩余项）。
- POSIX 全量基线本身的建立（需要 WSL/Docker/CI 环境）。

## Notes

- 这三条是本轮最有价值的发现：它们证明"本机红 ≠ Windows 问题"，也证明票 09 的验收标准 1
  从来没有被真正执行过。
- 修法都很小，但 D2 是产品语义，必须先定。

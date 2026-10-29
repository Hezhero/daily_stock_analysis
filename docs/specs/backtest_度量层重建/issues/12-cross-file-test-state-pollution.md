# 12: 跨文件测试状态污染（全量与单跑结果不一致）

**What to build:** 让 `python -m pytest -m "not network"` 的失败集合不再取决于"这次跑了哪些文件、
按什么顺序跑"。当前存在一类只在全量里出现的失败：文件单独跑全绿，混在套件里就红。它们不是平台
问题、也不是环境变量问题（后者归票 11），而是**测试之间通过共享单例 / 模块级缓存 / 未复原的进程
状态互相影响**。

**Blocked by:** None（但建议排在票 11 之后，理由见「与票 11 的次序」）。

**Gate:** `none`
**Signal set:** `none`

**Status:** ready-for-human（需要先定"污染源怎么定位"与"是否引入随机顺序"两个口径）

## 背景（2026-10-26 实测）

| 现象 | 证据 |
| --- | --- |
| `tests/test_system_config_service.py` 全量红 8 条 | 单跑 **225 passed**；全量里 6 条 `ConfigValidationError: Configuration validation failed` + 1 条 `'true' != 'false'`（`SEARXNG_PUBLIC_INSTANCES_ENABLED`） |
| `tests/test_system_config_api.py` 全量红 3 条 | 单跑只剩 **1 条**真实失败（`test_preview_agent_backend_status_uses_draft_without_persisting`），另 2 条属污染 |
| 合计 10 条 | 占 native Windows 当前 48 条失败的约 1/5 |

已排除的邻居（都不复现，因此污染源不在"最像"的地方）：

```
pytest tests/test_system_config_api.py tests/test_system_config_service.py
  → 1 failed, 259 passed（只有那条真实失败）
pytest tests/test_config_env_compat.py tests/test_system_config_service.py
  → 1 failed, 268 passed（只有 config_env_compat 自己那条）
```

**为什么难定位**：套件没有随机顺序插件（`pytest-randomly` 未安装，`-p no:randomly` 是空操作），
文件顺序恒为字典序，所以污染是"确定性但跨 ~300 个文件"的。逐文件二分意味着每一轮都要跑几分钟，
且在拿到第一个可复现的最小组合之前，无法判断是哪类状态泄漏。

**可疑的状态类别**（按本仓已知结构列举，未验证）：

- `Config` / `ConfigManager` 的单例与缓存（`.env` 读入后写入 `os.environ`，跨文件残留）；
- `DatabaseManager` 单例与引擎（票 09 本轮已修 6 条同类句柄泄漏）；
- `tests/litellm_stub.ensure_litellm_stub()` 这类模块级桩替换；
- 模块级可变全局（例如 `src/llm/*` 的后端注册表、`src/search_service` 的客户端缓存）。

**已量化的起点**（`grep` 结果，供 D1 参考）：有 **23** 个测试文件改 `os.environ["ENV_FILE"]`，
**18** 个改 `ADMIN_AUTH_ENABLED`，其中只有少数在 teardown 里复原。票 11 的会话级基线实测（48 → 61
failed）证实这批改动是主要泄漏源。其中**唯一确认在 collection 期就生效**的是
`test_analysis_api_contract.py:19`（模块级赋值），见「定位进展」。

## 定位进展（2026-10-26）

**已确认**

- 泄漏真实存在且与顺序相关：会话级环境基线（票 11 的 D1）实测把全量从 `48 failed` 推到
  `61 failed`（+14 / −1），新增失败全部可追到 env 状态，该改动已回滚。
- **collection 期就有污染**：`tests/test_analysis_api_contract.py:15-19` 在**模块导入时**直接
  `os.environ["ENV_FILE"] = <临时 .env>`（为了控制 import 期配置），复原写在同文件的
  `:60-65`。30 文件探针里，唯一一条归因到 `<session start>` 的变更就是它。这是本票第一个
  确定要处理的入口。

**已排除**

- `test_analysis_api_contract.py` + `test_intelligence_api.py` → **133 passed**，该模块级 set 本身
  不足以复现 401。
- `test_system_config_api.py` → `test_system_config_service.py` → 1 failed / 259 passed。
- `test_config_env_compat.py` → `test_system_config_service.py` → 1 failed / 268 passed。

**工具（已落地并校准）**

`tests/conftest.py` 里有一个惰性的环境纯度探针：`DSA_ENV_LEAK_PROBE=1 python -m pytest <files>`。
它在每个用例 **setup** 时把 `ENV_FILE` / `DATABASE_PATH` / `ADMIN_AUTH_ENABLED` 与上一个用例的
setup 比对，差异归因给上一个用例，session 结束时列出 `nodeid :: VAR: before -> after`。
默认完全不生效，只在设置该变量时工作。

校准过程（这是本票最重要的方法结论）：

| 步骤 | 期望 | 实测 |
| --- | --- | --- |
| 已知干净：`test_decision_signal_api.py`（手工 `old_env_file` 还原） | 无输出 | 报 1 条 `ADMIN_AUTH_ENABLED: None -> 'true'`（见「已修」第 2 条） |
| 已知可疑：`test_main_schedule_mode.py` | 报 `ENV_FILE` 反复变化 | 84 条 `ENV_FILE` 过渡（见「已修」第 1 条） |
| 修完第 1 条后复测同一文件 | 归零 | **0** |

**已修（2 处，均有前后计数）**

1. `tests/test_main_schedule_mode.py`：`tearDown` 里两个 `patch.dict` 按**正序** stop。`patch.dict`
   的 `stop()` 恢复的是各自 start 时的快照，先停 `env_patch` 再停 `actions_env_patch`，后者会把
   含临时 `ENV_FILE` 的快照写回——于是 85 条用例每条都泄漏一次。改为逆序后 `ENV_FILE` 过渡
   **84 → 0**，`85 passed`。
2. `tests/test_decision_signal_api.py`：两个用例写入了带 `ADMIN_AUTH_ENABLED=true` 的临时 `.env`，
   还原了 `ENV_FILE` / `DATABASE_PATH` 却没还原 `ADMIN_AUTH_ENABLED`（它已被导出进 `os.environ`）。
   补上同款保存/还原后，该文件探针输出归零。

全量复核：`48 failed / 7728 passed / 289 skipped`，与修复前**一致**（这两处泄漏不是当前 48 条的
成因，属于"先拆掉地雷"）。

**扫描结果（2026-10-26，28 个候选文件一次跑完，按文件 × 变量聚合"净变化"）**

判据从"过渡次数"改成了"净变化"：class/module 级的 set + 还原会产生成对过渡，只有首尾值不同
才算真泄漏。据此定位到 5 个文件、6 处净泄漏，全部修复：

| 文件 | 变量 | 原状 | 修法 |
| --- | --- | --- | --- |
| `test_main_schedule_mode.py` | `ENV_FILE` | 84 次过渡 | `patch.dict` 改逆序 stop |
| `test_decision_signal_api.py` | `ADMIN_AUTH_ENABLED` | `None -> 'true'` | 补保存/还原 |
| `test_auth_api.py` | 3 个变量 | `pop` 归零而非还原 | 改快照/还原（含 `setUpClass`） |
| `test_alert_api.py` | `ENV_FILE` / `ADMIN_AUTH_ENABLED` | `pop` 归零 | 改快照/还原 |
| `test_system_config_api.py` | `ADMIN_AUTH_ENABLED` | `None -> 'true'` | 补保存/还原 |

复测：该 7 个文件探针输出只剩 1 条 `test_analysis_api_contract.py` 的 `ENV_FILE`，且那是
module 级作用域的探针假象——它在 `_ORIGINAL_ENVIRON` 里保存并在 `:60-65` 还原，挂上票 11 的
基线后会正确还原到基线值。

**修复后的决定性问题**：票 11 的会话级基线重新挂上后，全量 `48 failed / 7728 passed` ——与无基线
**完全一致**（此前是 61）。说明这些泄漏正是 48 → 61 的成因。

**下一步（已部分验证）**：把基线从"会话开始时设一次"加强为"每个用例前清掉 ambient 键"后，
`test_get_config_defaults_public_searxng_instances_off` 由红转绿（全量 48 → 47）。剩下 9 条
（`test_system_config_service` 6 条 `ConfigValidationError` + `test_system_config_api` 3 条）
**不是 env 问题**——per-test 清除没有影响它们。下一步应把探针扩展到**单例状态**
（`Config._instance` / `DatabaseManager._instance` / `ConfigManager` 缓存）：它们单跑全绿、全量
全红，最符合"共享单例被前一个文件留下"的画像。

**单例探针（已实现，噪声过大）**

探针已扩展到 `Config._instance` / `DatabaseManager._instance`（取值 `none` / `set` /
`unimported`，懒加载以免在基线之前触发 `setup_env()`）。全量一次，`_instance` 过渡 **217 条**——
因为几乎每个测试都会创建并重置单例，"none→set / set→none"是常态抖动，无法区分泄漏。

副产品：探针显示 `test_storage.py` 与 `test_tickflow_fetcher.py` 会互相把 `ADMIN_AUTH_ENABLED`
设成 `'true'`——该泄漏已被 per-test 的 ambient 清除兜住（见票 11 第三阶段），不是当前失败成因。

**最小复现尝试（均阴性）**

```
tests/test_storage.py + tests/test_system_config_service.py            → 4 failed（全在 test_storage）
tests/test_tickflow_fetcher.py + tests/test_system_config_service.py   → 243 passed
test_system_config_api.py → test_system_config_service.py              → 1 failed / 259 passed
test_config_env_compat.py → test_system_config_service.py              → 1 failed / 268 passed
```

结论：这 9 条的污染源不在"最像"的邻居里，单靠人工猜组合不划算。

**前缀二分（已跑完，定位成功）**

脚本逻辑：取字典序的 303 个测试文件，二分"前 N 个 + 受害者 `tests/test_system_config_service.py`"，
每轮只判断受害者是否变红。

```
prefix=152 reproduces=True
prefix=76  reproduces=True
prefix=38  reproduces=False
prefix=57  reproduces=True      ← 最小前缀
prefix=48/53/55/56 reproduces=False
→ 最小前缀 57，新增的最后一个文件：tests/test_backtest_card_d.py
→ 'test_backtest_card_d.py + test_system_config_service.py' reproduces=True（7 failed）
```

**精确失败原因**（`--showlocals` 拿到异常里的 `issues`）：

```
items  = [{'key': 'LITELLM_MODEL', 'value': 'openai/kimi-k2.6'}]
errors = [{'key': 'LITELLM_MODEL', 'code': 'unknown_model',
           'message': '... Available models: deepseek/deepseek-flash'}]
```

也就是说：校验时看到的**可用模型目录**被压缩成了 `deepseek/deepseek-flash` 这一个
（`tests/litellm_stub` 的桩模型），于是任何真实模型名都被判成 `unknown_model`。
污染发生在 `test_backtest_card_d.py` 通过 `_load_driver()` 导入
`scripts/backtest_5y_23strategies.py` 的过程中——它替换了 litellm 桩 / 频道注册表 / 默认频道映射。

**两个已排除的猜测**（改动均已回滚，`git diff` 为空）：还原 `LOG_DIR`、
在 fixture 收尾调 `Config.reset_instance()`——都不改变失败。

**下一步**：查 `scripts/backtest_5y_23strategies.py` 导入期对 litellm 桩或频道注册表做了什么，
在 `test_backtest_card_d.py` 的 fixture 里复原；或让校验器不再依赖进程级模型目录。

## 2026-10-26 第二轮：修掉两个触发器，全量仍未动

按上面的下一步执行后的事实：

1. 机制已确认：driver 的 `load_dotenv()` **不带参数**，绕过 `ENV_FILE` 直接把仓库根 `.env`
   灌回 `os.environ`；其中 `LLM_CHANNELS=deepseek` 让校验器只认 `deepseek/deepseek-flash`。
2. 修了 4 个导入该 driver 的测试模块（`test_backtest_card_d` / `test_backtest_pit_universe` /
   `test_backtest_stdio_encoding` / `test_portfolio_sim_integration`）与
   `test_fetch_tushare_stock_list`：统一用 `tests/conftest.py` 的共享 fixture
   `restore_env_after_test`（`pytestmark = pytest.mark.usefixtures(...)`），逐用例做整份
   `os.environ` 快照/还原。
3. **验证**：`4 个 driver 文件 + victim` → 260 passed；`card_d + victim` → 234 passed（此前 7 failed）。
4. **但全量仍是 47 failed / 10 条 system_config 不变** ⇒ 至少有第三个、独立的
   `LLM_CHANNELS` 污染源未找到。二分脚本再跑一次只会找到下一个，修完可能还有。
5. **已回滚的尝试**（均未证明有收益，不留未验证改动）：把 6 个脚本的 `load_dotenv()` 改成
   接受 `ENV_FILE`（行为等价但全量无变化）；`pytest_collection_finish` 里重放基线。

**结论**：逐触发器打补丁是 O(污染源数量) 的工作量，而病根是**生产代码让校验依赖进程级
`LLM_CHANNELS`**。建议改口径：`SystemConfigService` 校验时应只依据"正在编辑的配置"
（env 文件 + 提交项），不读 `os.environ` 里的其它测试残留。这是生产语义决策，超出本票范围，
应单开一票讨论。

## 2026-10-26 第三轮：把 `LLM_CHANNELS` 纳入 ambient 基线（47 → 37）

票 13 定 A（校验继续实时读 env）后，正确做法变成"让每个用例从干净环境开始"，而不是追每一个写入者。
把 `LLM_CHANNELS` 加进 `_AMBIENT_KEYS` 后：

```
test_decision_signal_extractor.py + test_system_config_service.py  → 245 passed
test_backtest_card_d.py + test_system_config_service.py            → 234 passed
全量                                                               → 37 failed / 7739 passed
（原 47；9 条污染型 ConfigValidationError 全部消失）
```

剩下唯一一条 `system_config` 失败是
`test_preview_agent_backend_status_uses_draft_without_persisting`——它**单跑也红**，是真实失败，
不属于本票。

**剩余 37 条的分布**（31 条 nodeid + 6 条子测试级）：`test_market_analyzer_generate_text` 5、
`test_docker_entrypoint` 3、`test_name_to_code_resolver` 3、`test_storage` 3、
`test_fundamental_timeout_guard` 2、`test_miaoxiang_fetcher` 2，其余 14 个文件各 1 条。

## 2026-10-26 第四轮：分析结果缓存（27 → 22）

`test_market_analyzer_generate_text` 的 5 条也是跨用例状态，但**不是环境、也不是单例**：

- 二分把 culprit 定位到 `test_analyze_uses_litellm_fallback_when_codex_cli_config_error_is_fallbackable`；
- 一个临时插件（`pytest_configure` 里包一层 `GeminiAnalyzer._get_generation_backend` 打印
  `id(self)` / `id(backend)`）显示 **victim 根本没调用后端**；
- 顺着 `model_used` 反查，命中 `src/utils/cache.py` 的 `AnalysisCache`：**进程级单例，键是
  `(code, cache_date)`**。两条用例都分析 `600519`、`date` 都为空 → 同键 → victim 直接吃到
  culprit 缓存下来的结果（`analysis_summary="fallback ok"`、`model_used="gemini/gemini-2.0-flash"`）。

修法：该文件加 autouse fixture，在每个用例前后 `get_analysis_cache().clear()`。

结果：`tests/test_market_analyzer_generate_text.py` **5 failed → 146 passed**；全量 `27 → 22 failed`。

**方法结论**：这类"对象身份泄漏"用 `id()` 探针一次就能定性，比读代码猜快得多——先证伪"victim 走了
后端路径"，再顺着结果字段（`model_used`）反查，两步就落到缓存。

## 2026-10-26 第五轮：resolver 的模块级缓存 + 三条"测已删功能"的用例（22 → 19）

`test_name_to_code_resolver` 的 3 条**单跑也红**，所以不是顺序问题。拆成两件事：

1. **缓存隔离**：`src/services/name_to_code_resolver.py` 有 5 个模块级缓存
   （`_akshare_cache` / `_akshare_failure_cache` / `_akshare_merged` / `_names_cache` /
   `_pinyin_cache`），而文件里原有的 `clean_db` **只在 teardown 重置**，于是每个用例开始时继承
   上一个用例的状态，且 `test_akshare_fallback_when_not_in_local` 根本没用该 fixture。
   已抽出 `_reset_resolver_state()` 并加 autouse fixture，在**每个用例前后**都重置。
2. **契约漂移**：`resolve_name_to_code` 的 docstring 明写 "AKShare online fallback has been
   removed"，对应提交 `bbbceea3`（「去掉akshare渠道」）。3 条用例仍在模拟 `_get_akshare_name_to_code`
   并断言在线回退生效——测的是已被移除的能力。已按现契约改写：

| 原用例 | 改为 |
| --- | --- |
| `test_akshare_fallback_when_not_in_local` | `test_local_miss_returns_none_after_akshare_fallback_removal` |
| `test_akshare_exact_fallback_beats_fuzzy_for_non_local_name` | `test_local_exact_and_fuzzy_both_land_on_the_same_code` |
| `test_legacy_fullwidth_input_resolves`（依赖 akshare 补全 000725） | 改用本地映射里的 `ＭｉｃｒｏＳｔｒａｔｅｇｙ → MSTR` |

结果：该文件 `3 failed → 69 passed`；全量 `22 → 19 failed`。

**待观察**：`test_storage::test_provider_turns_do_not_appear_in_visible_or_web_messages_and_delete_with_session`
在最近四次全量里反复偶发，是当前最稳定的 flake 候选。

## 2026-10-26 第六轮：那条"稳定 flake"其实是生产排序缺陷（19 → 18）

`tests/test_storage.py::TestStorage::test_provider_turns_do_not_appear_in_visible_or_web_messages_and_delete_with_session`
在最近四次全量里偶发。实测：**同一文件、同一顺序连跑 5–6 次会红 1–2 次**，所以不是顺序污染，
是真正的非确定性。

失败现场：两条会话消息顺序**互换**（期望 `[user, assistant]`，得到 `[assistant, user]`）。根因在
`src/storage.py::get_conversation_history`：

```python
.order_by(ConversationMessage.created_at.desc()).limit(limit)
...
return [... for msg in reversed(messages)]
```

同一时刻写入的两条消息 `created_at` 相同，缺 `id` tiebreaker → 数据库返回顺序不确定 → `reversed()`
后就成了倒序。隔壁 `get_visible_conversation_messages`（`:3738`）**本来就**用了
`(created_at, id)`，这里漏了。

修法：改为 `order_by(created_at.desc(), id.desc())`。这是**生产修复**，不是测试让步——真实场景里
同一秒内的问答在 UI 上可能渲染成倒序。

验证：`tests/test_storage.py` 连跑 **8 次全绿**（修前 5–6 次必红 1–2 次）；全量 `19 → 18 failed`。

## 待定决策

## 待定决策

### D1. 定位方法：先造"纯度哨兵"，还是直接前缀二分？

- **(a) 纯度哨兵**：在受害文件开头加一个断言式 fixture，快照关键入口（`ENV_FILE`、
  `REALTIME_SOURCE_PRIORITY`、`ADMIN_AUTH_ENABLED`、`Config._instance`、`DatabaseManager._instance`），
  一旦与"干净基线"不符就**指名道姓**报出被污染的键，而不是报 `ConfigValidationError`。
  - 优点：一次全量跑就能拿到泄漏的具体入口；把"猜污染源"变成"读诊断"。
  - 代价：要维护一份"干净基线"的定义（与票 11 的 deny-list 高度重叠）。
- **(b) 直接前缀二分**：把字典序文件列表切两半，前半 + 受害文件 → 判断是否复现，递归。
  - 优点：不依赖对污染种类的假设。
  - 代价：每轮 3–8 分钟，约 8–9 轮；且只找到"哪个文件"，不告诉你"泄漏了什么"。

➡️ 推荐 **(a) 先做**，用它的输出决定是否需要 (b)。这也是唯一能防止同类问题复发的手段。

### D2. 修的方向：加固受害方，还是加固污染方？

- **(a) 加固受害方**：让这些用例自己 `Config.reset_instance()` / 隔离环境 / 不依赖全局默认值。
  - 优点：局部、可预测；符合"测试应自洽"。
  - 代价：同样的问题会在下一个受害文件上重演。
- **(b) 加固污染方**：找到泄漏者，在它的 teardown 里复原。
  - 优点：从源头断掉，全仓受益。
  - 代价：需要 D1 的输出；且"复原"写错会制造新的隐性耦合。

➡️ 推荐 **两者都做，但次序是 (a) 先止血、(b) 再根治**，且 (b) 只针对 D1 指名的那一处，不做
"全仓加 reset fixture" 这种覆盖面过大的动作。

### D3. 是否引入随机顺序来持续验证"顺序无关"？

- **(a) 不引入**：继续靠"全量 vs 单跑"的人工比对。
- **(b) 把 `pytest-randomly` 加成开发依赖，但**不进阻断 CI**，只在本地/夜间用固定 seed 跑。
- **(c) 进阻断 CI**：随机顺序会让既有污染随机爆红，CI 立刻变成噪声源。

➡️ 推荐 **(b)**。只有在 D1/D2 收敛、全量连续多次稳定之后，才考虑升到 (c)。

### D4. 与票 11 的次序

票 11（环境基线）会让一部分"`.env` 派生值"污染消失，但不会消除单例/缓存污染。

➡️ 推荐 **先 12 后 11**（此条在 2026-10-26 被实测推翻过一次，见下）。

**实测依据**：2026-10-26 曾按"先 11"实现会话级环境基线，全量从 `48 failed` 变成 `61 failed`
（+14 / −1）。新增失败里 10 条在 `test_intelligence_api.py`、2 条是 `test_data_capability_service`
/ `test_usage_api` 的 401 复发——原因是**前面的测试改掉 `ENV_FILE` / `ADMIN_AUTH_ENABLED` 后没有
复原**，原先的 per-file fixture 每次都会重新隔离，统一基线只设一次就把这些泄漏暴露出来。该实现
已完整回滚。结论：**先把"谁在改环境且不复原"解决掉（本票），再谈统一基线（票 11）**。

## 验收标准

1. `tests/test_system_config_service.py` 与 `tests/test_system_config_api.py` 在**全量与单跑**下失败
   集合一致（当前差异 10 条）。
2. D1 的哨兵能在污染发生时指出**具体的泄漏入口**，而不是只报一条 `ConfigValidationError`。
3. 修复后，全量连续跑 3 次失败集合完全相同（消除"同一命令两次结果不同"）。
4. 不删除、不 `xfail`、不放宽任何断言来达成上述目标。
5. 若引入随机顺序（D3 选 b），必须记录使用的 seed 与命令，且不得接入阻断 CI。

## 不在本票范围

- **环境变量基线**（开发者 `.env` 泄漏）：票 11。
- **native Windows 平台契约失败**：票 10 的白名单机制已覆盖；本票修完对应行会变 `STALE`。
- **`test_storage.py` 3 条 `WinError 32`**：根因疑似迁移代码里裸 `sqlite3.connect` 未关闭，
  属票 09「其余零散」，与顺序无关。

## Notes

- 判据不是"变绿"，而是"**同一份代码、无论跑哪些文件、跑几次，失败集合都一样**"。绿是副产品。
- 全量当前耗时 8–13 分钟（同机波动），所以本票的每一次验证都要算成本；先用 D1 减少试错轮次。
- 这条不只是测试卫生问题：如果污染来自生产单例的隐式全局状态，同样的耦合在生产里也会以
  "先调 A 再调 B 才出错"的形式出现。根因定位到哪一层，决定这张票要不要扩到生产代码。

# 11: 测试环境基线——把开发者本机环境从测试结果里摘出去

**What to build:** 让测试结果不再取决于"跑测试的人本机有什么"。当前仓库根目录的 `.env` 在
`import src.config` 时就被 `setup_env()` 写进 `os.environ`，之后所有从环境派生的配置（认证开关、
实时源优先级、SearXNG 开关等）都会读它。结果是同一份代码在不同机器上跑出不同的失败集合，
而且这些失败混在 native Windows 白名单里，看起来像平台问题。

**Blocked by:** None。

**Gate:** `none`
**Signal set:** `none`

**Status:** ready-for-human（需要先定"测试是否允许读开发者 `.env`"的口径，见「待定决策」）

## 背景（2026-10-26 实测）

票 09 的 52 条 `TODO` 里，有 5 条被归到"`.env` 污染"，但真正动手时发现机制比想象的深：

| 现象 | 实测证据 | 结论 |
| --- | --- | --- |
| `test_data_capability_service` / `test_usage_api` 全站 401 | 断言 `401 != 200`；把 `ENV_FILE` 指向不存在路径后转绿 | 读的是开发者 `.env` 里的 `ADMIN_AUTH_ENABLED=true` |
| `test_config_env_compat` / `test_tickflow_manager_routing` 实时源优先级不符 | 期望 `tencent,akshare_sina,efinance,akshare_em`，实际 `tencent,efinance,tushare` | 该列表由**多个环境变量派生**，不是单一键 |
| 只删 `REALTIME_SOURCE_PRIORITY` 的尝试 | 目标用例仍红，同文件原本通过的 `test_config_loads_tickflow_priority` 反而变红 | 单键隔离会破坏邻居，已整块回滚 |
| `test_system_config_service.py` 全量红 8 条 | **单跑 225 passed** | 不是 `.env`，是跨文件顺序污染 |
| `test_system_config_api.py` 全量红 3 条 | 单跑只剩 1 条红 | 2 条污染 + 1 条真实失败 |

**关键机制**：`setup_env()` 在 `import src.config` 时就把 `.env` 的值**写进 `os.environ`**。
因此**事后用 fixture 改 `ENV_FILE` 只对"调用时才读路径"的代码有效**（`src/auth.py` 就是这种，
所以 401 能修好），对"已经写进 `os.environ` 的派生值"无效。这解释了为什么同一手法一半有效一半无效。

**已经手工修过两次**：早先 `tests/test_intelligence_api.py`（10 条）、本轮
`test_data_capability_service` / `test_usage_api`（2 条）。每次都要新加一个 per-file fixture，
而本轮已经因为"逐条隔离"弄红了一个原本通过的用例。这条路不 scale。

## 已尝试且失败的方案

- **per-file autouse fixture 设 `ENV_FILE`**：只对 2/5 有效；对派生配置无效，且对 `test_tickflow_manager_routing`
  产生了新失败（`test_config_loads_tickflow_priority`），已回滚。
- **只 `delenv` 单个键**：同文件邻居用例依赖该键，会互相打架。

## 实现（2026-10-26，两阶段）

**第一版（失败并回滚）**：在 `tests/conftest.py` 顶部把 `ENV_FILE` 指向不存在路径并按 D2 清
deny-list，同时把 3 处手工隔离迁移到统一机制。实测全量 `48 → 61 failed`（+14 / −1），新增失败
集中在 `test_intelligence_api.py`（10 条）与 `test_data_capability_service` / `test_usage_api`
的 401 复发。根因：前面若干测试改掉 `ENV_FILE` / `ADMIN_AUTH_ENABLED` 后**没有还原**，原先的
per-file fixture 每次都会重新隔离，统一基线只设一次就把这些泄漏暴露出来。该版已回滚。

**第二阶段（先修泄漏，再上基线）**：按票 12 用环境纯度探针定位并修掉 5 个文件的泄漏
（详见票 12「扫描结果」），然后重新挂上同一份基线。

实测（native Windows，同一命令）：

```
无基线        → 48 failed / 7728 passed / 289 skipped
有基线        → 48 failed / 7728 passed / 289 skipped      ← 失败集合完全一致
分项：test_intelligence_api / test_data_capability_service / test_usage_api 均不再出现
```

结论：**会话级基线已落地**，验收标准 1（有/无开发者 `.env` 得到同一失败集合）在本机达成。

**第三阶段（按用例强制）**：仅"会话开始时设一次"还不够——某个用例把 `SEARXNG_PUBLIC_INSTANCES_ENABLED`
写进 `os.environ` 就能污染后面所有用例（`_build_runtime_display_config_map` 直接读 `os.environ`）。
因此 `tests/conftest.py` 增加了 autouse fixture：**每个用例执行前**把 `_AMBIENT_KEYS` 三个键清掉。
`ENV_FILE` 刻意不在其中——多个模块在 import 期就设它，按用例覆盖会打坏它们。

效果：`test_get_config_defaults_public_searxng_instances_off` 由红转绿，全量 `48 → 47 failed`。

**未做**：票 11 验收标准 4 要求把 3 处历史的 per-file `ENV_FILE` fixture
（`test_intelligence_api.py` / `test_data_capability_service.py` / `test_usage_api.py`）删掉并
迁到统一机制。它们在基线之上已经冗余但无害；删除需要再跑一次全量验证，本轮未做。

## 待定决策

**另一条归因更正**：`test_config_env_compat` / `test_tickflow_manager_routing` 那 2 条**不是
`.env` 污染**。`Config._resolve_realtime_source_priority()` 的代码默认值就是
`tencent,efinance,tushare`，而两个用例断言 `tencent,akshare_sina,efinance,akshare_em`，且用
`patch.dict(clear=True)` 清空了环境——所以是**断言与代码默认值不同步**，在所有平台都会红。
它们不属于本票，应从「背景」表的 `.env` 桶里剔除（已在票 09 更正）。

## 待定决策

### D1. 隔离的落点：import 期一次性设置，还是 per-test fixture？

- **(a) `tests/conftest.py` 在 import 期把 `ENV_FILE` 指向一个不存在的路径**，并且**在任何测试
  模块 `import src.config` 之前**执行。
  - 优点：唯一能拦住"`setup_env()` 已把值写进 `os.environ`"的那类污染；一处生效，覆盖全部用例。
  - 代价：改变全仓 7000+ 用例的初始环境；需要给"就是要测 `.env` 行为"的用例一个显式入口。
- **(b) 继续 per-file fixture**。
  - 优点：改动局部。
  - 代价：已验证一半无效；文件数会持续增长。

➡️ 推荐 **(a)**，配套一个显式入口（见 D3）。

### D2. 隔离的范围：只挡 `ENV_FILE`，还是连派生键一起？

- **(a) 只挡 `ENV_FILE`**：最小，但已证明挡不住派生配置。
- **(b) `ENV_FILE` + 一份"已观察到会泄漏的键"deny-list**（每条注明是哪张票/哪个用例发现的）。
- **(c) 除白名单外清空 `os.environ`**：最强，但会打掉 `PATH` / `SystemRoot` / `USERPROFILE` 等
  运行时必需项，风险最高。

➡️ 推荐 **(b)**，deny-list 集中在一个文件里，每条带"为什么在这里"。初始条目至少包含
`ADMIN_AUTH_ENABLED`、`REALTIME_SOURCE_PRIORITY`、`SEARXNG_PUBLIC_INSTANCES_ENABLED`。

### D3. 需要真实 `.env` 的测试怎么写？

- **(a) 提供 `tests/support/env_file.py` 之类的 helper**：`with isolated_env_file(...)` 写入临时
  `.env`、显式刷新配置、退出时复原。
- **(b) 允许用例自己 `os.environ[...] = ...`**（现状）。

➡️ 推荐 **(a)**。否则"显式 opt-in"只是句口号，实际仍会散落成各处手写环境变量。

### D4. 顺序污染是否并入本票？

**建议不并。** `test_system_config_service.py` 单跑 225 passed 说明问题在跨文件残留状态
（共享单例 / 模块级缓存），修法是另一套（按共享资源做 reset fixture），并入会让本票无界。
建议单开票 12，本票只负责"环境基线"。

## 验收标准

1. **同一份代码、两种环境、同一个失败集合**：在"仓库根目录有开发者 `.env`"与"没有该文件"两种
   状态下跑 `python -m pytest -m "not network"`，失败集合必须完全一致（记录两次的 nodeid 列表并 diff）。
2. 除显式 opt-in 的用例外，没有测试读取仓库根目录 `.env`。
3. 隔离范围的 deny-list 与 `ENV_FILE` 处理集中在**一个**文件里，每条带理由；不允许再新增
   per-file 的同义 fixture。
4. 已经手工修过的三处（`test_intelligence_api.py`、`test_data_capability_service.py`、
   `test_usage_api.py`）在迁移到统一机制后仍然通过，并删除各自的本地 fixture。
5. 不改变任何生产代码行为；只动 `tests/` 与（必要时）`setup.cfg`。

## 不在本票范围

- **跨文件顺序污染**（`test_system_config_service.py` 8 条、`test_system_config_api.py` 2 条）：
  建议票 12。
- **native Windows 已知失败白名单**：票 10 已落地；本票修完后对应行自然消失（checker 会报
  `STALE`，需删除）。
- **`test_storage.py` 3 条 `WinError 32`**：根因疑似迁移代码里裸 `sqlite3.connect` 未关闭，
  属票 09「其余零散」。

## Notes

- 判据不是"让测试变绿"，而是"让失败与机器无关"。若某个用例真的需要读本机 `.env`，那它就应该
  显式声明，而不是靠运气活在全量里。
- 动这项前先量基线：当前 native Windows 全量 `48 failed / 7728 passed / 289 skipped`，
  junit 可见失败 `43`。本票的目标不是把这 43 降下来，而是让下一次数字的变化只反映代码。

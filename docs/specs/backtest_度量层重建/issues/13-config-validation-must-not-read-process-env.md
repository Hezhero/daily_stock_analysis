# 13: 配置校验不应依赖进程级 `LLM_CHANNELS`

**What to build:** 让 `SystemConfigService` 在读取/预览/校验配置时，只依据"正在编辑的那份配置"
（保存的 env 文件 + 本次提交项 + **启动时**注入的运行时环境），而不是读取运行期任意时刻被写脏的
`os.environ`。目前只要进程里任何一个代码路径把 `LLM_CHANNELS` 之类写进环境，后续所有配置校验都会
套用它，把无关模型判成非法。

**Blocked by:** None（票 12 已把"谁在写脏环境"定位到若干触发器，但本票不依赖它们全部修完）。

**Gate:** `none`
**Signal set:** `none`

**Status:** ready-for-human（需要先定"运行时环境"的边界，见「待定决策」）

## 背景（2026-10-26 实测）

票 12 的前缀二分把 `tests/test_system_config_service.py` 的全量失败定位到
`tests/test_backtest_card_d.py`，再用 `--showlocals` 拿到真实错误：

```
items  = [{'key': 'LITELLM_MODEL', 'value': 'openai/kimi-k2.6'}]
errors = [{'key': 'LITELLM_MODEL', 'code': 'unknown_model',
           'message': '... Available models: deepseek/deepseek-flash'}]
```

链路：

1. `scripts/backtest_5y_23strategies.py:133` 调 `load_dotenv()`（**不带参数**，绕过 `ENV_FILE`），
   把仓库根 `.env` 灌回 `os.environ`，其中含 `LLM_CHANNELS=deepseek`；
2. `SystemConfigService._build_runtime_display_config_map()` 遍历 `os.environ`，把所有注册键当作
   "运行时注入值"合并进当前配置；
3. `_validate_llm_runtime_selection()` 用合并后的 map 算 `available_models`，于是只剩
   `deepseek/deepseek-flash`，任何真实模型名都被判 `unknown_model`。

**关键证据**：`test_system_config_service.py` 单跑 **225 passed**、全量必红；修掉 5 个"导入脚本即
写脏环境"的测试模块后，`card_d + victim` 从 7 failed 变 234 passed，**但全量仍有第三个独立触发源**。
也就是说：触发源数量不确定，而缺陷是"校验读了不该读的进程状态"。

## 待定决策

### D1. "运行时环境"的边界

- **(a) 完全不读进程环境**：校验只看 env 文件 + 提交项。
  - 优点：最简单、彻底免疫。
  - 代价：Docker `env_file` / `--env-file` 部署下，注入的配置在 Settings 页面上会变成"不存在"——
    这是现有功能（`_build_runtime_display_config_map` 的 docstring 明确为此存在）。
- **(b) 只认启动时快照**：在应用启动（`setup_env()` 期间）把进程环境里"属于配置注册键"的部分
  冻结成一份不可变映射，服务只读这份快照。
  - 优点：Docker 语义不变；运行期被测试/脚本写脏的环境不再影响校验。
  - 代价：新增一个"启动快照"的概念与生命周期，需要明确它在 `Config.reset_instance()` 时是否重建。
- **(c) 维持现状，继续逐个修测试**：已证明是 O(污染源数量) 的工作量，否决。

➡️ 推荐 **(b)**。

### D2. 快照放在哪一层

- `setup_env()`（现有唯一的环境装载入口，`src/config.py:869`）
- 服务构造时现拍（每次 `SystemConfigService(...)`）
- 进程级单例（模块级，首次访问时初始化）

➡️ 推荐 **`setup_env()`**：它是"环境进入进程"的唯一既成入口，放这里语义最直白，也天然覆盖
CLI、Web、Docker 三条路径。需要回答的细节：`override=True` 的重载路径是否要刷新快照。

### D3. 测试如何证明修好了

必须有一条**不依赖"某个人恰好修好了所有测试"**的回归测试：

1. 启动后修改 `os.environ["LLM_CHANNELS"]`；
2. 断言 `get_config()` / 校验结果**不受影响**；
3. 再断言"启动时就在环境里的键仍被当作运行时注入"（保护 D1(b) 的 Docker 语义）。

➡️ 推荐做成两条对称用例，放在 `tests/test_system_config_service.py`。

### D4. 与票 12 的关系

票 12 已落地的 5 个测试模块守卫（`restore_env_after_test`）**不删**——它们防的是"测试互相污染"，
本票防的是"生产代码读错来源"，两件事。等本票落地、且全量连续 3 次稳定后，再评估是否收敛。

## 验收标准

1. 在同一个 pytest 进程里先导入 `scripts/backtest_5y_23strategies.py`（会写脏 `LLM_CHANNELS`），
   再跑 `tests/test_system_config_service.py` / `tests/test_system_config_api.py` → 全绿，
   **且不依赖任何 per-module env 守卫**。
2. D3 的两条对称用例存在并通过。
3. Docker `env_file` 语义不回退：启动时注入的配置仍在 Settings 可见（有测试或明确的手工验证记录）。
4. 不改变 CLI 主流程与 `main.py` 的行为。
5. native Windows 全量失败集合不再包含 `ConfigValidationError` 相关的 10 条。

## 不在本票范围

- **把 5 个脚本的 `load_dotenv()` 改成接受 `ENV_FILE`**：已试过，行为等价但全量无变化，已回滚；
  它们"读仓库 .env"对独立运行是正确行为。
- **测试间环境泄漏的其余触发器**：票 12。
- **`SystemConfigService` 的其它校验规则**：本票只改"数据来源"，不改规则本身。

## Notes

- 判据不是"测试变绿"，而是**"校验结果只取决于输入配置"**。绿是副产品。
- 这条缺陷在生产里同样成立：任何长驻进程里被改过的环境（插件、子进程回调、热重载）都会污染配置
  校验，只是不容易被看见。测试只是把它放大成了红。

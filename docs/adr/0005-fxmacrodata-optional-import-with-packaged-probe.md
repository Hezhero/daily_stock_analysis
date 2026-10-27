# 0005. FXMacroData 客户端降为可选导入，发布产物继续必需

- 状态: 已接受（2026-10-25，用户显式授权）

## 背景

`fxmacrodata-public-client` 在 `requirements.txt` 固定为 GitHub Release 的 wheel：PyPI 与国内镜像
都没有这个包，访问不到 `github.com:443` 的环境（例如本机 native Windows 开发环境、内网 CI）
无法安装它。缺失时 `src/agent/tools/fxmacrodata_tools.py` 的模块级
`from fxmacrodata_public import FXMacroDataClient, list_operations` 直接抛 `ModuleNotFoundError`，
并沿 `factory.get_tool_registry()` 级联成 3 个 collection error 与约 43 个用例失败（ticket 09）。

但该模块**已经声明过降级意图**：`_available_operations()` 捕获操作目录加载失败、记一条 WARNING
并返回空列表，其 docstring 写明"打包故障应让 FXMacroData 工具不可用，而不是拖垮整个 agent
工具注册表"。也就是说"依赖不可用时工具集为空"本就是既有设计，只是那条模块级硬导入让这条路径
永远走不到——包整体缺失时连模块都 import 不了。

同时 `main.py` 在 `DSA_PACKAGED_IMPORT_PROBE=src.agent.factory` 下断言注册表里存在
`fxmacrodata_` 工具。该探针只由桌面端打包脚本触发（`desktop-release.yml`，`windows-latest`），
不在 `main.py` 的正常运行路径上；而 CI 的 `backend-tests` 只在 `ubuntu-latest` 上跑三分片，
native Windows 从不参与全量基线。所以"开发环境没有这个包"和"发布产物没有这个包"是两件不同的事，
现状却用同一条硬断言语义同时表达。

## 决策

把该客户端降为**可选导入**，把发布产物的完整性交给打包探针，而不是交给开发环境的依赖安装状态：

- 包缺失时 `fxmacrodata_tools` 导入成功、导出空 `ALL_FXMACRODATA_TOOLS`，并记录一次明确
  WARNING；不得让 `ImportError` 冒泡到 `factory.get_tool_registry()`（否则 `risk_agent`、
  `portfolio_agent`、`intel_agent` 三个 specialist 连带失败）。
- 模块作用域只在 `try` 里做可选导入；`ImportError` 时把 `FXMacroDataClient` / `list_operations`
  绑定为 `None`。客户端构造与调用严格在 handler 调用期经模块级名字解析，因此 `None` 时返回
  `unavailable` 而不是抛 `TypeError`。
- `fxmacrodata_tool_names(*operations, require_integration=True)`：操作目录不可用时，**无参**返回
  `[]`；**点名指定了 operation** 则显式报错，不得静默返回空列表——静默会让 allowlist 无声缩短，
  属于不可观测的契约漂移。`risk_agent` / `portfolio_agent` 在类体里点名了具体操作，必须能在缺
  依赖时正常导入，因此这两处显式传 `require_integration=False`；它们仍然受下面那条一次性降级
  信号覆盖，不是"无声"的例外。
- 降级必须可观测：一条**只发一次**、带原因类别的启动日志（不是每次查目录都刷 warning），外加
  `fxmacrodata_availability()` 作为可供诊断面或测试读取的原因字段；"包缺失"与"目录读不到"
  必须可区分。
- `main.py` 的冻结产物探针**保持原样**并继续断言工具存在。它现在承担双重职责：既是打包完整性
  检查，也是本决策的补偿控制——开发环境可以缺，发布产物不可以缺。
- `requirements.txt` 的 pin 不变。本 ADR 不改变"推荐安装方式"，只改变"装不上时的行为"。

## 后果

- 3 个 collection error 归零，约 43 个用例恢复可执行；native Windows 全量失败集从 119 降到
  约 73（剩余项仍待逐项分类）。
- 无 GitHub 访问的环境可以跑主流程，但会丢失 fx 工具（23 个 REST 操作 + 49 个 MCP 工具）。
  这是真实的能力缺失，因此不允许只剩一条 WARNING 就算交代。
- 发布产物保证没有削弱：探针仍在 `windows-latest` 上强制该断言。
- 该模式**只适用于有显式补偿控制的集成类依赖**。它不构成"任何缺依赖都可静默降级"的先例：
  没有打包探针或等价强断言的依赖，缺了仍应失败。

## 备选方案

- **维持必需依赖，只在文档里前置说明"必须能访问 GitHub Release"。** 否决。collection error
  无法用"失败白名单"表达——白名单只能列出 failed，collection error 需要
  `--continue-on-collection-errors` 才能绕过，等于让默认命令继续返回非零却不说明原因。
- **可选导入，同时放宽 `main.py` 的探针断言。** 否决。那会把"发布产物缺依赖"也变成静默降级，
  恰好丢掉这个决策唯一的补偿控制。
- **可选导入，且 `fxmacrodata_tool_names()` 在目录不可用时一律返回 `[]`。** 否决。调用方点名要
  具体 operation 时返回空，是契约漂移且不可观测。
- **把 wheel vendored 进仓库或自建镜像。** 否决。体积、供应链与更新责任都超出本票范围；
  "可选导入 + 探针"两条已经覆盖目标。

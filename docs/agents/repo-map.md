# 仓库速览（repo map）

`AGENTS.md` 只保留协作规则；目录职责与关键入口放在这里，避免规则文件持续膨胀。

- 项目定位：股票智能分析系统，覆盖 A 股、港股、美股。
- 主流程：抓取数据 -> 技术分析/新闻检索 -> LLM 分析 -> 生成报告 -> 通知推送。
- 关键入口：
  - `main.py`：分析任务主入口
  - `server.py`：FastAPI 服务入口
  - `apps/dsa-web/`：Web 前端
  - `apps/dsa-desktop/`：Electron 桌面端
  - `.github/workflows/`：CI、发布、每日任务
- 核心职责：
  - `src/core/`：主流程编排
  - `src/services/`：业务服务层
  - `src/repositories/`：数据访问层
  - `src/reports/`：报告生成
  - `src/schemas/`：Schema / 数据结构
  - `data_provider/`：多数据源适配与 fallback
  - `api/`：FastAPI API
  - `bot/`：机器人接入
  - `scripts/`：本地脚本
  - `.github/scripts/`：GitHub 自动化脚本
  - `tests/`：pytest 测试
  - `docs/`：文档与说明
  - `docs/agents/`：协作规则、验证矩阵、平台边界与已知失败
  - `docs/specs/`：本地 issue / 规格与变更记录
  - `result/`：回测产物；`result/指标契约_metric_contract_v1_20261017.md` 是冻结度量口径，`result/gates/` 是翻转门槛判决与 registry
- 回测 driver seam（改 `scripts/backtest_5y_23strategies.py` 前先沿这条链定位）：`compute_market_ok` → `main()` 中按 `date` merge 进 `df_all` → `_regime_col` / `_entry_mask` 统一取列 → 启动日志与 `result/` 产物；口径解析与测试 seam 见 `docs/specs/auto-regime-mode/spec.md` Testing Decisions。

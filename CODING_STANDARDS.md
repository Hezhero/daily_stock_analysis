# CODING STANDARDS（review 阶段读取）

本文件只收录**判断类**规则：那些 lint / typecheck / 脚本无法判定、必须由 reviewer
在两轴 review 的 Standards 轴上人工收敛的跨文件一致性。机械类问题（语法、未定义名、
import 形状、文件位置、元数据字段）一律交给已有的确定性检查，不写在这里：

- `scripts/ci_gate.sh flake8`（`E9,F63,F7,F82`，含 `F821 undefined name`）
- `scripts/check_ai_assets.py` / `scripts/check_spec_tickets.py`
- `scripts/ci_gate.ps1 offline-tests` 全量套件

`docs/CONTRIBUTING.md` 仍管 PEP 8 / docstring / 注释等基础约定；本文件是它的补充，
不重复其中任何一条。实现阶段不必读本文件；review 阶段读。

## 单一来源：同一语义出现在两处，必须可判定一致

一个概念若在两处表达（日志 vs 产物、冻结定义 vs 登记规则、spec 状态 vs ticket 状态、
CLI 帮助 vs 运行行为），要么两处都从**同一个值/函数**派生，要么有一处测试直接断言二者
相等。两条独立表达式即使都正确，也会在下一次改动时静默分叉——这是 review 必须挡住的
漂移，不是「顺手优化」。

判定手法：

- 找出本次 diff 里**语义相同、写法不同**的两处（同一个 override 标记的两种字符串、
  同一个规则版本的两个三元表达式、同一个阈值的内联字面量与共享常量）。
- 要求收敛为单源（例如 `format_regime_log_summary` 复用 `format_override_marker`），
  或补一条针对二者相等性的回归测试。
- 特别检查**翻转默认值 / 新增第 4 个取值**这类改动：所有 `if mode == ...` 分支、
  fallback 链、日志与产物出口是否用了同一个判据。

## 冻结定义与已登记规则必须解耦

「冻结度量定义」与「已登记实验参数」即使当前取值相同，也**不得共享同一个常量**。
冻结项（如 `compute_market_ok` 的三列算法及其内联阈值）改动须走 ADR；登记项
（如 auto 选择器的阈值与规则版本）允许经登记路径覆盖并计入 `config_hash`。把二者
别名到同一常量，会让「改登记阈值」静默改写冻结定义，抹掉 ADR 0006 确立的分界。

## 文档状态必须与 ticket 状态一致

CHANGELOG / spec / ADR 里声称「已实现 / 已落地」的表述，必须与对应 ticket 的
`Status:` 与验收框一致。ticket 仍有未勾选项或为 `ready-for-human` 时，文档只能写
「已交付脚手架 / 待授权」，不得写「落地」。这是 `AGENTS.md` §8「PR 描述与实际改动
实质性矛盾」的同类阻断项，review 阶段逐条核对。

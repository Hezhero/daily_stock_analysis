# CODING STANDARDS（review 阶段读取）

本文件只收录**判断类**规则：那些 lint / typecheck / 脚本无法判定、必须由 reviewer
在两轴 review 的 Standards 轴上人工收敛的跨文件一致性。机械类问题（语法、未定义名、
import 形状、文件位置、元数据字段）一律交给已有的确定性检查，不写在这里：

- `scripts/ci_gate.sh flake8`（`E9,F63,F7,F82`，含 `F821 undefined name`）
- `scripts/check_ai_assets.py` / `scripts/check_spec_tickets.py`
- `scripts/ci_gate.ps1 offline-tests` 全量套件

`docs/CONTRIBUTING.md` 仍管 PEP 8 / docstring / 注释等基础约定；本文件是它的补充，
不重复其中任何一条。实现阶段不必读本文件；review 阶段读。

## 扫描目录后删除或改写文件

凡「枚举一个目录，然后删除或改写其中一部分文件」的代码（清理陈旧缓存、裁剪产物、轮转日志），
必须做到三件事，因为这类代码的失败模式都是**静默**的：

1. **先收集元数据，再排序**。不要在校验或排序的键里直接调 `path.stat()`：文件可能在
   `glob()` 与 `stat()` 之间被另一个进程删掉（并发运行、外部清理、备份工具），一次
   `FileNotFoundError` 会让整轮清理中断，并把计数留在初值上——日志随后报告「保留 0 个」，
   而磁盘上的文件一个没少。
2. **按文件粒度容错**。单个文件消失是**目标已达成**，不算失败；取不到大小就按 0 计入
   字节数，不要因此跳过删除。权限等真失败计入 `failed` 并告警，绝不向外抛。
3. **必须有「文件在扫描期间消失」的测试**。这是唯一能证明前两条没写错的检查；
   用 monkeypatch 让 `Path.stat` 对某个名字抛 `FileNotFoundError` 即可。

## 默认值与开关

CLI 默认值是**同一语义散落在最多五处**的典型：argparse `default=`、函数签名默认值、
`--help` 文本、文档（runbook / CHANGELOG）、以及测试里的期望值。任一处独立书写，都会在
下一次调整时静默分叉。

- 默认值只在**一个常量**里定义（如 `CACHE_KEEP_DEFAULT`），其余各处引用它。
- `--help` 用 f-string 插入该常量，不要重抄数字。
- 补一条测试锁定 `CLI 默认 == 函数默认 == help 文本 == 文档所述`：读
  `_build_parser().parse_args([])`、`inspect.signature(...).parameters[...].default`
  与 `format_help()`，断言三者一致。

## 破坏性默认行为必须落票

当一次改动的**默认行为**会删除或覆盖用户机器上的既有数据（清理缓存、清空产物目录、
重写历史文件），仅靠口头确认不够：`AGENTS.md` §1.3 要求人类拍板，而
`scripts/check_spec_tickets.py` **只能校验已存在的票**，对「本该有票却没有票」是盲的。

- 在 `docs/specs/<feature>/issues/` 下开一张票，把裁决逐条写成验收框，并把**授权来源**
  （谁、何时、依据哪次裁决）写进票的 Comments。
- 票的 `Gate:` / `Signal set:` 按实际影响填：纯本地产物管理用 `Gate: none` +
  `Signal set: none`；改了信号或可开仓判定才需要 `v1-signal` + 理由。

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

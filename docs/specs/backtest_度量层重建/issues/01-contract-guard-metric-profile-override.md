# 01: 契约守卫改造：min_resonance_families 由冻结常量 → 度量画像 + 登记覆盖

**What to build:** 让「度量契约只冻结度量口径、不冻结可部署参数」在代码里真正成立。把契约守卫从「`min_resonance_families` 必须等于 2」改为「`SimConfig` 默认值必须匹配冻结度量画像；已登记的实验参数允许显式覆盖」。覆盖时打出 warning 并改变 `config_hash`；默认值不变、基线逐位可复现。同步修订《指标契约》文档里的冻结语义与守卫说明。这是让后续翻参变简单的 prefactor。

**Blocked by:** None (can start immediately)

**Gate:** `none`
**Signal set:** `none`

**Status:** ready-for-human

- [x] 默认配置跑出的 `config_hash` 仍为已发布基线值，指标逐位不变。
- [x] 显式覆盖已登记的实验参数（如 `min_resonance_families`）时：发出 warning，且 `config_hash` 改变；结果可复现（同参两次运行逐位一致）。
- [x] 当 `SimConfig` 默认值与冻结度量画像不符时，仍抛 `RuntimeError`（画像本身继续被守卫，不允许静默调参）。
- [x] 《指标契约》文档与守卫行为一致：冻结项 = 成本 0.35% 往返 / T+1 盯市 / PIT 宇宙 / 去簇共振定义 / `next_open` 入场；实验参数 = `min_resonance_families` 等。
- [x] 守卫与覆盖两条路径都有单元测试覆盖（契约守卫 seam）。

**Note:** 2026-10-19 用户批准扩展：同步将持仓上限 / 最大持有 / 持仓簇上限 / 行业上限降级为已登记实验参数（见《指标契约》§2.1）；本 ticket 的 `min_resonance_families` 验收不变。

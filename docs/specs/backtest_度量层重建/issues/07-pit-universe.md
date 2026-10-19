# 07: PIT 宇宙落地（roadmap T0.4）

**What to build:** 把回测股票池从静态 `list_status='L'` 改为 point-in-time 宇宙：窗口内曾上市、以及后续退市的股票，在其可交易期间进入样本。这是 ADR 0001 列出的**冻结度量口径**之一（PIT 宇宙），但目前实际仍是 L-only——本卡把现实与契约对齐。

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] 用 `delist_date` 等时点信息构造 PIT 池；窗口内退市的股票在其可交易期间进入样本，而非被整体剔除。
- [ ] 与 L-only 池并列给出同口径指标，量化差异（重点看反转、涨停、小市值类策略的绩效变化）。
- [ ] 数据源失败按 AGENTS.md 的数据源 / fallback 约定降级，不拖垮主流程；数据缺失时明确告警而非静默回退到 L-only。
- [ ] 若触发口径变化，`config_hash` 相应改变；账本记录 PIT 与 L-only 的对照结果。
- [ ] 落地后，若 03/04/05 已在 L-only 池上翻转默认值，须在 PIT 池上重验这些翻转。

**Notes:** 与 01、02 同属无阻塞的 frontier。已知风险：个股 PIT 需要退市股清单，行业 PIT 曾因源端 `out_date` 全空而数据受阻（见账本 §9.11 P3-❻）——数据可得性需先确认。当前 L-only 会系统性抬高部分策略绩效，本卡决定裁判口径是否可信。

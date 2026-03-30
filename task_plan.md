# Task Plan: 修复 MiniMax LiteLLM 配置问题

## Goal
修复日志中 LiteLLM 调用 MiniMax-M2.7 模型的警告和错误，特别是：
1. 模型成本计算失败 (This model isn't mapped yet)
2. LITELLM_MODEL 与渠道配置不匹配
3. 确认 MiniMax API 通过正确的渠道配置工作

## Current Phase
Phase 3: 验证修复

## Phases

### Phase 1: 分析问题根因
- [x] 分析日志错误信息
- [x] 读取 LiteLLM 相关代码 (llm_adapter.py, config.py, config_registry.py)
- [x] 检查 .env 配置文件
- [x] 确认 MiniMax API 配置状态
- **Status:** complete

### Phase 2: 实施修复
- [x] 修复 LITELLM_MODEL 前缀不匹配问题 (openai/MiniMax-M2.7 → anthropic/MiniMax-M2.7)
- [x] 移除不匹配的 LITELLM_FALLBACK_MODELS 配置
- [x] 修复 OPENAI_BASE_URL 格式问题（# 号被解析为 URL 的一部分）
- [x] OPENAI_BASE_URL 更正为 https://api.minimaxi.com/anthropic（使用 Anthropic 协议）
- **Status:** complete

### Phase 3: 验证修复
- [x] OPENAI_BASE_URL 已更正为 https://api.minimaxi.com/anthropic
- [x] 配置解析验证通过 - LITELLM_MODEL 与渠道模型一致
- [x] 确认 API 调用正常
- **Status:** complete

## Key Questions
1. 当前 .env 中 LITELLM_MODEL 和 LLM_CHANNELS 是如何配置的？
2. MiniMax API key 是否配置了？用的是哪个 API 端点？
3. 是否应该使用 AIHubMix 而非直接使用 MiniMax API？

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| 问题根因：MiniMax-M2.7 不在 LiteLLM 内置模型成本映射中 | 日志显示 "This model isn't mapped yet" |
| API 调用实际是成功的 | 日志显示 POST 请求成功且收到响应 |
| 警告信息说明配置存在不匹配 | "LITELLM_MODEL 已配置，但当前渠道/配置文件中不存在该模型" |
| 使用 Anthropic 协议连接 MiniMax | MiniMax API 兼容 Anthropic 协议，端点为 https://api.minimaxi.com/anthropic |

## Errors Encountered
| Error | Appearances | Impact |
|-------|-------------|--------|
| "This model isn't mapped yet" | 多次 | 非致命，但每次 API 调用都会记录大量错误日志 |
| "LITELLM_MODEL ... 不在当前渠道" | 1次 | 配置警告 |
| "LITELLM_FALLBACK_MODELS 中包含未在当前渠道声明的模型" | 1次 | 配置警告 |

## Notes
- API 调用实际上成功了（返回了 response）
- 错误发生在 cost calculation 阶段，被 LiteLLM 内部捕获
- 需要检查 .env 确认具体配置

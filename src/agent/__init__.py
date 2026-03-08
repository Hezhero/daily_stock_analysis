# -*- coding: utf-8 -*-
"""
Agent module for stock analysis system.

Provides LLM-based agent with tool-calling capabilities,
pluggable trading strategies, and multi-turn conversation support.

Enabled via AGENT_MODE=true environment variable.

Use explicit imports to avoid pulling in heavy dependencies (e.g. json_repair)
when only lightweight sub-modules like tools.registry are needed::

    from src.agent.executor import AgentExecutor, AgentResult
    from src.agent.runner import run_agent_loop, RunLoopResult
    from src.agent.protocols import AgentContext, AgentOpinion, StageResult, AgentRunStats
"""


def __getattr__(name):
    """Lazy import to avoid triggering json_repair etc. on package access."""
    if name == "AgentExecutor":
        from src.agent.executor import AgentExecutor
        return AgentExecutor
    if name == "AgentResult":
        from src.agent.executor import AgentResult
        return AgentResult
    if name == "RunLoopResult":
        from src.agent.runner import RunLoopResult
        return RunLoopResult
    if name in ("AgentContext", "AgentOpinion", "StageResult", "AgentRunStats"):
        from src.agent import protocols
        return getattr(protocols, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "AgentExecutor",
    "AgentResult",
    "RunLoopResult",
    "AgentContext",
    "AgentOpinion",
    "StageResult",
    "AgentRunStats",
]

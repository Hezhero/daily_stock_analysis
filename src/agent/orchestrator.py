# -*- coding: utf-8 -*-
"""
AgentOrchestrator — multi-agent pipeline coordinator.

Manages the lifecycle of specialised agents (Technical → Intel → Risk →
Strategy → Decision) for a single stock analysis run.

Modes:
- ``quick``   : Technical only → Decision (fastest, ~2 LLM calls)
- ``standard``: Technical → Intel → Decision (default)
- ``full``    : Technical → Intel → Risk → Decision
- ``strategy``: Technical → Intel → Risk → Strategy evaluation → Decision

The orchestrator:
1. Seeds an :class:`AgentContext` with the user query and stock code
2. Runs agents sequentially, passing the shared context
3. Collects :class:`StageResult` from each agent
4. Produces a unified :class:`OrchestratorResult` with the final dashboard

Importantly, this class exposes the same ``run(task, context)`` and
``chat(message, session_id, ...)`` interface as ``AgentExecutor`` so it
can be a drop-in replacement via the factory.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from src.agent.llm_adapter import LLMToolAdapter
from src.agent.protocols import (
    AgentContext,
    AgentRunStats,
    StageResult,
    StageStatus,
)
from src.agent.runner import parse_dashboard_json
from src.agent.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from src.agent.executor import AgentResult

logger = logging.getLogger(__name__)

# Valid orchestrator modes (ordered by cost/depth)
VALID_MODES = ("quick", "standard", "full", "strategy")


@dataclass
class OrchestratorResult:
    """Unified result from a multi-agent pipeline run."""

    success: bool = False
    content: str = ""
    dashboard: Optional[Dict[str, Any]] = None
    tool_calls_log: List[Dict[str, Any]] = field(default_factory=list)
    total_steps: int = 0
    total_tokens: int = 0
    provider: str = ""
    model: str = ""
    error: Optional[str] = None
    stats: Optional[AgentRunStats] = None


class AgentOrchestrator:
    """Multi-agent pipeline coordinator.

    Drop-in replacement for ``AgentExecutor`` — exposes the same ``run()``
    and ``chat()`` interface.  The factory switches between them via
    ``AGENT_ARCH``.
    """

    def __init__(
        self,
        tool_registry: ToolRegistry,
        llm_adapter: LLMToolAdapter,
        skill_instructions: str = "",
        max_steps: int = 10,
        mode: str = "standard",
        skill_manager=None,
        config=None,
    ):
        self.tool_registry = tool_registry
        self.llm_adapter = llm_adapter
        self.skill_instructions = skill_instructions
        self.max_steps = max_steps
        self.mode = mode if mode in VALID_MODES else "standard"
        self.skill_manager = skill_manager
        self.config = config

    def _get_timeout_seconds(self) -> int:
        """Return the pipeline timeout in seconds.

        ``0`` means disabled. The timeout is a cooperative budget for the
        whole pipeline rather than a hard interruption of an in-flight stage.
        """
        raw_value = getattr(self.config, "agent_orchestrator_timeout_s", 0)
        try:
            return max(0, int(raw_value or 0))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _build_timeout_result(
        stats: AgentRunStats,
        all_tool_calls: List[Dict[str, Any]],
        models_used: List[str],
        elapsed_s: float,
        timeout_s: int,
    ) -> OrchestratorResult:
        """Build a standard timeout result payload."""
        stats.total_duration_s = round(elapsed_s, 2)
        stats.models_used = list(dict.fromkeys(models_used))
        return OrchestratorResult(
            success=False,
            error=f"Pipeline timed out after {elapsed_s:.2f}s (limit: {timeout_s}s)",
            stats=stats,
            total_steps=stats.total_stages,
            total_tokens=stats.total_tokens,
            tool_calls_log=all_tool_calls,
            provider=stats.models_used[0] if stats.models_used else "",
            model=", ".join(stats.models_used),
        )

    def _prepare_agent(self, agent: Any) -> Any:
        """Apply orchestrator-level runtime settings to a child agent."""
        if hasattr(agent, "max_steps"):
            agent.max_steps = self.max_steps
        return agent

    # -----------------------------------------------------------------
    # Public interface (mirrors AgentExecutor)
    # -----------------------------------------------------------------

    def run(self, task: str, context: Optional[Dict[str, Any]] = None) -> "AgentResult":
        """Run the multi-agent pipeline for a dashboard analysis.

        Returns an ``AgentResult`` (same type as ``AgentExecutor.run``).
        """
        from src.agent.executor import AgentResult

        ctx = self._build_context(task, context)
        orch_result = self._execute_pipeline(ctx, parse_dashboard=True)

        return AgentResult(
            success=orch_result.success,
            content=orch_result.content,
            dashboard=orch_result.dashboard,
            tool_calls_log=orch_result.tool_calls_log,
            total_steps=orch_result.total_steps,
            total_tokens=orch_result.total_tokens,
            provider=orch_result.provider,
            model=orch_result.model,
            error=orch_result.error,
        )

    def chat(
        self,
        message: str,
        session_id: str,
        progress_callback: Optional[Callable] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> "AgentResult":
        """Run the pipeline in chat mode (free-form answer, no dashboard parse).

        Conversation history is managed externally by the caller (via
        ``conversation_manager``); the orchestrator focuses on multi-agent
        coordination.
        """
        from src.agent.executor import AgentResult
        from src.agent.conversation import conversation_manager

        ctx = self._build_context(message, context)
        ctx.session_id = session_id

        # Persist user turn
        conversation_manager.add_message(session_id, "user", message)

        orch_result = self._execute_pipeline(
            ctx,
            parse_dashboard=False,
            progress_callback=progress_callback,
        )

        # Persist assistant response
        if orch_result.success:
            conversation_manager.add_message(session_id, "assistant", orch_result.content)
        else:
            conversation_manager.add_message(
                session_id, "assistant",
                f"[分析失败] {orch_result.error or '未知错误'}",
            )

        return AgentResult(
            success=orch_result.success,
            content=orch_result.content,
            dashboard=orch_result.dashboard,
            tool_calls_log=orch_result.tool_calls_log,
            total_steps=orch_result.total_steps,
            total_tokens=orch_result.total_tokens,
            provider=orch_result.provider,
            model=orch_result.model,
            error=orch_result.error,
        )

    # -----------------------------------------------------------------
    # Pipeline execution
    # -----------------------------------------------------------------

    def _execute_pipeline(
        self,
        ctx: AgentContext,
        parse_dashboard: bool = True,
        progress_callback: Optional[Callable] = None,
    ) -> OrchestratorResult:
        """Run the agent pipeline according to ``self.mode``."""
        stats = AgentRunStats()
        all_tool_calls: List[Dict[str, Any]] = []
        models_used: List[str] = []
        t0 = time.time()
        timeout_s = self._get_timeout_seconds()

        agents = self._build_agent_chain(ctx)

        for agent in agents:
            elapsed_s = time.time() - t0
            if timeout_s and elapsed_s >= timeout_s:
                logger.error("[Orchestrator] pipeline timed out before stage '%s'", agent.agent_name)
                if progress_callback:
                    progress_callback({
                        "type": "pipeline_timeout",
                        "stage": agent.agent_name,
                        "elapsed": round(elapsed_s, 2),
                        "timeout": timeout_s,
                    })
                return self._build_timeout_result(stats, all_tool_calls, models_used, elapsed_s, timeout_s)

            # Aggregate strategy opinions before the decision agent
            if agent.agent_name == "decision" and getattr(self, "_strategy_agent_names", None):
                self._aggregate_strategy_opinions(ctx)

            if progress_callback:
                progress_callback({
                    "type": "stage_start",
                    "stage": agent.agent_name,
                    "message": f"Starting {agent.agent_name} analysis...",
                })

            result: StageResult = agent.run(ctx, progress_callback=progress_callback)
            stats.record_stage(result)
            all_tool_calls.extend(
                tc for tc in (result.meta.get("tool_calls_log") or [])
            )
            models_used.extend(result.meta.get("models_used", []))

            elapsed_s = time.time() - t0
            if timeout_s and elapsed_s >= timeout_s:
                logger.error("[Orchestrator] pipeline timed out after stage '%s'", agent.agent_name)
                if progress_callback:
                    progress_callback({
                        "type": "pipeline_timeout",
                        "stage": agent.agent_name,
                        "elapsed": round(elapsed_s, 2),
                        "timeout": timeout_s,
                    })
                return self._build_timeout_result(stats, all_tool_calls, models_used, elapsed_s, timeout_s)

            if progress_callback:
                progress_callback({
                    "type": "stage_done",
                    "stage": agent.agent_name,
                    "status": result.status.value,
                    "duration": result.duration_s,
                })

            # Abort pipeline on critical failure (except intel — degrade gracefully)
            if result.status == StageStatus.FAILED and agent.agent_name not in ("intel", "risk"):
                logger.error("[Orchestrator] critical stage '%s' failed: %s", agent.agent_name, result.error)
                return OrchestratorResult(
                    success=False,
                    error=f"Stage '{agent.agent_name}' failed: {result.error}",
                    stats=stats,
                    total_tokens=stats.total_tokens,
                    tool_calls_log=all_tool_calls,
                )

        # Assemble final output
        total_duration = round(time.time() - t0, 2)
        stats.total_duration_s = total_duration
        stats.models_used = list(dict.fromkeys(models_used))

        # Final content: prefer dashboard from decision agent, else last opinion text
        content = ""
        dashboard = None

        final_dashboard = ctx.get_data("final_dashboard")
        final_raw = ctx.get_data("final_dashboard_raw")

        if final_dashboard:
            dashboard = final_dashboard
            content = json.dumps(final_dashboard, ensure_ascii=False, indent=2)
        elif final_raw:
            content = final_raw
            if parse_dashboard:
                dashboard = parse_dashboard_json(final_raw)
        elif ctx.opinions:
            # Fallback: synthesise a summary from available opinions
            content = self._fallback_summary(ctx)

        model_str = ", ".join(dict.fromkeys(m for m in models_used if m))

        return OrchestratorResult(
            success=bool(content),
            content=content,
            dashboard=dashboard,
            tool_calls_log=all_tool_calls,
            total_steps=stats.total_stages,
            total_tokens=stats.total_tokens,
            provider=stats.models_used[0] if stats.models_used else "",
            model=model_str,
            stats=stats,
        )

    # -----------------------------------------------------------------
    # Agent chain construction
    # -----------------------------------------------------------------

    def _build_agent_chain(self, ctx: AgentContext) -> list:
        """Instantiate the ordered agent list based on ``self.mode``."""
        from src.agent.agents.technical_agent import TechnicalAgent
        from src.agent.agents.intel_agent import IntelAgent
        from src.agent.agents.decision_agent import DecisionAgent
        from src.agent.agents.risk_agent import RiskAgent

        self._strategy_agent_names = set()

        common_kwargs = dict(
            tool_registry=self.tool_registry,
            llm_adapter=self.llm_adapter,
            skill_instructions=self.skill_instructions,
        )

        technical = self._prepare_agent(TechnicalAgent(**common_kwargs))
        intel = self._prepare_agent(IntelAgent(**common_kwargs))
        risk = self._prepare_agent(RiskAgent(**common_kwargs))
        decision = self._prepare_agent(DecisionAgent(**common_kwargs))

        if self.mode == "quick":
            return [technical, decision]
        elif self.mode == "standard":
            return [technical, intel, decision]
        elif self.mode == "full":
            return [technical, intel, risk, decision]
        elif self.mode == "strategy":
            chain = [technical, intel, risk]
            # Insert strategy evaluation agents if applicable
            strategy_agents = self._build_strategy_agents(ctx, common_kwargs)
            chain.extend(strategy_agents)
            # Mark where to aggregate strategy opinions before decision
            self._strategy_agent_names = {a.agent_name for a in strategy_agents}
            chain.append(decision)
            return chain
        else:
            return [technical, intel, decision]

    def _build_strategy_agents(self, ctx: AgentContext, common_kwargs: dict) -> list:
        """Build strategy-specific sub-agents based on requested strategies.

        Uses the strategy router to select applicable strategies, then
        creates lightweight agent wrappers for each.
        """
        try:
            from src.agent.strategies.router import StrategyRouter
            router = StrategyRouter()
            selected = router.select_strategies(ctx)
            if not selected:
                return []

            from src.agent.strategies.strategy_agent import StrategyAgent
            agents = []
            for strategy_id in selected[:3]:  # cap at 3 concurrent strategies
                agent = self._prepare_agent(StrategyAgent(
                    strategy_id=strategy_id,
                    **common_kwargs,
                ))
                agents.append(agent)
            return agents
        except Exception as exc:
            logger.warning("[Orchestrator] failed to build strategy agents: %s", exc)
            return []

    # -----------------------------------------------------------------
    # Strategy aggregation
    # -----------------------------------------------------------------

    def _aggregate_strategy_opinions(self, ctx: AgentContext) -> None:
        """Run StrategyAggregator to produce a consensus opinion.

        Merges individual ``strategy_*`` opinions into a single weighted
        consensus and stores it in context so the decision agent can use it.
        """
        try:
            from src.agent.strategies.aggregator import StrategyAggregator
            aggregator = StrategyAggregator()
            consensus = aggregator.aggregate(ctx)
            if consensus:
                ctx.opinions.append(consensus)
                ctx.set_data("strategy_consensus", {
                    "signal": consensus.signal,
                    "confidence": consensus.confidence,
                    "reasoning": consensus.reasoning,
                })
                logger.info(
                    "[Orchestrator] strategy consensus: signal=%s confidence=%.2f",
                    consensus.signal, consensus.confidence,
                )
            else:
                logger.info("[Orchestrator] no strategy opinions to aggregate")
        except Exception as exc:
            logger.warning("[Orchestrator] strategy aggregation failed: %s", exc)

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    def _build_context(self, task: str, context: Optional[Dict[str, Any]] = None) -> AgentContext:
        """Seed an ``AgentContext`` from the user request."""
        ctx = AgentContext(query=task)

        if context:
            ctx.stock_code = context.get("stock_code", "")
            ctx.stock_name = context.get("stock_name", "")
            ctx.meta["strategies_requested"] = context.get("strategies", [])

            # Pre-populate data fields that the caller already has
            for data_key in ("realtime_quote", "daily_history", "chip_distribution",
                             "trend_result", "news_context"):
                if context.get(data_key):
                    ctx.set_data(data_key, context[data_key])

        # Try to extract stock code from the query text
        if not ctx.stock_code:
            ctx.stock_code = _extract_stock_code(task)

        return ctx

    @staticmethod
    def _fallback_summary(ctx: AgentContext) -> str:
        """Build a plaintext summary when dashboard JSON is unavailable."""
        lines = [f"# Analysis Summary: {ctx.stock_code} ({ctx.stock_name})", ""]
        for op in ctx.opinions:
            lines.append(f"## {op.agent_name}")
            lines.append(f"Signal: {op.signal} (confidence: {op.confidence:.0%})")
            lines.append(op.reasoning)
            lines.append("")
        if ctx.risk_flags:
            lines.append("## Risk Flags")
            for rf in ctx.risk_flags:
                lines.append(f"- [{rf['severity']}] {rf['description']}")
        return "\n".join(lines)


# Common English words (2-5 uppercase letters) that should NOT be treated as
# US stock tickers.  This set is checked by _extract_stock_code() and should
# be kept at module level to avoid re-creating it on every call.
_COMMON_WORDS: set[str] = {
    # Pronouns / articles / prepositions / conjunctions
    "THE", "AND", "FOR", "ARE", "BUT", "NOT", "YOU", "ALL",
    "CAN", "HAD", "HER", "WAS", "ONE", "OUR", "OUT", "HAS",
    "HIS", "HOW", "ITS", "LET", "MAY", "NEW", "NOW", "OLD",
    "SEE", "WAY", "WHO", "DID", "GET", "HIM", "USE", "SAY",
    "SHE", "TOO", "ANY", "WITH", "FROM", "THAT", "THAN",
    "THIS", "WHAT", "WHEN", "WILL", "JUST", "ALSO",
    "BEEN", "EACH", "HAVE", "MUCH", "ONLY", "OVER",
    "SOME", "SUCH", "THEM", "THEN", "THEY", "VERY",
    "WERE", "YOUR", "ABOUT", "AFTER", "COULD", "EVERY",
    "OTHER", "THEIR", "THERE", "THESE", "THOSE", "WHICH",
    "WOULD", "BEING", "STILL", "WHERE",
    # Finance/analysis jargon that looks like tickers
    "BUY", "SELL", "HOLD", "LONG", "PUT", "CALL",
    "ETF", "IPO", "RSI", "EPS", "PEG", "ROE", "ROA",
    "USA", "USD", "CNY", "HKD", "EUR", "GBP",
    "STOCK", "TRADE", "PRICE", "INDEX", "FUND",
    "HIGH", "LOW", "OPEN", "CLOSE", "STOP", "LOSS",
    "TREND", "BULL", "BEAR", "RISK", "CASH", "BOND",
    "MACD", "VWAP", "BOLL",
}


def _extract_stock_code(text: str) -> str:
    """Best-effort stock code extraction from free text."""
    import re
    # A-share 6-digit — use lookarounds instead of \b because Python's \b
    # does not fire at Chinese-character / digit boundaries.
    m = re.search(r'(?<!\d)([036]\d{5})(?!\d)', text)
    if m:
        return m.group(1)
    # HK — same lookaround approach
    m = re.search(r'(?<![a-zA-Z])(hk\d{5})(?!\d)', text, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    # US ticker — require 2+ uppercase letters bounded by non-alpha chars.
    m = re.search(r'(?<![a-zA-Z])([A-Z]{2,5})(?![a-zA-Z])', text)
    if m:
        candidate = m.group(1)
        if candidate not in _COMMON_WORDS:
            return candidate
    return ""

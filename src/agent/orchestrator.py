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
import re
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from src.agent.llm_adapter import LLMToolAdapter
from src.agent.protocols import (
    AgentContext,
    AgentRunStats,
    StageResult,
    StageStatus,
    normalize_decision_signal,
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
        ctx.meta["response_mode"] = "dashboard"
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
        ctx.meta["response_mode"] = "chat"

        session = conversation_manager.get_or_create(session_id)
        history = session.get_history()
        if history:
            ctx.meta["conversation_history"] = history

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
        strategy_agents_inserted = False
        index = 0

        while index < len(agents):
            agent = agents[index]
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

            if (
                self.mode == "strategy"
                and agent.agent_name == "decision"
                and not strategy_agents_inserted
            ):
                strategy_agents = self._build_strategy_agents(ctx)
                self._strategy_agent_names = {a.agent_name for a in strategy_agents}
                strategy_agents_inserted = True
                if strategy_agents:
                    agents[index:index] = strategy_agents
                    continue

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

            if ctx.meta.get("response_mode") == "chat" and agent.agent_name == "decision":
                final_text = result.meta.get("raw_text")
                if isinstance(final_text, str) and final_text.strip():
                    ctx.set_data("final_response_text", final_text.strip())

            if result.success and agent.agent_name == "decision":
                self._apply_risk_override(ctx)

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

            index += 1

        # Assemble final output
        total_duration = round(time.time() - t0, 2)
        stats.total_duration_s = total_duration
        stats.models_used = list(dict.fromkeys(models_used))

        # Final content: prefer dashboard from decision agent, else last opinion text
        content = ""
        dashboard = None

        final_dashboard = ctx.get_data("final_dashboard")
        final_raw = ctx.get_data("final_dashboard_raw")
        final_text = ctx.get_data("final_response_text")
        chat_mode = ctx.meta.get("response_mode") == "chat"

        if parse_dashboard:
            if final_dashboard:
                dashboard = final_dashboard
                content = json.dumps(final_dashboard, ensure_ascii=False, indent=2)
            elif final_raw:
                content = final_raw
                dashboard = parse_dashboard_json(final_raw)
            elif ctx.opinions:
                # Fallback: synthesise a summary from available opinions
                content = self._fallback_summary(ctx)
        else:
            if chat_mode and isinstance(final_text, str) and final_text.strip():
                content = final_text.strip()
            elif final_raw:
                content = final_raw
            elif final_dashboard:
                content = json.dumps(final_dashboard, ensure_ascii=False, indent=2)
            elif ctx.opinions:
                content = self._fallback_summary(ctx)

        model_str = ", ".join(dict.fromkeys(m for m in models_used if m))
        provider = stats.models_used[0] if stats.models_used else ""

        if parse_dashboard and dashboard is None:
            return OrchestratorResult(
                success=False,
                content=content,
                dashboard=None,
                tool_calls_log=all_tool_calls,
                total_steps=stats.total_stages,
                total_tokens=stats.total_tokens,
                provider=provider,
                model=model_str,
                error="Failed to parse dashboard JSON from agent response",
                stats=stats,
            )

        return OrchestratorResult(
            success=bool(content),
            content=content,
            dashboard=dashboard,
            tool_calls_log=all_tool_calls,
            total_steps=stats.total_stages,
            total_tokens=stats.total_tokens,
            provider=provider,
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
            # Strategy agents are inserted lazily right before the decision
            # stage so the router can see the finished technical opinion.
            return [technical, intel, risk, decision]
        else:
            return [technical, intel, decision]

    def _build_strategy_agents(self, ctx: AgentContext) -> list:
        """Build strategy-specific sub-agents based on requested strategies.

        Uses the strategy router to select applicable strategies, then
        creates lightweight agent wrappers for each.
        """
        try:
            from src.agent.strategies.router import StrategyRouter
            common_kwargs = dict(
                tool_registry=self.tool_registry,
                llm_adapter=self.llm_adapter,
                skill_instructions=self.skill_instructions,
            )
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

    def _apply_risk_override(self, ctx: AgentContext) -> None:
        """Apply risk-agent veto/downgrade rules to the final dashboard."""
        if not getattr(self.config, "agent_risk_override", True):
            return

        dashboard = ctx.get_data("final_dashboard")
        if not isinstance(dashboard, dict):
            return

        risk_opinion = next((op for op in reversed(ctx.opinions) if op.agent_name == "risk"), None)
        risk_raw = risk_opinion.raw_data if risk_opinion and isinstance(risk_opinion.raw_data, dict) else {}

        adjustment = str(risk_raw.get("signal_adjustment") or "").lower()
        has_high_flag = any(str(flag.get("severity", "")).lower() == "high" for flag in ctx.risk_flags)
        veto_buy = bool(risk_raw.get("veto_buy")) or adjustment == "veto" or has_high_flag

        current_signal = normalize_decision_signal(dashboard.get("decision_type", "hold"))
        new_signal = current_signal
        if veto_buy and current_signal == "buy":
            new_signal = "hold"
        elif adjustment == "downgrade_one":
            new_signal = _downgrade_signal(current_signal, steps=1)
        elif adjustment == "downgrade_two":
            new_signal = _downgrade_signal(current_signal, steps=2)

        if new_signal == current_signal:
            return

        dashboard["decision_type"] = new_signal
        dashboard["risk_warning"] = self._merge_risk_warning(
            dashboard.get("risk_warning"),
            risk_raw,
            ctx.risk_flags,
            new_signal,
        )

        sentiment_score = dashboard.get("sentiment_score")
        try:
            score = int(sentiment_score)
        except (TypeError, ValueError):
            score = 50
        dashboard["sentiment_score"] = _adjust_sentiment_score(score, new_signal)

        operation_advice = dashboard.get("operation_advice")
        if isinstance(operation_advice, str):
            dashboard["operation_advice"] = _adjust_operation_advice(operation_advice, new_signal)

        summary = dashboard.get("analysis_summary")
        if isinstance(summary, str) and summary:
            dashboard["analysis_summary"] = f"[风控下调: {current_signal} -> {new_signal}] {summary}"

        dashboard_block = dashboard.get("dashboard")
        if isinstance(dashboard_block, dict):
            core = dashboard_block.get("core_conclusion")
            if isinstance(core, dict):
                signal_type = {
                    "buy": "🟡持有观望",
                    "hold": "🟡持有观望",
                    "sell": "🔴卖出信号",
                }.get(new_signal, "⚠️风险警告")
                core["signal_type"] = signal_type
                sentence = core.get("one_sentence")
                if isinstance(sentence, str) and sentence:
                    core["one_sentence"] = f"{sentence}（风控下调）"
                position = core.get("position_advice")
                if isinstance(position, dict):
                    if new_signal == "hold":
                        position["no_position"] = "风险未解除前先观望，等待更清晰的入场条件。"
                        position["has_position"] = "谨慎持有并收紧止损，待风险缓解后再考虑加仓。"
                    elif new_signal == "sell":
                        position["no_position"] = "风险明显偏高，暂不新开仓。"
                        position["has_position"] = "优先控制回撤，建议减仓或退出高风险仓位。"

        ctx.set_data("final_dashboard", dashboard)
        ctx.set_data("risk_override_applied", {
            "from": current_signal,
            "to": new_signal,
            "adjustment": adjustment or ("veto" if veto_buy else "none"),
        })

        for opinion in reversed(ctx.opinions):
            if opinion.agent_name == "decision":
                opinion.signal = new_signal
                if isinstance(dashboard.get("analysis_summary"), str):
                    opinion.reasoning = dashboard["analysis_summary"]
                opinion.raw_data = dashboard
                break

        logger.info(
            "[Orchestrator] risk override applied: %s -> %s (adjustment=%s, high_flag=%s)",
            current_signal,
            new_signal,
            adjustment or ("veto" if veto_buy else "none"),
            has_high_flag,
        )

    @staticmethod
    def _merge_risk_warning(
        existing_warning: Any,
        risk_raw: Dict[str, Any],
        risk_flags: List[Dict[str, Any]],
        signal: str,
    ) -> str:
        """Build a concise risk warning after a forced downgrade."""
        warnings: List[str] = []
        if isinstance(existing_warning, str) and existing_warning.strip():
            warnings.append(existing_warning.strip())
        if isinstance(risk_raw.get("reasoning"), str) and risk_raw["reasoning"].strip():
            warnings.append(risk_raw["reasoning"].strip())
        for flag in risk_flags[:3]:
            description = str(flag.get("description", "")).strip()
            severity = str(flag.get("severity", "")).lower()
            if description:
                warnings.append(f"[{severity or 'risk'}] {description}")
        prefix = f"风控接管：最终信号已下调为 {signal}。"
        merged = " ".join(dict.fromkeys([prefix] + warnings))
        return merged[:500]


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
    # Greetings / filler words that often appear in chat messages
    "HELLO", "PLEASE", "THANKS", "CHECK", "LOOK", "THINK",
    "MAYBE", "GUESS", "TELL", "SHOW", "WHAT", "WHATS",
    "WHY", "WHEN", "HOWDY", "HEY", "HI",
}

_LOWERCASE_TICKER_HINTS = re.compile(
    r"分析|看看|查一?下|研究|诊断|走势|趋势|股价|股票|个股",
)


def _extract_stock_code(text: str) -> str:
    """Best-effort stock code extraction from free text."""
    # A-share 6-digit — use lookarounds instead of \b because Python's \b
    # does not fire at Chinese-character / digit boundaries.
    m = re.search(r'(?<!\d)((?:[03648]\d{5}|92\d{4}))(?!\d)', text)
    if m:
        return m.group(1)
    # HK — same lookaround approach
    m = re.search(r'(?<![a-zA-Z])(hk\d{5})(?!\d)', text, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    # US ticker — require 2+ uppercase letters bounded by non-alpha chars.
    m = re.search(r'(?<![a-zA-Z])([A-Z]{2,5}(?:\.[A-Z]{1,2})?)(?![a-zA-Z])', text)
    if m:
        candidate = m.group(1)
        if candidate not in _COMMON_WORDS:
            return candidate

    stripped = (text or "").strip()
    bare_match = re.fullmatch(r'([A-Za-z]{2,5}(?:\.[A-Za-z]{1,2})?)', stripped)
    if bare_match:
        candidate = bare_match.group(1).upper()
        if candidate not in _COMMON_WORDS:
            return candidate

    if not _LOWERCASE_TICKER_HINTS.search(stripped):
        return ""

    for match in re.finditer(r'(?<![a-zA-Z])([A-Za-z]{2,5}(?:\.[A-Za-z]{1,2})?)(?![a-zA-Z])', text):
        raw_candidate = match.group(1)
        candidate = raw_candidate.upper()
        if candidate in _COMMON_WORDS:
            continue
        return candidate
    return ""


def _downgrade_signal(signal: str, steps: int = 1) -> str:
    """Downgrade a dashboard decision signal by one or more levels."""
    order = ["buy", "hold", "sell"]
    try:
        index = order.index(signal)
    except ValueError:
        return signal
    return order[min(len(order) - 1, index + max(0, steps))]


def _adjust_sentiment_score(score: int, signal: str) -> int:
    """Clamp sentiment score into the target band for the overridden signal."""
    bands = {
        "buy": (60, 79),
        "hold": (40, 59),
        "sell": (0, 39),
    }
    low, high = bands.get(signal, (0, 100))
    return max(low, min(high, score))


def _adjust_operation_advice(advice: str, signal: str) -> str:
    """Normalize action wording to the overridden decision signal."""
    mapping = {
        "buy": "买入",
        "hold": "观望",
        "sell": "减仓/卖出",
    }
    if signal not in mapping:
        return advice
    if advice == mapping[signal]:
        return advice
    return f"{mapping[signal]}（原建议已被风控下调）"

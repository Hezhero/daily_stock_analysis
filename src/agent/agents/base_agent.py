# -*- coding: utf-8 -*-
"""
BaseAgent — abstract base for all specialised agents.

Every agent in the multi-agent pipeline inherits from this class and
implements :meth:`run`.  The base class provides shared utilities:
tool-subset selection, prompt assembly, LLM invocation via the shared
runner, and structured opinion output.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from src.agent.llm_adapter import LLMToolAdapter
from src.agent.protocols import AgentContext, AgentOpinion, StageResult, StageStatus
from src.agent.runner import RunLoopResult, run_agent_loop
from src.agent.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """Abstract base for all specialised agents.

    Subclasses **must** implement:
    - :pyattr:`agent_name` — unique agent identifier
    - :meth:`system_prompt` — return the LLM system prompt
    - :meth:`build_user_message` — construct the user message for the LLM

    Subclasses **may** override:
    - :pyattr:`tool_names` — restrict which tools the agent can access
    - :pyattr:`max_steps` — per-agent step limit  (default 6)
    - :meth:`post_process` — transform the raw LLM text into an :class:`AgentOpinion`
    """

    # Subclass overrides
    agent_name: str = "base"
    tool_names: Optional[List[str]] = None  # None → all tools available
    max_steps: int = 6

    def __init__(
        self,
        tool_registry: ToolRegistry,
        llm_adapter: LLMToolAdapter,
        skill_instructions: str = "",
    ):
        self.tool_registry = tool_registry
        self.llm_adapter = llm_adapter
        self.skill_instructions = skill_instructions

    # -----------------------------------------------------------------
    # Abstract interface
    # -----------------------------------------------------------------

    @abstractmethod
    def system_prompt(self, ctx: AgentContext) -> str:
        """Build the system prompt for this agent."""

    @abstractmethod
    def build_user_message(self, ctx: AgentContext) -> str:
        """Build the user message sent to the LLM."""

    # -----------------------------------------------------------------
    # Default hook for structured output
    # -----------------------------------------------------------------

    def post_process(self, ctx: AgentContext, raw_text: str) -> Optional[AgentOpinion]:
        """Extract a structured :class:`AgentOpinion` from the raw LLM text.

        Default: returns ``None`` (the raw text is still stored in
        ``StageResult.meta["raw_text"]``).  Subclasses that produce
        analysis opinions should override this.
        """
        return None

    # -----------------------------------------------------------------
    # Execution
    # -----------------------------------------------------------------

    def run(
        self,
        ctx: AgentContext,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> StageResult:
        """Execute this agent and return a :class:`StageResult`.

        Steps:
        1. Build system + user messages.
        2. Optionally inject pre-fetched data from ``ctx.data``.
        3. Delegate to :func:`run_agent_loop`.
        4. Call :meth:`post_process` to produce an opinion.
        5. Append the opinion to ``ctx.opinions``.
        """
        t0 = time.time()
        result = StageResult(stage_name=self.agent_name, status=StageStatus.RUNNING)

        try:
            messages = self._build_messages(ctx)

            # Restrict tools if the agent declares a subset
            registry = self._filtered_registry()

            loop_result: RunLoopResult = run_agent_loop(
                messages=messages,
                tool_registry=registry,
                llm_adapter=self.llm_adapter,
                max_steps=self.max_steps,
                progress_callback=progress_callback,
            )

            result.tokens_used = loop_result.total_tokens
            result.tool_calls_count = len(loop_result.tool_calls_log)
            result.meta["raw_text"] = loop_result.content
            result.meta["models_used"] = loop_result.models_used

            if not loop_result.success:
                result.status = StageStatus.FAILED
                result.error = loop_result.error or "Agent loop did not produce a final answer"
                return result

            # Post-process into structured opinion
            opinion = self.post_process(ctx, loop_result.content)
            if opinion is not None:
                opinion.agent_name = self.agent_name
                ctx.add_opinion(opinion)
                result.opinion = opinion

            result.status = StageStatus.COMPLETED

        except Exception as exc:
            logger.error("[%s] execution failed: %s", self.agent_name, exc, exc_info=True)
            result.status = StageStatus.FAILED
            result.error = str(exc)
        finally:
            result.duration_s = round(time.time() - t0, 2)

        return result

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _build_messages(self, ctx: AgentContext) -> List[Dict[str, Any]]:
        """Assemble the initial messages list for the LLM."""
        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": self.system_prompt(ctx)},
        ]

        # Inject pre-fetched data as a synthetic assistant context
        cached_data = self._inject_cached_data(ctx)
        if cached_data:
            messages.append({"role": "user", "content": cached_data})
            messages.append({"role": "assistant", "content": "Understood, I have the pre-fetched data. Proceeding with analysis."})

        messages.append({"role": "user", "content": self.build_user_message(ctx)})
        return messages

    def _inject_cached_data(self, ctx: AgentContext) -> str:
        """Build a context string from already-fetched data in ``ctx.data``.

        This avoids redundant tool calls when earlier stages have already
        fetched the data this agent needs.
        """
        import json
        parts: List[str] = []
        for key, value in ctx.data.items():
            if value is not None:
                try:
                    serialised = json.dumps(value, ensure_ascii=False, default=str)
                except (TypeError, ValueError):
                    serialised = str(value)
                # Cap per-field size to avoid overwhelming the context window
                if len(serialised) > 8000:
                    serialised = serialised[:8000] + "...(truncated)"
                parts.append(f"[Pre-fetched: {key}]\n{serialised}")
        return "\n\n".join(parts) if parts else ""

    def _filtered_registry(self) -> ToolRegistry:
        """Return a ToolRegistry restricted to ``self.tool_names``.

        If ``tool_names`` is None (default), the full registry is returned.
        """
        if self.tool_names is None:
            return self.tool_registry

        from src.agent.tools.registry import ToolRegistry as TR
        filtered = TR()
        for name in self.tool_names:
            tool_def = self.tool_registry.get(name)
            if tool_def:
                filtered.register(tool_def)
            else:
                logger.warning("[%s] requested tool '%s' not found in registry", self.agent_name, name)
        return filtered

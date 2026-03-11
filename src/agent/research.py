# -*- coding: utf-8 -*-
"""
ResearchAgent — deep research specialist for in-depth analysis.

Responsible for:
- Decomposing a complex research query into sub-questions
- Iterative search and information gathering
- Cross-verification of findings
- Producing a structured research report

Triggered by ``/research`` command or API async task interface.
Designed for long-running analysis (up to ``AGENT_DEEP_RESEARCH_BUDGET``
tokens).
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from src.agent.llm_adapter import LLMToolAdapter
from src.agent.runner import RunLoopResult, run_agent_loop
from src.agent.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)

# Default token budget for deep research
_DEFAULT_TOKEN_BUDGET = 30000


class ResearchAgent:
    """Multi-turn deep research agent.

    Unlike the standard agent loop which runs a fixed number of steps,
    the ResearchAgent:
    1. Decomposes the query into sub-questions (planning phase)
    2. Researches each sub-question with dedicated searches
    3. Synthesises findings into a comprehensive report
    4. Tracks total token usage against a configurable budget
    """

    agent_name = "research"
    tool_names = [
        "search_stock_news",
        "search_comprehensive_intel",
        "get_stock_info",
        "get_realtime_quote",
        "get_daily_history",
        "get_sector_rankings",
        "get_market_indices",
    ]

    def __init__(
        self,
        tool_registry: ToolRegistry,
        llm_adapter: LLMToolAdapter,
        token_budget: int = _DEFAULT_TOKEN_BUDGET,
        max_sub_questions: int = 5,
    ):
        self.tool_registry = tool_registry
        self.llm_adapter = llm_adapter
        self.token_budget = token_budget
        self.max_sub_questions = max_sub_questions

    def research(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> ResearchResult:
        """Execute a deep research task.

        Args:
            query: The research question or topic.
            context: Optional context (stock_code, stock_name, etc.).
            progress_callback: Optional progress updates.

        Returns:
            A :class:`ResearchResult` containing the report and metadata.
        """
        t0 = time.time()
        tokens_used = 0
        all_findings: List[Dict[str, Any]] = []

        # Phase 1: Decompose
        if progress_callback:
            progress_callback({"type": "research_phase", "phase": "decompose", "message": "Decomposing research query..."})

        sub_questions = self._decompose_query(query, context)
        tokens_used += sub_questions.get("tokens", 0)

        questions = sub_questions.get("questions", [query])[:self.max_sub_questions]
        logger.info("[ResearchAgent] decomposed into %d sub-questions", len(questions))

        # Phase 2: Research each sub-question
        for i, question in enumerate(questions):
            if tokens_used >= self.token_budget:
                logger.warning("[ResearchAgent] token budget exceeded (%d/%d), stopping", tokens_used, self.token_budget)
                break

            if progress_callback:
                progress_callback({
                    "type": "research_phase",
                    "phase": "search",
                    "message": f"Researching ({i + 1}/{len(questions)}): {question[:60]}...",
                    "progress": (i + 1) / len(questions),
                })

            finding = self._research_sub_question(question, context, tokens_used)
            tokens_used += finding.get("tokens", 0)
            all_findings.append(finding)

        # Phase 3: Synthesise
        if progress_callback:
            progress_callback({"type": "research_phase", "phase": "synthesize", "message": "Synthesising research report..."})

        report = self._synthesise_report(query, all_findings, context) if all_findings else {"content": "No findings gathered.", "tokens": 0}
        tokens_used += report.get("tokens", 0)

        duration = round(time.time() - t0, 2)

        return ResearchResult(
            success=bool(report.get("content")),
            report=report.get("content", ""),
            sub_questions=questions,
            findings_count=len(all_findings),
            total_tokens=tokens_used,
            duration_s=duration,
            error=report.get("error"),
        )

    def _decompose_query(self, query: str, context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Use LLM to decompose a research query into sub-questions."""
        stock_hint = ""
        if context and context.get("stock_code"):
            stock_hint = f"\nStock context: {context['stock_code']} ({context.get('stock_name', '')})"

        system = """\
You are a research planning assistant. Given a research query, decompose it \
into 3-5 specific, searchable sub-questions.

Return a JSON object:
{"questions": ["question 1", "question 2", ...]}
"""
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": f"Research query: {query}{stock_hint}"},
        ]

        try:
            import litellm
            from src.config import get_config
            config = get_config()
            model = config.litellm_model
            if not model:
                return {"questions": [query], "tokens": 0}

            resp = litellm.completion(
                model=model, messages=messages,
                temperature=0.3, max_tokens=400, timeout=15,
            )
            raw = resp.choices[0].message.content.strip()
            tokens = resp.usage.total_tokens if resp.usage else 0

            # Parse JSON
            if raw.startswith("```"):
                raw = re.sub(r'^```(?:json)?\s*', '', raw)
                raw = re.sub(r'\s*```$', '', raw)
            parsed = json.loads(raw)
            return {"questions": parsed.get("questions", [query]), "tokens": tokens}
        except Exception as exc:
            logger.warning("[ResearchAgent] decompose failed: %s", exc)
            return {"questions": [query], "tokens": 0}

    def _research_sub_question(
        self,
        question: str,
        context: Optional[Dict[str, Any]],
        current_tokens: int,
    ) -> Dict[str, Any]:
        """Research a single sub-question using the agent loop."""
        remaining_budget = self.token_budget - current_tokens

        system = f"""\
You are a research agent investigating a specific question.
Use your tools to search for relevant information, then summarise \
your findings in 2-4 paragraphs.  Be factual and cite sources.
Token budget remaining: ~{remaining_budget}
"""
        stock_context = ""
        if context and context.get("stock_code"):
            stock_context = f" (related to stock {context['stock_code']})"

        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": f"Research question: {question}{stock_context}"},
        ]

        try:
            registry = self._filtered_registry()
            result: RunLoopResult = run_agent_loop(
                messages=messages,
                tool_registry=registry,
                llm_adapter=self.llm_adapter,
                max_steps=4,
            )
            return {
                "question": question,
                "content": result.content,
                "tokens": result.total_tokens,
                "success": result.success,
            }
        except Exception as exc:
            logger.warning("[ResearchAgent] sub-question failed: %s", exc)
            return {"question": question, "content": "", "tokens": 0, "success": False, "error": str(exc)}

    def _synthesise_report(
        self,
        original_query: str,
        findings: List[Dict[str, Any]],
        context: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Synthesise all findings into a coherent research report."""
        findings_text = "\n\n".join(
            f"### Sub-question: {f['question']}\n{f.get('content', 'No data')}"
            for f in findings if f.get("content")
        )

        system = """\
You are a senior research analyst. Synthesise the following research \
findings into a comprehensive, well-structured report.

## Report Structure
1. **Executive Summary** (2-3 sentences)
2. **Key Findings** (bullet points)
3. **Detailed Analysis** (sections per topic)
4. **Risk Factors** (if applicable)
5. **Conclusion & Recommendations**

Use Markdown formatting.  Be concise but thorough.
"""
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": f"Original query: {original_query}\n\n## Research Findings\n\n{findings_text}"},
        ]

        try:
            import litellm
            from src.config import get_config
            config = get_config()
            model = config.litellm_model
            if not model:
                return {"content": findings_text, "tokens": 0}

            resp = litellm.completion(
                model=model, messages=messages,
                temperature=0.3, max_tokens=2000, timeout=30,
            )
            content = resp.choices[0].message.content.strip()
            tokens = resp.usage.total_tokens if resp.usage else 0
            return {"content": content, "tokens": tokens}
        except Exception as exc:
            logger.warning("[ResearchAgent] synthesis failed: %s", exc)
            return {"content": findings_text, "tokens": 0, "error": str(exc)}

    def _filtered_registry(self) -> ToolRegistry:
        """Return a registry restricted to research-related tools.

        Reuses the same filtering logic as :meth:`BaseAgent._filtered_registry`.
        """
        from src.agent.agents.base_agent import BaseAgent
        # Borrow the shared implementation; it respects self.tool_names / self.tool_registry.
        return BaseAgent._filtered_registry(self)


@dataclass
class ResearchResult:
    """Output from a deep research task."""

    success: bool = False
    report: str = ""
    sub_questions: List[str] = field(default_factory=list)
    findings_count: int = 0
    total_tokens: int = 0
    duration_s: float = 0.0
    error: Optional[str] = None

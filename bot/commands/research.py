# -*- coding: utf-8 -*-
"""
Research command — deep research on a stock or market topic.

Usage:
    /research 600519                        -> Deep research on Kweichow Moutai
    /research 600519 近期业绩风险            -> Focused research with specific question
    /research 新能源板块前景分析              -> Topic-based research
"""

import logging
import time
import uuid
from typing import List, Optional

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse
from src.config import get_config

logger = logging.getLogger(__name__)


class ResearchCommand(BotCommand):
    """
    Research command handler — invoke the deep research agent.

    Usage:
        /research 600519                    -> Deep research on a stock
        /research 600519 业绩风险分析        -> Focused question
        /research 新能源板块 发展前景         -> Sector research
    """

    @property
    def name(self) -> str:
        return "research"

    @property
    def aliases(self) -> List[str]:
        return ["深研", "deepsearch"]

    @property
    def description(self) -> str:
        return "Deep research on a stock or market topic"

    @property
    def usage(self) -> str:
        return "/research <stock_code|topic> [specific question]"

    def execute(self, message: BotMessage, args: List[str]) -> BotResponse:
        if not args:
            return BotResponse.text_response(
                f"Usage: {self.usage}\n"
                "Example: /research 600519 近期有哪些风险\n"
                "Example: /research 新能源板块前景分析"
            )

        config = get_config()

        # Check agent availability (consistent with /chat and API)
        if not config.is_agent_available():
            return BotResponse.text_response(
                "⚠️ Agent mode is not available. Configure LITELLM_MODEL or set AGENT_MODE=true to use /research."
            )

        # Parse arguments — first arg may be stock code, rest is the question
        query_parts = list(args)
        stock_code: Optional[str] = None

        # Try to detect a stock code in the first argument
        first = query_parts[0].upper().replace("，", ",")
        import re
        if re.match(r"^\d{6}$", first) or re.match(r"^HK\d{5}$", first) or re.match(r"^[A-Z]{1,5}(\.[A-Z]{1,2})?$", first):
            stock_code = first
            query_parts = query_parts[1:]

        # Build the research query
        if query_parts:
            question = " ".join(query_parts)
        elif stock_code:
            question = f"Comprehensive deep research on stock {stock_code}: fundamentals, technicals, news sentiment, and risk factors"
        else:
            question = " ".join(args)

        if stock_code:
            question = f"[Stock: {stock_code}] {question}"

        # Run the research agent
        try:
            from src.agent.research import ResearchAgent
            from src.agent.factory import get_tool_registry
            from src.agent.llm_adapter import LLMToolAdapter

            registry = get_tool_registry()
            llm_adapter = LLMToolAdapter(config)
            budget = getattr(config, "agent_deep_research_budget", 30000)

            agent = ResearchAgent(
                tool_registry=registry,
                llm_adapter=llm_adapter,
                token_budget=budget,
            )

            # Send a "working" notice for long-running research
            logger.info("[ResearchCommand] Starting deep research: %s", question[:100])
            t0 = time.time()

            result = agent.research(question)
            duration = round(time.time() - t0, 1)

            if result.success:
                # Build rich response
                header = f"🔬 **Deep Research Report**\n"
                if stock_code:
                    header += f"Stock: {stock_code}\n"
                header += f"Sub-questions: {len(result.sub_questions)} | Sources: {result.findings_count}\n"
                header += f"Time: {duration}s | Tokens: {result.total_tokens:,}\n"
                header += "─" * 40 + "\n\n"

                report = header + result.report

                # Truncate if too long for bot message
                max_len = 4000
                if len(report) > max_len:
                    report = report[:max_len] + "\n\n... (report truncated, full report available via API)"

                return BotResponse.markdown_response(report)
            else:
                return BotResponse.text_response(
                    f"⚠️ Research did not complete successfully.\n"
                    f"Partial results: {result.findings_count} findings collected.\n"
                    f"Time: {duration}s"
                )

        except Exception as exc:
            logger.error("[ResearchCommand] Error: %s", exc, exc_info=True)
            return BotResponse.text_response(f"❌ Research failed: {exc}")

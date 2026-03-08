# -*- coding: utf-8 -*-
"""
TechnicalAgent — technical & price analysis specialist.

Responsible for:
- Fetching realtime quotes and historical K-line data
- Running technical indicators (trend, MA, volume, pattern)
- Producing a structured opinion on trend/momentum/support-resistance
"""

from __future__ import annotations

import json
import logging
import re
from typing import List, Optional

from src.agent.agents.base_agent import BaseAgent
from src.agent.protocols import AgentContext, AgentOpinion

logger = logging.getLogger(__name__)


class TechnicalAgent(BaseAgent):
    agent_name = "technical"
    max_steps = 6
    tool_names = [
        "get_realtime_quote",
        "get_daily_history",
        "analyze_trend",
        "calculate_ma",
        "get_volume_analysis",
        "analyze_pattern",
        "get_chip_distribution",
        "get_analysis_context",
    ]

    def system_prompt(self, ctx: AgentContext) -> str:
        skills = ""
        if self.skill_instructions:
            skills = f"\n## Active Trading Strategies\n\n{self.skill_instructions}\n"

        return f"""\
You are a **Technical Analysis Agent** specialising in Chinese A-shares, \
Hong Kong stocks, and US equities.

Your task: perform a thorough technical analysis of the given stock and \
output a structured JSON opinion.

## Workflow (execute stages in order)
1. Fetch realtime quote + daily history (if not already provided)
2. Run trend analysis (MA alignment, MACD, RSI)
3. Analyse volume and chip distribution
4. Identify chart patterns

## Core Trading Rules
- Bullish alignment: MA5 > MA10 > MA20
- Bias from MA5 < 2% → ideal buy zone; 2-5% → small position; > 5% → no chase
- Shrink-pullback to MA5 is the best buy pattern
- Below MA20 → hold off
{skills}
## Output Format
Return **only** a JSON object (no markdown fences):
{{
  "signal": "strong_buy|buy|hold|sell|strong_sell",
  "confidence": 0.0-1.0,
  "reasoning": "2-3 sentence summary",
  "key_levels": {{
    "support": <float>,
    "resistance": <float>,
    "stop_loss": <float>
  }},
  "trend_score": 0-100,
  "ma_alignment": "bullish|neutral|bearish",
  "volume_status": "heavy|normal|light",
  "pattern": "<detected pattern or none>"
}}
"""

    def build_user_message(self, ctx: AgentContext) -> str:
        parts = [f"Perform technical analysis on stock **{ctx.stock_code}**"]
        if ctx.stock_name:
            parts[0] += f" ({ctx.stock_name})"
        parts.append("Use your tools to fetch any missing data, then output the JSON opinion.")
        return "\n".join(parts)

    def post_process(self, ctx: AgentContext, raw_text: str) -> Optional[AgentOpinion]:
        """Parse the JSON opinion from the LLM response."""
        parsed = _try_parse_json(raw_text)
        if parsed is None:
            logger.warning("[TechnicalAgent] failed to parse opinion JSON")
            return None

        return AgentOpinion(
            agent_name=self.agent_name,
            signal=parsed.get("signal", "hold"),
            confidence=float(parsed.get("confidence", 0.5)),
            reasoning=parsed.get("reasoning", ""),
            key_levels={
                k: float(v) for k, v in parsed.get("key_levels", {}).items()
                if isinstance(v, (int, float))
            },
            raw_data=parsed,
        )


def _try_parse_json(text: str) -> Optional[dict]:
    """Best-effort JSON parse from LLM text."""
    # Strip markdown fences
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
        cleaned = re.sub(r'\s*```$', '', cleaned)
    try:
        obj = json.loads(cleaned)
        return obj if isinstance(obj, dict) else None
    except (json.JSONDecodeError, ValueError):
        pass
    # Try brace-delimited
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            obj = json.loads(text[start:end + 1])
            return obj if isinstance(obj, dict) else None
        except (json.JSONDecodeError, ValueError):
            pass
    return None

# -*- coding: utf-8 -*-
"""
DecisionAgent — final synthesis and decision-making specialist.

Responsible for:
- Aggregating opinions from technical + intel + risk + strategy agents
- Producing the final Decision Dashboard JSON
- Generating actionable buy/hold/sell recommendations with price levels
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from src.agent.agents.base_agent import BaseAgent
from src.agent.protocols import AgentContext, AgentOpinion

logger = logging.getLogger(__name__)


class DecisionAgent(BaseAgent):
    """Synthesise prior agent opinions into the final dashboard."""

    agent_name = "decision"
    max_steps = 3  # pure synthesis, should not need many tool calls
    tool_names: Optional[List[str]] = []  # no tool access — works from context only

    def system_prompt(self, ctx: AgentContext) -> str:
        skills = ""
        if self.skill_instructions:
            skills = f"\n## Active Trading Strategies\n\n{self.skill_instructions}\n"

        return f"""\
You are a **Decision Synthesis Agent** that produces the final investment \
Decision Dashboard.

You will receive:
1. Structured opinions from a Technical Agent and an Intel Agent
2. Any risk flags raised by a Risk Agent
3. Strategy evaluation results (if applicable)

Your task: synthesise all inputs into a single, actionable Decision Dashboard.
{skills}
## Core Principles
1. **Core conclusion first** — one sentence, ≤30 chars
2. **Split advice** — different for no-position vs has-position
3. **Precise sniper levels** — concrete price numbers, no hedging
4. **Checklist visual** — ✅⚠️❌ for each checkpoint
5. **Risk priority** — risk alerts must be prominent. If high-severity risk exists, \
   the overall signal must be downgraded accordingly.

## Signal Weighting Guidelines
- Technical opinion weight: ~40%
- Intel / sentiment weight: ~30%
- Risk flags weight: ~30% (negative override: any high-severity risk caps signal at "hold")
- If a strategy opinion is present, blend it at 20% weight (reducing others proportionally)

## Scoring
- 80-100: strong_buy (all conditions met, low risk)
- 60-79: buy (mostly positive, minor caveats)
- 40-59: hold (mixed signals, or risk present)
- 20-39: sell (negative trend + risk)
- 0-19: strong_sell (major risk + bearish)

## Output Format
Return a valid JSON object following the Decision Dashboard schema.  The JSON \
must include at minimum these top-level keys:
  stock_name, sentiment_score, trend_prediction, operation_advice,
  decision_type, confidence_level, dashboard, analysis_summary,
  key_points, risk_warning
"""

    def build_user_message(self, ctx: AgentContext) -> str:
        parts = [
            f"# Synthesis Request for {ctx.stock_code}",
            f"Stock: {ctx.stock_code} ({ctx.stock_name})" if ctx.stock_name else f"Stock: {ctx.stock_code}",
            "",
        ]

        # Feed prior opinions
        if ctx.opinions:
            parts.append("## Agent Opinions")
            for op in ctx.opinions:
                parts.append(f"\n### {op.agent_name}")
                parts.append(f"Signal: {op.signal} | Confidence: {op.confidence:.2f}")
                parts.append(f"Reasoning: {op.reasoning}")
                if op.key_levels:
                    parts.append(f"Key levels: {json.dumps(op.key_levels)}")
                if op.raw_data:
                    extra_keys = {k: v for k, v in op.raw_data.items()
                                  if k not in ("signal", "confidence", "reasoning", "key_levels")}
                    if extra_keys:
                        parts.append(f"Extra data: {json.dumps(extra_keys, ensure_ascii=False, default=str)}")
                parts.append("")

        # Feed risk flags
        if ctx.risk_flags:
            parts.append("## Risk Flags")
            for rf in ctx.risk_flags:
                parts.append(f"- [{rf.get('severity', 'medium')}] {rf.get('category', '')}: {rf.get('description', '')}")
            parts.append("")

        # Strategy meta
        if ctx.meta.get("strategies_requested"):
            parts.append(f"## Strategies: {', '.join(ctx.meta['strategies_requested'])}")
            parts.append("")

        parts.append("Synthesise the above into the Decision Dashboard JSON.")
        return "\n".join(parts)

    def post_process(self, ctx: AgentContext, raw_text: str) -> Optional[AgentOpinion]:
        """Store the parsed dashboard in ctx.meta; also return an opinion."""
        from src.agent.runner import parse_dashboard_json

        dashboard = parse_dashboard_json(raw_text)
        if dashboard:
            ctx.set_data("final_dashboard", dashboard)
            return AgentOpinion(
                agent_name=self.agent_name,
                signal=dashboard.get("decision_type", "hold"),
                confidence=min(1.0, (dashboard.get("sentiment_score", 50) or 50) / 100.0),
                reasoning=dashboard.get("analysis_summary", ""),
                raw_data=dashboard,
            )
        else:
            # Even if JSON parsing fails, store the raw text for downstream use
            ctx.set_data("final_dashboard_raw", raw_text)
            logger.warning("[DecisionAgent] failed to parse dashboard JSON")
            return None

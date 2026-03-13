# -*- coding: utf-8 -*-
"""
Research command — deep multi-step stock analysis using the Agent pipeline.

Usage:
    /research 600519            -> Deep analysis with all default strategies
    /research 600519 full       -> Deep analysis with full dashboard output
"""

import logging
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import List, Optional

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse
from data_provider.base import canonical_stock_code
from src.config import get_config

logger = logging.getLogger(__name__)

# Default timeout for deep research (seconds).  Longer than /ask because the
# agent runs more steps and searches more broadly.
_DEFAULT_TIMEOUT_SECONDS = 180


class ResearchCommand(BotCommand):
    """
    Deep research command — runs the Agent with extended steps for comprehensive
    stock analysis, applying all default strategies simultaneously.

    Usage:
        /research 600519           -> Deep analysis with default strategies
        /research 600519 full      -> Full dashboard output (not truncated)
        /research hk00700          -> HK stock deep research
    """

    @property
    def name(self) -> str:
        return "research"

    @property
    def aliases(self) -> List[str]:
        return ["深研", "深度分析", "r"]

    @property
    def description(self) -> str:
        return "深度研究股票（多策略 Agent 综合分析）"

    @property
    def usage(self) -> str:
        return "/research <股票代码> [full]"

    def validate_args(self, args: List[str]) -> Optional[str]:
        """Validate arguments."""
        if not args:
            return (
                "请输入股票代码。用法: /research <股票代码> [full]\n"
                "示例: /research 600519"
            )

        code = args[0].upper()
        is_a_stock = re.match(r"^\d{6}$", code)
        is_hk_stock = re.match(r"^HK\d{5}$", code)
        is_us_stock = re.match(r"^[A-Z]{1,5}(\.[A-Z]{1,2})?$", code)

        if not (is_a_stock or is_hk_stock or is_us_stock):
            return (
                f"无效的股票代码: {code}\n"
                "（A股6位数字 / 港股HK+5位数字 / 美股1-5个字母）"
            )

        return None

    def execute(self, message: BotMessage, args: List[str]) -> BotResponse:
        """Execute deep research via Agent pipeline with timeout protection."""
        config = get_config()

        if not config.agent_mode:
            return BotResponse.text_response(
                "⚠️ Agent 模式未开启，无法使用深度研究功能。\n"
                "请在配置中设置 `AGENT_MODE=true`。"
            )

        code = canonical_stock_code(args[0])
        full_mode = len(args) > 1 and args[1].lower() in ("full", "完整", "详细")

        logger.info("[ResearchCommand] Stock: %s, full=%s", code, full_mode)

        timeout = getattr(config, "agent_research_timeout", _DEFAULT_TIMEOUT_SECONDS)

        try:
            from src.agent.factory import build_agent_executor, DEFAULT_AGENT_SKILLS

            # Deep research uses all default strategies and more steps
            executor = build_agent_executor(config, skills=DEFAULT_AGENT_SKILLS)

            task = f"请对股票 {code} 进行深度综合分析，应用所有可用策略，全面评估技术面、基本面和市场情绪。"
            session_id = f"research_{code}_{uuid.uuid4()}"

            def _run():
                return executor.run(task=task)

            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(_run)
                try:
                    result = future.result(timeout=timeout)
                except FuturesTimeoutError:
                    logger.warning(
                        "[ResearchCommand] Research timed out after %ds for %s",
                        timeout, code,
                    )
                    return BotResponse.text_response(
                        f"⏱️ 深度研究超时（>{timeout}s），请稍后重试或使用 /ask 快速分析。"
                    )

            if result.success:
                header = f"🔬 {code} 深度研究报告\n{'═' * 32}\n"
                body = result.content
                if not full_mode and len(body) > 2000:
                    body = body[:2000] + f"\n\n…（使用 `/research {code} full` 查看完整报告）"
                return BotResponse.text_response(header + body)
            else:
                return BotResponse.text_response(
                    f"⚠️ 深度研究失败: {result.error}"
                )

        except Exception as e:
            logger.error("[ResearchCommand] Research command failed: %s", e)
            logger.exception("Research error details:")
            return BotResponse.text_response(f"⚠️ 深度研究执行出错: {str(e)}")

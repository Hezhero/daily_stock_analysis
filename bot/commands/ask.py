# -*- coding: utf-8 -*-
"""
Ask command - analyze a stock using a specific Agent strategy.

Usage:
    /ask 600519                        -> Analyze with default strategy
    /ask 600519 用缠论分析              -> Parse strategy from message
    /ask 600519 chan_theory             -> Specify strategy id directly
"""

import re
import logging
import uuid
from typing import Dict, List, Optional

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse
from data_provider.base import canonical_stock_code
from src.config import get_config

logger = logging.getLogger(__name__)

# Strategy name to id mapping (CN name -> strategy id)
STRATEGY_NAME_MAP = {
    "缠论": "chan_theory",
    "缠论分析": "chan_theory",
    "波浪": "wave_theory",
    "波浪理论": "wave_theory",
    "艾略特": "wave_theory",
    "箱体": "box_oscillation",
    "箱体震荡": "box_oscillation",
    "情绪": "emotion_cycle",
    "情绪周期": "emotion_cycle",
    "趋势": "bull_trend",
    "多头趋势": "bull_trend",
    "均线金叉": "ma_golden_cross",
    "金叉": "ma_golden_cross",
    "缩量回踩": "shrink_pullback",
    "回踩": "shrink_pullback",
    "放量突破": "volume_breakout",
    "突破": "volume_breakout",
    "地量见底": "bottom_volume",
    "龙头": "dragon_head",
    "龙头战法": "dragon_head",
    "一阳穿三阴": "one_yang_three_yin",
}


class AskCommand(BotCommand):
    """
    Ask command handler - invoke Agent with a specific strategy to analyze a stock.

    Usage:
        /ask 600519                    -> Analyze with default strategy (bull_trend)
        /ask 600519 用缠论分析          -> Automatically selects chan_theory strategy
        /ask 600519 chan_theory         -> Directly specify strategy id
        /ask hk00700 波浪理论看看       -> HK stock with wave_theory
    """

    @property
    def name(self) -> str:
        return "ask"

    @property
    def aliases(self) -> List[str]:
        return ["问股"]

    @property
    def description(self) -> str:
        return "使用 Agent 策略分析股票"

    @property
    def usage(self) -> str:
        return "/ask <股票代码[,代码2,...]> [策略名称]"

    def _parse_stock_codes(self, raw: str) -> List[str]:
        """Parse one or more stock codes from the first argument.

        Supports:
        - Single: ``600519``
        - Comma separated: ``600519,000858``
        - ``vs`` separated: ``600519 vs 000858`` (handled at arg level)
        """
        # Split by comma
        parts = [p.strip().upper() for p in raw.replace("，", ",").split(",") if p.strip()]
        codes = []
        for p in parts:
            codes.append(canonical_stock_code(p))
        return codes

    def _validate_single_code(self, code: str) -> Optional[str]:
        """Validate a single stock code format. Returns error string or None."""
        c = code.upper()
        is_a = re.match(r"^\d{6}$", c)
        is_hk = re.match(r"^HK\d{5}$", c)
        is_us = re.match(r"^[A-Z]{1,5}(\.[A-Z]{1,2})?$", c)
        if not (is_a or is_hk or is_us):
            return f"无效的股票代码: {c}（A股6位数字 / 港股HK+5位数字 / 美股1-5个字母）"
        return None

    def validate_args(self, args: List[str]) -> Optional[str]:
        """Validate arguments."""
        if not args:
            return "请输入股票代码。用法: /ask <股票代码> [策略名称]\n示例: /ask 600519 用缠论分析"

        # Handle "600519 vs 000858" — merge into comma form
        raw_codes_parts = [args[0]]
        rest_args = list(args[1:])
        while rest_args and rest_args[0].lower() == "vs" and len(rest_args) > 1:
            raw_codes_parts.append(rest_args[1])
            rest_args = rest_args[2:]
        raw_code_str = ",".join(raw_codes_parts)

        codes = self._parse_stock_codes(raw_code_str)
        if not codes:
            return "请输入至少一个有效的股票代码"

        for c in codes:
            err = self._validate_single_code(c)
            if err:
                return err

        if len(codes) > 5:
            return "一次最多分析 5 只股票"

        return None

    def _parse_strategy(self, args: List[str]) -> str:
        """Parse strategy from arguments, returning strategy id."""
        if len(args) < 2:
            return "bull_trend"

        # Join remaining args as the strategy text
        strategy_text = " ".join(args[1:]).strip()

        # Try direct strategy id match first
        try:
            from src.agent.factory import get_skill_manager
            sm = get_skill_manager()
            available_ids = [s.name for s in sm.list_skills()]
            if strategy_text in available_ids:
                return strategy_text
        except Exception:
            pass

        # Try CN name mapping
        for cn_name, strategy_id in STRATEGY_NAME_MAP.items():
            if cn_name in strategy_text:
                return strategy_id

        # Default
        return "bull_trend"

    def _get_strategy_args(self, args: List[str]) -> List[str]:
        """Extract strategy-related args (everything after codes and 'vs' tokens)."""
        # Skip leading code tokens and 'vs'
        # Regex must accept dotted tickers (e.g. BRK.B) to stay consistent
        # with _validate_single_code.
        _CODE_RE = r"^(\d{6}|hk\d{5}|[A-Za-z]{1,5}(\.[A-Za-z]{1,2})?)$"
        rest = list(args[1:])
        while rest and (rest[0].lower() == "vs" or re.match(_CODE_RE, rest[0], re.IGNORECASE)):
            rest = rest[1:] if rest[0].lower() == "vs" else rest
            if rest and re.match(_CODE_RE, rest[0], re.IGNORECASE):
                rest = rest[1:]
        return rest

    def execute(self, message: BotMessage, args: List[str]) -> BotResponse:
        """Execute the ask command via Agent pipeline. Supports multi-stock."""
        config = get_config()

        if not config.is_agent_available():
            return BotResponse.text_response(
                "⚠️ Agent 模式不可用，无法使用问股功能。\n请配置 `LITELLM_MODEL` 或设置 `AGENT_MODE=true`。"
            )

        # Parse stock codes — handle "600519,000858" and "600519 vs 000858"
        raw_codes_parts = [args[0]]
        rest_args = list(args[1:])
        while rest_args and rest_args[0].lower() == "vs" and len(rest_args) > 1:
            raw_codes_parts.append(rest_args[1])
            rest_args = rest_args[2:]
        raw_code_str = ",".join(raw_codes_parts)
        codes = self._parse_stock_codes(raw_code_str)

        strategy_args = self._get_strategy_args(args)
        strategy_id = self._parse_strategy(["placeholder"] + strategy_args) if strategy_args else self._parse_strategy(args)
        strategy_text = " ".join(strategy_args).strip()

        logger.info(f"[AskCommand] Stocks: {codes}, Strategy: {strategy_id}, Extra: {strategy_text}")

        # Single stock — original path
        if len(codes) == 1:
            return self._analyze_single(config, message, codes[0], strategy_id, strategy_text)

        # Multi-stock — parallel analysis + comparison
        return self._analyze_multi(config, message, codes, strategy_id, strategy_text)

    def _resolve_strategy_name(self, strategy_id: str) -> str:
        """Resolve strategy id to display name."""
        try:
            from src.agent.factory import get_skill_manager
            sm = get_skill_manager()
            for s in sm.list_skills():
                if s.name == strategy_id:
                    return s.display_name
        except Exception:
            pass
        return strategy_id

    def _analyze_single(self, config, message: BotMessage, code: str, strategy_id: str, strategy_text: str) -> BotResponse:
        """Analyze a single stock."""
        try:
            from src.agent.factory import build_agent_executor
            executor = build_agent_executor(config, skills=[strategy_id] if strategy_id else None)

            user_msg = f"请使用 {strategy_id} 策略分析股票 {code}"
            if strategy_text:
                user_msg = f"请分析股票 {code}，{strategy_text}"

            session_id = f"{message.platform}_{message.user_id}:ask_{code}_{uuid.uuid4()}"
            result = executor.chat(message=user_msg, session_id=session_id)

            if result.success:
                strategy_name = self._resolve_strategy_name(strategy_id)
                header = f"📊 {code} | 策略: {strategy_name}\n{'─' * 30}\n"
                return BotResponse.text_response(header + result.content)
            else:
                return BotResponse.text_response(f"⚠️ 分析失败: {result.error}")

        except Exception as e:
            logger.error(f"Ask command failed: {e}")
            logger.exception("Ask error details:")
            return BotResponse.text_response(f"⚠️ 问股执行出错: {str(e)}")

    def _analyze_multi(self, config, message: BotMessage, codes: List[str], strategy_id: str, strategy_text: str) -> BotResponse:
        """Analyze multiple stocks in parallel and produce a comparison summary."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        strategy_name = self._resolve_strategy_name(strategy_id)
        results: Dict[str, str] = {}
        errors: Dict[str, str] = {}

        def _run_one(stock_code: str) -> tuple:
            """Returns (stock_code, content_or_None, error_or_None)."""
            try:
                from src.agent.factory import build_agent_executor
                executor = build_agent_executor(config, skills=[strategy_id] if strategy_id else None)
                user_msg = f"请使用 {strategy_id} 策略分析股票 {stock_code}"
                if strategy_text:
                    user_msg = f"请分析股票 {stock_code}，{strategy_text}"
                session_id = f"{message.platform}_{message.user_id}:ask_{stock_code}_{uuid.uuid4()}"
                result = executor.chat(message=user_msg, session_id=session_id)
                if result.success:
                    return (stock_code, result.content, None)
                else:
                    return (stock_code, None, result.error or "未知错误")
            except Exception as e:
                return (stock_code, None, str(e))

        with ThreadPoolExecutor(max_workers=min(len(codes), 5)) as pool:
            future_map = {pool.submit(_run_one, c): c for c in codes}
            try:
                for future in as_completed(future_map, timeout=150):
                    try:
                        code, content, err = future.result(timeout=5)
                        if content is not None:
                            results[code] = content
                        else:
                            errors[code] = err or "未知错误"
                    except Exception as exc:
                        code = future_map[future]
                        errors[code] = f"执行异常: {exc}"
            except TimeoutError:
                # Some futures didn't finish within the deadline — collect
                # whatever has completed and mark the rest as timed-out.
                logger.warning("[AskCommand] Multi-stock analysis hit overall timeout (150s)")
                for fut, code in future_map.items():
                    if code in results or code in errors:
                        continue
                    if fut.done():
                        try:
                            code_r, content, err = fut.result(timeout=0)
                            if content is not None:
                                results[code_r] = content
                            else:
                                errors[code_r] = err or "未知错误"
                        except Exception as exc:
                            errors[code] = f"执行异常: {exc}"
                    else:
                        fut.cancel()
                        errors[code] = "分析超时（未在 150 秒内完成）"

        # Check for codes that never completed (shouldn't happen with pool, but be safe)
        for code in codes:
            if code not in results and code not in errors:
                errors[code] = "分析超时"

        # Build combined response
        parts = [f"📊 **多股对比分析** | 策略: {strategy_name}", f"{'─' * 30}", ""]

        # Quick-reference comparison table (best-effort, extracted from text)
        if len(results) >= 2:
            parts.append("| 股票 | 状态 |")
            parts.append("|------|------|")
            for code in codes:
                if code in results:
                    # Extract first meaningful line as summary (skip empty / header lines)
                    summary_line = ""
                    for line in results[code].splitlines():
                        stripped = line.strip()
                        if stripped and len(stripped) > 4 and not stripped.startswith(("#", "─", "=", "📊")):
                            summary_line = stripped[:80]
                            break
                    parts.append(f"| {code} | {summary_line or '分析完成'} |")
                elif code in errors:
                    parts.append(f"| {code} | ⚠️ {errors[code][:40]} |")
            parts.append("")

        # Individual detail sections
        for code in codes:
            if code in results:
                # Truncate individual results in multi-stock mode for readability
                content = results[code]
                if len(content) > 800:
                    content = content[:800] + "\n... (已截断，完整分析请单独查询)"
                parts.append(f"### {code}")
                parts.append(content)
                parts.append("")
            elif code in errors:
                parts.append(f"### {code}")
                parts.append(f"⚠️ 分析失败: {errors[code]}")
                parts.append("")

        return BotResponse.markdown_response("\n".join(parts))

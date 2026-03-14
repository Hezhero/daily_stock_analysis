# -*- coding: utf-8 -*-
"""
Ask command - analyze a stock using a specific Agent strategy.

Usage:
    /ask 600519                        -> Analyze with default strategy
    /ask 600519 用缠论分析              -> Parse strategy from message
    /ask 600519 chan_theory             -> Specify strategy id directly
"""

import logging
import re
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse
from data_provider.base import canonical_stock_code
from src.config import get_config
from src.storage import get_db

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

    _MULTI_ANALYZE_TIMEOUT_S = 150.0

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

    def _merge_code_args(self, args: List[str]) -> tuple[str, List[str]]:
        """Merge stock code arguments separated by commas or explicit ``vs`` markers.

        Returns ``(raw_code_str, remaining_args)`` where ``remaining_args`` are
        preserved for strategy parsing. This intentionally does not treat a
        plain space-separated token as another stock code; otherwise words like
        ``trend`` would be swallowed as fake US tickers.
        """
        if not args:
            return "", []

        code_like = re.compile(
            r"^,?(\d{6}|hk\d{5}|[A-Za-z]{1,5}(\.[A-Za-z]{1,2})?),?$",
            re.IGNORECASE,
        )
        raw_codes_parts = [args[0]]
        rest_args = list(args[1:])

        while rest_args:
            token = rest_args[0]
            prev = raw_codes_parts[-1].rstrip()

            if token.lower() == "vs" and len(rest_args) > 1 and code_like.match(rest_args[1]):
                raw_codes_parts.append(rest_args[1])
                rest_args = rest_args[2:]
                continue

            has_comma_separator = (
                prev.endswith(",")
                or prev.endswith("，")
                or token.lstrip().startswith(",")
                or token.lstrip().startswith("，")
            )
            if code_like.match(token) and has_comma_separator:
                raw_codes_parts.append(token)
                rest_args = rest_args[1:]
                continue

            break

        normalized_parts = [part.strip(",，") for part in raw_codes_parts]
        raw_code_str = ",".join(normalized_parts)
        return raw_code_str, rest_args

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
            return "请输入股票代码"

        # Handle "600519 vs 000858", "600519, 000858", "600519,000858"
        raw_code_str, _ = self._merge_code_args(args)

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

        # Parse stock codes — handle "600519,000858", "600519 vs 000858", "600519, 000858"
        raw_code_str, remaining_args = self._merge_code_args(args)
        codes = self._parse_stock_codes(raw_code_str)

        strategy_args = remaining_args
        strategy_id = self._parse_strategy(["placeholder"] + strategy_args) if strategy_args else self._parse_strategy(args)
        strategy_text = " ".join(strategy_args).strip()

        logger.info(f"[AskCommand] Stocks: {codes}, Strategy: {strategy_id}, Extra: {strategy_text}")

        # Single stock — original path
        if len(codes) == 1:
            return self._analyze_single(config, message, codes[0], strategy_id, strategy_text)

        # Multi-stock — parallel analysis + comparison
        return self._analyze_multi(config, message, codes, strategy_id, strategy_text)

    def _resolve_strategy_name(self, strategy_id: Optional[str]) -> str:
        """Resolve strategy id to display name."""
        if not strategy_id:
            return "default"
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

            user_msg = f"请分析股票 {code}"
            if strategy_id:
                user_msg = f"请使用 {strategy_id} 策略分析股票 {code}"
            if strategy_text:
                user_msg = f"请分析股票 {code}，{strategy_text}"

            session_id = f"{message.platform}_{message.user_id}:ask_{code}_{uuid.uuid4()}"
            result = executor.chat(
                message=user_msg,
                session_id=session_id,
                context={
                    "stock_code": code,
                    "strategies": [strategy_id] if strategy_id else [],
                },
            )

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
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError, as_completed

        strategy_name = self._resolve_strategy_name(strategy_id)
        results: Dict[str, Dict[str, Any]] = {}
        errors: Dict[str, str] = {}
        started_at = time.monotonic()
        overall_timeout_s = self._MULTI_ANALYZE_TIMEOUT_S

        platform = message.platform
        user_id = message.user_id

        def _run_one(stock_code: str) -> Tuple[str, Optional[Dict[str, Any]], Optional[str]]:
            """Return structured single-stock analysis for portfolio overlay.

            Results are persisted to conversation history so they show up in
            ``/history``, matching the behaviour of the single-stock path
            which uses ``executor.chat()``.
            """
            try:
                from src.agent.factory import build_agent_executor
                from src.agent.conversation import conversation_manager

                executor = build_agent_executor(config, skills=[strategy_id] if strategy_id else None)
                user_msg = f"请分析股票 {stock_code}"
                if strategy_id:
                    user_msg = f"请使用 {strategy_id} 策略分析股票 {stock_code}"
                if strategy_text:
                    user_msg = f"请分析股票 {stock_code}，{strategy_text}"

                session_id = f"{platform}_{user_id}:ask_{stock_code}_{uuid.uuid4()}"
                conversation_manager.add_message(session_id, "user", user_msg)

                result = executor.run(
                    task=user_msg,
                    context={
                        "stock_code": stock_code,
                        "strategies": [strategy_id] if strategy_id else [],
                    },
                )
                if result.success or self._should_accept_fallback_content(result):
                    dashboard = result.dashboard if isinstance(result.dashboard, dict) else None
                    formatted_analysis = self._format_stock_result(stock_code, dashboard, result.content)
                    conversation_manager.add_message(session_id, "assistant", formatted_analysis)
                    return (stock_code, {
                        "content": result.content,
                        "dashboard": dashboard,
                        "signal": self._extract_signal(dashboard),
                        "confidence": self._extract_confidence(dashboard),
                        "summary": self._extract_summary(stock_code, dashboard, result.content),
                        "markdown": formatted_analysis,
                        "stock_name": self._extract_stock_name(stock_code, dashboard),
                        "risk_flags": self._extract_risk_flags(dashboard),
                    }, None)
                else:
                    error_note = f"[分析失败] {result.error or '未知错误'}"
                    conversation_manager.add_message(session_id, "assistant", error_note)
                    return (stock_code, None, result.error or "未知错误")
            except Exception as e:
                return (stock_code, None, str(e))

        # IMPORTANT: Do NOT use `with ThreadPoolExecutor(...)` here.
        # The context-manager __exit__ calls shutdown(wait=True), which blocks
        # until every submitted thread finishes — that defeats the 150s timeout.
        # Instead we manage the pool lifecycle explicitly and call
        # shutdown(wait=False) on the timeout path so the Bot response returns
        # immediately.
        get_db()
        pool = ThreadPoolExecutor(max_workers=min(len(codes), 5))
        future_map = {pool.submit(_run_one, c): c for c in codes}
        try:
            for future in as_completed(future_map, timeout=overall_timeout_s):
                try:
                    code, content, err = future.result(timeout=5)
                    if content is not None:
                        results[code] = content
                    else:
                        errors[code] = err or "未知错误"
                except Exception as exc:
                    code = future_map[future]
                    errors[code] = f"执行异常: {exc}"
        except FutureTimeoutError:
            # Some futures didn't finish within the deadline — collect
            # whatever has completed and mark the rest as timed-out.
            logger.warning("[AskCommand] Multi-stock analysis hit overall timeout (%.1fs)", overall_timeout_s)
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
                    errors[code] = "分析超时（未在 150 秒内完成）"
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

        # Check for codes that never completed (shouldn't happen with pool, but be safe)
        for code in codes:
            if code not in results and code not in errors:
                errors[code] = "分析超时"

        # Build combined response
        parts = [f"📊 **多股对比分析** | 策略: {strategy_name}", f"{'─' * 30}", ""]

        remaining_timeout_s = max(0.0, overall_timeout_s - (time.monotonic() - started_at))
        portfolio_section = self._build_portfolio_section(
            config,
            codes,
            results,
            timeout_s=remaining_timeout_s,
        )
        if portfolio_section:
            parts.append(portfolio_section)
            parts.append("")

        # Quick-reference comparison table (best-effort, extracted from text)
        if len(results) >= 2:
            parts.append("| 股票 | 信号 | 置信度 | 摘要 |")
            parts.append("|------|------|--------|------|")
            for code in codes:
                if code in results:
                    item = results[code]
                    signal = item.get("signal") or "unknown"
                    confidence = item.get("confidence")
                    confidence_text = f"{confidence:.0%}" if isinstance(confidence, (int, float)) else "-"
                    summary_line = str(item.get("summary") or "分析完成").replace("|", "/")[:80]
                    parts.append(f"| {code} | {signal} | {confidence_text} | {summary_line} |")
                elif code in errors:
                    parts.append(f"| {code} | error | - | ⚠️ {errors[code][:40]} |")
            parts.append("")

        # Individual detail sections
        for code in codes:
            if code in results:
                parts.append(f"### {code}")
                parts.append(results[code]["markdown"])
                parts.append("")
            elif code in errors:
                parts.append(f"### {code}")
                parts.append(f"⚠️ 分析失败: {errors[code]}")
                parts.append("")

        return BotResponse.markdown_response("\n".join(parts))

    @staticmethod
    def _should_accept_fallback_content(result: Any) -> bool:
        """Keep usable free-form answers when dashboard JSON parsing fails."""
        if getattr(result, "success", False):
            return True

        content = getattr(result, "content", "")
        error = str(getattr(result, "error", "") or "")
        if not isinstance(content, str) or not content.strip():
            return False

        return error == "Failed to parse dashboard JSON from agent response"

    @staticmethod
    def _extract_stock_name(stock_code: str, dashboard: Optional[Dict[str, Any]]) -> str:
        if isinstance(dashboard, dict):
            stock_name = dashboard.get("stock_name")
            if isinstance(stock_name, str) and stock_name.strip():
                return stock_name.strip()
        return stock_code

    @staticmethod
    def _extract_signal(dashboard: Optional[Dict[str, Any]]) -> str:
        if isinstance(dashboard, dict):
            signal = dashboard.get("decision_type")
            if isinstance(signal, str) and signal.strip():
                return signal.strip()
        return "unknown"

    @staticmethod
    def _extract_confidence(dashboard: Optional[Dict[str, Any]]) -> Optional[float]:
        if not isinstance(dashboard, dict):
            return None

        score = dashboard.get("sentiment_score")
        try:
            return max(0.0, min(1.0, float(score) / 100.0))
        except (TypeError, ValueError):
            pass

        level = str(dashboard.get("confidence_level") or "").strip()
        mapping = {"高": 0.85, "中": 0.65, "低": 0.45}
        return mapping.get(level)

    @staticmethod
    def _extract_summary(stock_code: str, dashboard: Optional[Dict[str, Any]], raw_content: str) -> str:
        if isinstance(dashboard, dict):
            for key in ("analysis_summary", "risk_warning", "trend_prediction"):
                value = dashboard.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            dashboard_block = dashboard.get("dashboard")
            if not isinstance(dashboard_block, dict):
                dashboard_block = {}
            core_conclusion = dashboard_block.get("core_conclusion")
            if not isinstance(core_conclusion, dict):
                core_conclusion = {}
            core = core_conclusion.get("one_sentence")
            if isinstance(core, str) and core.strip():
                return core.strip()

        for line in raw_content.splitlines():
            stripped = line.strip()
            if stripped and len(stripped) > 4 and not stripped.startswith(("{", "}", "\"")):
                return stripped[:120]
        return f"{stock_code} 分析完成"

    @staticmethod
    def _extract_risk_flags(dashboard: Optional[Dict[str, Any]]) -> List[Dict[str, str]]:
        if not isinstance(dashboard, dict):
            return []

        flags: List[Dict[str, str]] = []
        dashboard_block = dashboard.get("dashboard")
        if not isinstance(dashboard_block, dict):
            dashboard_block = {}
        intelligence = dashboard_block.get("intelligence")
        if not isinstance(intelligence, dict):
            intelligence = {}
        for alert in intelligence.get("risk_alerts", [])[:5]:
            if isinstance(alert, str) and alert.strip():
                flags.append({"category": "portfolio_input", "description": alert.strip(), "severity": "medium"})

        risk_warning = dashboard.get("risk_warning")
        if isinstance(risk_warning, str) and risk_warning.strip():
            flags.append({"category": "portfolio_input", "description": risk_warning.strip(), "severity": "medium"})
        return flags

    @staticmethod
    def _format_sniper_value(value: Any) -> Optional[str]:
        if value is None:
            return None

        text = str(value).strip()
        if not text or text in {"-", "—", "N/A", "None"}:
            return None

        prefixes = (
            "理想买入点：",
            "次优买入点：",
            "止损位：",
            "目标位：",
            "理想买入点:",
            "次优买入点:",
            "止损位:",
            "目标位:",
        )
        for prefix in prefixes:
            if text.startswith(prefix):
                stripped = text[len(prefix):].strip()
                return stripped or None

        return text

    @staticmethod
    def _format_stock_result(stock_code: str, dashboard: Optional[Dict[str, Any]], raw_content: str) -> str:
        if not isinstance(dashboard, dict):
            content = raw_content
            if len(content) > 800:
                content = content[:800] + "\n... (已截断，完整分析请单独查询)"
            return content

        lines = []
        stock_name = dashboard.get("stock_name")
        if isinstance(stock_name, str) and stock_name.strip() and stock_name.strip() != stock_code:
            lines.append(f"**名称**: {stock_name.strip()}")

        decision = dashboard.get("decision_type")
        confidence = AskCommand._extract_confidence(dashboard)
        trend = dashboard.get("trend_prediction")
        if isinstance(decision, str):
            lines.append(
                f"**结论**: {decision}"
                + (f" | **置信度**: {confidence:.0%}" if isinstance(confidence, (int, float)) else "")
                + (f" | **趋势**: {trend}" if isinstance(trend, str) and trend.strip() else "")
            )

        summary = AskCommand._extract_summary(stock_code, dashboard, raw_content)
        if summary:
            lines.append(f"**摘要**: {summary}")

        operation = dashboard.get("operation_advice")
        if isinstance(operation, str) and operation.strip():
            lines.append(f"**操作建议**: {operation.strip()}")

        risk_warning = dashboard.get("risk_warning")
        if isinstance(risk_warning, str) and risk_warning.strip():
            lines.append(f"**风险提示**: {risk_warning.strip()}")

        dashboard_block = dashboard.get("dashboard")
        if not isinstance(dashboard_block, dict):
            dashboard_block = {}
        battle_plan = dashboard_block.get("battle_plan")
        if not isinstance(battle_plan, dict):
            battle_plan = {}
        sniper = battle_plan.get("sniper_points")
        if isinstance(sniper, dict):
            price_parts = []
            for key in ("ideal_buy", "secondary_buy", "stop_loss", "take_profit"):
                value = AskCommand._format_sniper_value(sniper.get(key))
                if value:
                    price_parts.append(f"{key}={value}")
            if price_parts:
                lines.append("**关键点位**: " + " | ".join(price_parts))

        return "\n\n".join(lines) if lines else raw_content[:800]

    def _build_portfolio_section(
        self,
        config,
        codes: List[str],
        results: Dict[str, Dict[str, Any]],
        timeout_s: Optional[float] = None,
    ) -> str:
        """Generate a portfolio-level overlay for multi-stock ask results."""
        if len(results) < 2:
            return ""

        if timeout_s is not None and timeout_s <= 0:
            logger.info("[AskCommand] Skip portfolio overlay because no timeout budget remains")
            return ""

        def _render_overlay() -> str:
            from src.agent.agents.portfolio_agent import PortfolioAgent
            from src.agent.protocols import AgentContext
            from src.agent.factory import get_tool_registry
            from src.agent.llm_adapter import LLMToolAdapter

            stock_opinions: Dict[str, Dict[str, Any]] = {}
            risk_flags: List[Dict[str, str]] = []
            stock_list: List[str] = []
            for code in codes:
                item = results.get(code)
                if not item:
                    continue
                stock_list.append(code)
                stock_opinions[code] = {
                    "signal": item.get("signal", "unknown"),
                    "confidence": item.get("confidence", 0.5),
                    "summary": item.get("summary", ""),
                    "stock_name": item.get("stock_name", code),
                }
                risk_flags.extend(item.get("risk_flags", []))

            ctx = AgentContext(query=f"Portfolio overlay for {', '.join(stock_list)}")
            ctx.data["stock_opinions"] = stock_opinions
            ctx.data["stock_list"] = stock_list
            ctx.risk_flags.extend(risk_flags[:10])

            agent = PortfolioAgent(
                tool_registry=get_tool_registry(),
                llm_adapter=LLMToolAdapter(config),
            )
            stage_result = agent.run(ctx)
            if not stage_result.success:
                return ""

            assessment = ctx.data.get("portfolio_assessment")
            if not isinstance(assessment, dict):
                return ""

            lines = ["## 组合视角", ""]
            summary = assessment.get("summary")
            if isinstance(summary, str) and summary.strip():
                lines.append(summary.strip())
                lines.append("")

            risk_score = assessment.get("portfolio_risk_score")
            if risk_score is not None:
                lines.append(f"- 组合风险分: {risk_score}")
            sector_warnings = assessment.get("sector_warnings") or []
            if sector_warnings:
                lines.append(f"- 行业集中: {'；'.join(str(x) for x in sector_warnings[:3])}")
            correlation_warnings = assessment.get("correlation_warnings") or []
            if correlation_warnings:
                lines.append(f"- 相关性风险: {'；'.join(str(x) for x in correlation_warnings[:3])}")
            rebalance = assessment.get("rebalance_suggestions") or []
            if rebalance:
                lines.append(f"- 调仓建议: {'；'.join(str(x) for x in rebalance[:3])}")
            positions = assessment.get("positions") or []
            if positions:
                position_parts = []
                for position in positions[:5]:
                    if not isinstance(position, dict):
                        continue
                    code = position.get("code")
                    weight = position.get("suggested_weight")
                    signal = position.get("signal")
                    if code and weight is not None:
                        try:
                            weight_text = f"{float(weight):.0%}"
                        except (TypeError, ValueError):
                            weight_text = str(weight)
                        suffix = f" ({signal})" if signal else ""
                        position_parts.append(f"{code}: {weight_text}{suffix}")
                if position_parts:
                    lines.append(f"- 建议仓位: {'；'.join(position_parts)}")

            return "\n".join(lines)

        if timeout_s is None:
            try:
                return _render_overlay()
            except Exception as exc:
                logger.warning("[AskCommand] Portfolio overlay failed: %s", exc)
                return ""

        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

        pool = ThreadPoolExecutor(max_workers=1)
        future = pool.submit(_render_overlay)
        try:
            return future.result(timeout=timeout_s)
        except FutureTimeoutError:
            logger.warning("[AskCommand] Portfolio overlay timed out after %.2fs", timeout_s)
            return ""
        except Exception as exc:
            logger.warning("[AskCommand] Portfolio overlay failed: %s", exc)
            return ""
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

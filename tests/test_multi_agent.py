# -*- coding: utf-8 -*-
"""
Tests for the multi-agent architecture modules.

Covers:
- _extract_stock_code: Chinese boundary, HK, US, common word filtering
- AgentContext / AgentOpinion / StageResult protocol basics
- AgentOrchestrator: pipeline execution, mode selection, error handling
- StrategyRouter: regime detection, manual mode, user override
- StrategyAggregator: weighted consensus, empty input
- PortfolioAgent.post_process: JSON parsing via try_parse_json
"""

import json
import sys
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Keep test runnable when optional LLM deps are missing
try:
    import litellm  # noqa: F401
except ModuleNotFoundError:
    sys.modules["litellm"] = MagicMock()

from src.agent.orchestrator import _extract_stock_code, _COMMON_WORDS
from src.agent.protocols import (
    AgentContext,
    AgentOpinion,
    AgentRunStats,
    Signal,
    StageResult,
    StageStatus,
)


# ============================================================
# _extract_stock_code
# ============================================================

class TestExtractStockCode(unittest.TestCase):
    """Validate stock code extraction from free text."""

    # --- A-share ---

    def test_a_share_plain(self):
        self.assertEqual(_extract_stock_code("600519"), "600519")

    def test_a_share_chinese_prefix(self):
        """Critical: Chinese char + digits must still match (no \\b)."""
        self.assertEqual(_extract_stock_code("分析600519"), "600519")

    def test_a_share_chinese_suffix(self):
        self.assertEqual(_extract_stock_code("600519怎么样"), "600519")

    def test_a_share_in_sentence(self):
        self.assertEqual(_extract_stock_code("请帮我看看600519的走势"), "600519")

    def test_a_share_with_prefix_0(self):
        self.assertEqual(_extract_stock_code("分析000858"), "000858")

    def test_a_share_with_prefix_3(self):
        self.assertEqual(_extract_stock_code("分析300750"), "300750")

    def test_a_share_not_match_7_digits(self):
        """Should not match 7-digit number."""
        self.assertEqual(_extract_stock_code("1234567"), "")

    def test_a_share_embedded_in_longer_number(self):
        """Should not extract from within a longer number."""
        self.assertEqual(_extract_stock_code("86006005190001"), "")

    # --- HK ---

    def test_hk_lowercase(self):
        self.assertEqual(_extract_stock_code("look at hk00700"), "HK00700")

    def test_hk_uppercase(self):
        self.assertEqual(_extract_stock_code("HK00700 analysis"), "HK00700")

    def test_hk_chinese(self):
        self.assertEqual(_extract_stock_code("分析hk00700"), "HK00700")

    def test_hk_not_match_alpha_prefix(self):
        """Letters before 'hk' should not prevent match."""
        # "xhk00700" has alpha before hk, lookbehind should block
        self.assertNotEqual(_extract_stock_code("xhk00700"), "HK00700")

    # --- US ---

    def test_us_ticker(self):
        self.assertEqual(_extract_stock_code("analyze AAPL"), "AAPL")

    def test_us_ticker_in_chinese(self):
        self.assertEqual(_extract_stock_code("看看TSLA"), "TSLA")

    def test_us_ticker_5_chars(self):
        self.assertEqual(_extract_stock_code("check GOOGL"), "GOOGL")

    # --- Common word filtering ---

    def test_common_word_buy(self):
        self.assertEqual(_extract_stock_code("should I BUY"), "")

    def test_common_word_sell(self):
        self.assertEqual(_extract_stock_code("should I SELL"), "")

    def test_common_word_hold(self):
        self.assertEqual(_extract_stock_code("should I HOLD"), "")

    def test_common_word_etf(self):
        self.assertEqual(_extract_stock_code("what about ETF"), "")

    def test_common_word_rsi(self):
        self.assertEqual(_extract_stock_code("RSI is high"), "")

    def test_common_word_macd(self):
        self.assertEqual(_extract_stock_code("check MACD"), "")

    def test_common_word_stock(self):
        self.assertEqual(_extract_stock_code("good STOCK pick"), "")

    def test_common_word_trend(self):
        self.assertEqual(_extract_stock_code("the TREND is up"), "")

    # --- Priority: A-share > HK > US ---

    def test_a_share_takes_priority_over_us(self):
        """When both A-share code and US ticker appear, A-share wins."""
        self.assertEqual(_extract_stock_code("600519 vs AAPL"), "600519")

    # --- Empty / irrelevant ---

    def test_empty_string(self):
        self.assertEqual(_extract_stock_code(""), "")

    def test_no_code(self):
        self.assertEqual(_extract_stock_code("hello world"), "")

    def test_single_char_uppercase(self):
        """Single uppercase letter should not match."""
        self.assertEqual(_extract_stock_code("I think"), "")

    def test_lowercase_not_us_ticker(self):
        """Lowercase letters should not match US regex."""
        self.assertEqual(_extract_stock_code("analyze aapl"), "")

    def test_common_words_set_completeness(self):
        """Ensure critical finance terms are in _COMMON_WORDS."""
        expected_in_set = {"BUY", "SELL", "HOLD", "ETF", "IPO", "RSI", "MACD", "STOCK", "TREND"}
        self.assertTrue(expected_in_set.issubset(_COMMON_WORDS))


# ============================================================
# Protocol dataclasses
# ============================================================

class TestAgentContext(unittest.TestCase):
    """Test AgentContext helpers."""

    def test_add_opinion(self):
        ctx = AgentContext(query="test", stock_code="600519")
        op = AgentOpinion(agent_name="tech", signal="buy", confidence=0.8)
        ctx.add_opinion(op)
        self.assertEqual(len(ctx.opinions), 1)
        self.assertGreater(op.timestamp, 0)

    def test_add_risk_flag(self):
        ctx = AgentContext()
        ctx.add_risk_flag("insider", "major sell-down", severity="high")
        self.assertTrue(ctx.has_risk_flags)
        self.assertEqual(ctx.risk_flags[0]["severity"], "high")

    def test_set_get_data(self):
        ctx = AgentContext()
        ctx.set_data("foo", {"bar": 1})
        self.assertEqual(ctx.get_data("foo"), {"bar": 1})
        self.assertIsNone(ctx.get_data("missing"))
        self.assertEqual(ctx.get_data("missing", "default"), "default")


class TestAgentOpinion(unittest.TestCase):
    """Test AgentOpinion clamping and signal parsing."""

    def test_confidence_clamp_high(self):
        op = AgentOpinion(confidence=1.5)
        self.assertEqual(op.confidence, 1.0)

    def test_confidence_clamp_low(self):
        op = AgentOpinion(confidence=-0.3)
        self.assertEqual(op.confidence, 0.0)

    def test_signal_enum_valid(self):
        op = AgentOpinion(signal="buy")
        self.assertEqual(op.signal_enum, Signal.BUY)

    def test_signal_enum_invalid(self):
        op = AgentOpinion(signal="maybe")
        self.assertIsNone(op.signal_enum)


class TestAgentRunStats(unittest.TestCase):
    """Test AgentRunStats aggregation."""

    def test_record_stage(self):
        stats = AgentRunStats()
        r1 = StageResult(
            stage_name="tech", status=StageStatus.COMPLETED,
            tokens_used=100, tool_calls_count=3, duration_s=1.2,
        )
        r2 = StageResult(
            stage_name="intel", status=StageStatus.FAILED,
            tokens_used=50, tool_calls_count=1, duration_s=0.8,
        )
        stats.record_stage(r1)
        stats.record_stage(r2)

        self.assertEqual(stats.total_stages, 2)
        self.assertEqual(stats.completed_stages, 1)
        self.assertEqual(stats.failed_stages, 1)
        self.assertEqual(stats.total_tokens, 150)
        self.assertEqual(stats.total_tool_calls, 4)

    def test_to_dict(self):
        stats = AgentRunStats()
        d = stats.to_dict()
        self.assertIn("total_stages", d)
        self.assertIn("models_used", d)


# ============================================================
# StrategyRouter
# ============================================================

class TestStrategyRouter(unittest.TestCase):
    """Test StrategyRouter selection logic."""

    def test_user_requested_strategies_take_priority(self):
        from src.agent.strategies.router import StrategyRouter
        router = StrategyRouter()
        ctx = AgentContext(query="test")
        ctx.meta["strategies_requested"] = ["chan_theory", "wave_theory"]
        result = router.select_strategies(ctx)
        self.assertEqual(result, ["chan_theory", "wave_theory"])

    def test_user_requested_capped_at_max(self):
        from src.agent.strategies.router import StrategyRouter
        router = StrategyRouter()
        ctx = AgentContext()
        ctx.meta["strategies_requested"] = ["a", "b", "c", "d", "e"]
        result = router.select_strategies(ctx, max_count=2)
        self.assertEqual(len(result), 2)

    @patch("src.agent.strategies.router.StrategyRouter._get_routing_mode", return_value="manual")
    def test_manual_mode_uses_defaults(self, _mock):
        from src.agent.strategies.router import StrategyRouter, _DEFAULT_STRATEGIES
        router = StrategyRouter()
        ctx = AgentContext()
        result = router.select_strategies(ctx)
        self.assertEqual(result, _DEFAULT_STRATEGIES[:3])

    def test_detect_regime_bullish(self):
        from src.agent.strategies.router import StrategyRouter
        router = StrategyRouter()
        ctx = AgentContext()
        ctx.add_opinion(AgentOpinion(
            agent_name="technical",
            signal="buy",
            confidence=0.8,
            raw_data={"ma_alignment": "bullish", "trend_score": 80, "volume_status": "normal"},
        ))
        regime = router._detect_regime(ctx)
        self.assertEqual(regime, "trending_up")

    def test_detect_regime_bearish(self):
        from src.agent.strategies.router import StrategyRouter
        router = StrategyRouter()
        ctx = AgentContext()
        ctx.add_opinion(AgentOpinion(
            agent_name="technical",
            signal="sell",
            confidence=0.7,
            raw_data={"ma_alignment": "bearish", "trend_score": 20, "volume_status": "light"},
        ))
        regime = router._detect_regime(ctx)
        self.assertEqual(regime, "trending_down")

    def test_detect_regime_none_without_technical(self):
        from src.agent.strategies.router import StrategyRouter
        router = StrategyRouter()
        ctx = AgentContext()
        regime = router._detect_regime(ctx)
        self.assertIsNone(regime)


# ============================================================
# StrategyAggregator
# ============================================================

class TestStrategyAggregator(unittest.TestCase):
    """Test StrategyAggregator consensus logic."""

    def test_no_strategy_opinions_returns_none(self):
        from src.agent.strategies.aggregator import StrategyAggregator
        agg = StrategyAggregator()
        ctx = AgentContext()
        ctx.add_opinion(AgentOpinion(agent_name="technical", signal="buy", confidence=0.8))
        result = agg.aggregate(ctx)
        self.assertIsNone(result)

    def test_single_strategy_consensus(self):
        from src.agent.strategies.aggregator import StrategyAggregator
        agg = StrategyAggregator()
        ctx = AgentContext()
        ctx.add_opinion(AgentOpinion(agent_name="strategy_bull_trend", signal="buy", confidence=0.7))
        result = agg.aggregate(ctx)
        self.assertIsNotNone(result)
        self.assertEqual(result.agent_name, "strategy_consensus")
        self.assertEqual(result.signal, "buy")

    def test_mixed_signals_produce_hold(self):
        from src.agent.strategies.aggregator import StrategyAggregator
        agg = StrategyAggregator()
        ctx = AgentContext()
        ctx.add_opinion(AgentOpinion(agent_name="strategy_a", signal="buy", confidence=0.6))
        ctx.add_opinion(AgentOpinion(agent_name="strategy_b", signal="sell", confidence=0.6))
        result = agg.aggregate(ctx)
        self.assertIsNotNone(result)
        # Average of buy(4) + sell(2) = 3.0, which maps to "hold"
        self.assertEqual(result.signal, "hold")


# ============================================================
# PortfolioAgent.post_process
# ============================================================

class TestPortfolioAgentPostProcess(unittest.TestCase):
    """Test PortfolioAgent.post_process uses try_parse_json correctly."""

    def _make_agent(self):
        from src.agent.agents.portfolio_agent import PortfolioAgent
        mock_registry = MagicMock()
        mock_adapter = MagicMock()
        return PortfolioAgent(tool_registry=mock_registry, llm_adapter=mock_adapter)

    def test_parse_plain_json(self):
        agent = self._make_agent()
        ctx = AgentContext()
        data = {"portfolio_risk_score": 3, "summary": "Looks good"}
        op = agent.post_process(ctx, json.dumps(data))
        self.assertIsNotNone(op)
        self.assertEqual(op.signal, "buy")
        self.assertEqual(ctx.data.get("portfolio_assessment"), data)

    def test_parse_markdown_json(self):
        agent = self._make_agent()
        ctx = AgentContext()
        data = {"portfolio_risk_score": 8, "summary": "High risk"}
        raw = f"Here is the analysis:\n```json\n{json.dumps(data)}\n```"
        op = agent.post_process(ctx, raw)
        self.assertIsNotNone(op)
        self.assertEqual(op.signal, "sell")

    def test_parse_failure_returns_hold(self):
        agent = self._make_agent()
        ctx = AgentContext()
        op = agent.post_process(ctx, "This is not JSON at all")
        self.assertIsNotNone(op)
        self.assertEqual(op.signal, "hold")
        self.assertAlmostEqual(op.confidence, 0.3)


# ============================================================
# AgentOrchestrator (with mocked sub-agents)
# ============================================================

class TestOrchestratorModes(unittest.TestCase):
    """Test that _build_agent_chain returns the right agents for each mode."""

    def _make_orchestrator(self, mode="standard"):
        from src.agent.orchestrator import AgentOrchestrator
        mock_registry = MagicMock()
        mock_adapter = MagicMock()
        return AgentOrchestrator(
            tool_registry=mock_registry,
            llm_adapter=mock_adapter,
            mode=mode,
        )

    def test_quick_mode(self):
        orch = self._make_orchestrator("quick")
        ctx = AgentContext(query="test", stock_code="600519")
        chain = orch._build_agent_chain(ctx)
        names = [a.agent_name for a in chain]
        self.assertEqual(names, ["technical", "decision"])

    def test_standard_mode(self):
        orch = self._make_orchestrator("standard")
        ctx = AgentContext(query="test", stock_code="600519")
        chain = orch._build_agent_chain(ctx)
        names = [a.agent_name for a in chain]
        self.assertEqual(names, ["technical", "intel", "decision"])

    def test_full_mode(self):
        orch = self._make_orchestrator("full")
        ctx = AgentContext(query="test", stock_code="600519")
        chain = orch._build_agent_chain(ctx)
        names = [a.agent_name for a in chain]
        self.assertEqual(names, ["technical", "intel", "risk", "decision"])

    def test_invalid_mode_falls_back_to_standard(self):
        orch = self._make_orchestrator("nonsense")
        self.assertEqual(orch.mode, "standard")

    def test_chain_agents_inherit_orchestrator_max_steps(self):
        orch = self._make_orchestrator("full")
        orch.max_steps = 9
        ctx = AgentContext(query="test", stock_code="600519")
        chain = orch._build_agent_chain(ctx)
        self.assertTrue(chain)
        self.assertTrue(all(agent.max_steps == 9 for agent in chain))

    def test_build_context_from_dict(self):
        orch = self._make_orchestrator()
        ctx = orch._build_context(
            "Analyze 600519",
            context={"stock_code": "600519", "stock_name": "贵州茅台", "strategies": ["bull_trend"]},
        )
        self.assertEqual(ctx.stock_code, "600519")
        self.assertEqual(ctx.stock_name, "贵州茅台")
        self.assertEqual(ctx.meta["strategies_requested"], ["bull_trend"])

    def test_build_context_extracts_code_from_query(self):
        orch = self._make_orchestrator()
        ctx = orch._build_context("分析600519的走势")
        self.assertEqual(ctx.stock_code, "600519")

    def test_fallback_summary(self):
        orch = self._make_orchestrator()
        ctx = AgentContext(query="test", stock_code="600519", stock_name="贵州茅台")
        ctx.add_opinion(AgentOpinion(agent_name="tech", signal="buy", confidence=0.8, reasoning="Strong trend"))
        ctx.add_risk_flag("insider", "Minor sell-down", severity="low")
        summary = orch._fallback_summary(ctx)
        self.assertIn("600519", summary)
        self.assertIn("Strong trend", summary)
        self.assertIn("Minor sell-down", summary)


class TestOrchestratorExecution(unittest.TestCase):
    """Test main orchestrator execution paths."""

    @staticmethod
    def _make_orchestrator(config=None):
        from src.agent.orchestrator import AgentOrchestrator
        return AgentOrchestrator(
            tool_registry=MagicMock(),
            llm_adapter=MagicMock(),
            config=config,
        )

    @staticmethod
    def _stage_result(name, status=StageStatus.COMPLETED, error=None, raw_text="ok"):
        result = StageResult(stage_name=name, status=status, error=error)
        result.meta["raw_text"] = raw_text
        result.meta["models_used"] = ["test/model"]
        return result

    def test_execute_pipeline_stops_on_critical_failure(self):
        orch = self._make_orchestrator()
        technical = MagicMock(agent_name="technical")
        technical.run.return_value = self._stage_result("technical", StageStatus.FAILED, error="boom")

        with patch.object(orch, "_build_agent_chain", return_value=[technical]):
            result = orch._execute_pipeline(AgentContext(query="test"))

        self.assertFalse(result.success)
        self.assertIn("technical", result.error)
        self.assertEqual(result.total_tokens, 0)

    def test_execute_pipeline_degrades_on_intel_failure(self):
        orch = self._make_orchestrator()
        ctx = AgentContext(query="test", stock_code="600519")
        ctx.add_opinion(AgentOpinion(agent_name="technical", signal="buy", confidence=0.8, reasoning="Strong trend"))

        intel = MagicMock(agent_name="intel")
        intel.run.return_value = self._stage_result("intel", StageStatus.FAILED, error="news down")
        decision = MagicMock(agent_name="decision")
        decision.run.return_value = self._stage_result("decision")

        with patch.object(orch, "_build_agent_chain", return_value=[intel, decision]):
            result = orch._execute_pipeline(ctx, parse_dashboard=False)

        self.assertTrue(result.success)
        self.assertIn("Analysis Summary", result.content)

    def test_execute_pipeline_times_out_after_stage(self):
        orch = self._make_orchestrator(config=SimpleNamespace(agent_orchestrator_timeout_s=1))
        agent = MagicMock(agent_name="technical")
        agent.run.return_value = self._stage_result("technical")

        with patch.object(orch, "_build_agent_chain", return_value=[agent]):
            with patch("src.agent.orchestrator.time.time", side_effect=[0.0, 0.1, 1.2, 1.2, 1.2, 1.2]):
                result = orch._execute_pipeline(AgentContext(query="test"))

        self.assertFalse(result.success)
        self.assertIn("timed out", result.error)

    def test_run_wraps_orchestrator_result(self):
        from src.agent.orchestrator import OrchestratorResult

        orch = self._make_orchestrator()
        fake_result = OrchestratorResult(success=True, content="done", total_steps=2, total_tokens=11, model="x")
        with patch.object(orch, "_execute_pipeline", return_value=fake_result):
            result = orch.run("Analyze 600519")

        self.assertTrue(result.success)
        self.assertEqual(result.content, "done")
        self.assertEqual(result.total_steps, 2)

    def test_chat_persists_user_and_assistant_messages(self):
        from src.agent.orchestrator import OrchestratorResult

        orch = self._make_orchestrator()
        fake_result = OrchestratorResult(success=True, content="assistant reply")

        with patch.object(orch, "_execute_pipeline", return_value=fake_result):
            with patch("src.agent.conversation.conversation_manager.add_message") as add_message:
                result = orch.chat("hello", "session-1")

        self.assertTrue(result.success)
        self.assertEqual(add_message.call_count, 2)
        add_message.assert_any_call("session-1", "user", "hello")
        add_message.assert_any_call("session-1", "assistant", "assistant reply")

    def test_chat_persists_failure_message(self):
        from src.agent.orchestrator import OrchestratorResult

        orch = self._make_orchestrator()
        fake_result = OrchestratorResult(success=False, error="boom")

        with patch.object(orch, "_execute_pipeline", return_value=fake_result):
            with patch("src.agent.conversation.conversation_manager.add_message") as add_message:
                result = orch.chat("hello", "session-2")

        self.assertFalse(result.success)
        add_message.assert_any_call("session-2", "assistant", "[分析失败] boom")


# ============================================================
# EventMonitor serialization
# ============================================================

class TestEventMonitor(unittest.TestCase):
    """Test EventMonitor serialize/deserialize round-trip."""

    def test_round_trip(self):
        from src.agent.events import EventMonitor, PriceAlert, VolumeAlert
        monitor = EventMonitor()
        monitor.add_alert(PriceAlert(stock_code="600519", direction="above", price=1800.0))
        monitor.add_alert(VolumeAlert(stock_code="000858", multiplier=3.0))

        data = monitor.to_dict_list()
        self.assertEqual(len(data), 2)

        restored = EventMonitor.from_dict_list(data)
        self.assertEqual(len(restored.rules), 2)
        self.assertEqual(restored.rules[0].stock_code, "600519")
        self.assertEqual(restored.rules[1].stock_code, "000858")

    def test_remove_expired(self):
        import time
        from src.agent.events import EventMonitor, PriceAlert
        monitor = EventMonitor()
        alert = PriceAlert(stock_code="600519", direction="above", price=1800.0, ttl_hours=0.0)
        alert.created_at = time.time() - 3600  # 1 hour ago
        monitor.rules.append(alert)
        removed = monitor.remove_expired()
        self.assertEqual(removed, 1)
        self.assertEqual(len(monitor.rules), 0)


class TestEventMonitorAsync(unittest.IsolatedAsyncioTestCase):
    """Test async EventMonitor checks offload blocking fetches."""

    async def test_check_price_uses_to_thread_and_triggers(self):
        from src.agent.events import EventMonitor, PriceAlert

        monitor = EventMonitor()
        rule = PriceAlert(stock_code="600519", direction="above", price=1800.0)
        quote = SimpleNamespace(price=1810.0)

        with patch("src.agent.events.asyncio.to_thread", new=AsyncMock(return_value=quote)) as to_thread:
            triggered = await monitor._check_price(rule)

        self.assertIsNotNone(triggered)
        self.assertEqual(triggered.rule.stock_code, "600519")
        to_thread.assert_awaited_once()

    async def test_check_volume_safe_when_fetch_returns_none(self):
        """_check_volume must not crash when get_daily_data returns None."""
        from src.agent.events import EventMonitor, VolumeAlert

        monitor = EventMonitor()
        rule = VolumeAlert(stock_code="600519", multiplier=2.0)

        with patch("src.agent.events.asyncio.to_thread", new=AsyncMock(return_value=None)):
            result = await monitor._check_volume(rule)

        self.assertIsNone(result)

    async def test_check_all_async_callback(self):
        """on_trigger callbacks should be properly awaited if coroutine."""
        from src.agent.events import EventMonitor, PriceAlert

        monitor = EventMonitor()
        rule = PriceAlert(stock_code="600519", direction="above", price=1800.0)
        monitor.add_alert(rule)

        callback_values = []
        async_cb = AsyncMock(side_effect=lambda alert: callback_values.append(alert.rule.stock_code))
        monitor.on_trigger(async_cb)

        quote = SimpleNamespace(price=1810.0)
        with patch("src.agent.events.asyncio.to_thread", new=AsyncMock(return_value=quote)):
            triggered = await monitor.check_all()

        self.assertEqual(len(triggered), 1)
        async_cb.assert_awaited_once()


# ============================================================
# AgentMemory
# ============================================================

class TestAgentMemory(unittest.TestCase):
    """Test AgentMemory disabled mode."""

    def test_disabled_returns_neutral(self):
        from src.agent.memory import AgentMemory
        mem = AgentMemory(enabled=False)
        cal = mem.get_calibration("technical")
        self.assertFalse(cal.calibrated)
        self.assertAlmostEqual(cal.calibration_factor, 1.0)

    def test_disabled_weights_all_equal(self):
        from src.agent.memory import AgentMemory
        mem = AgentMemory(enabled=False)
        weights = mem.compute_strategy_weights(["a", "b", "c"])
        self.assertEqual(weights, {"a": 1.0, "b": 1.0, "c": 1.0})

    def test_calibrate_confidence_passthrough_when_disabled(self):
        from src.agent.memory import AgentMemory
        mem = AgentMemory(enabled=False)
        self.assertAlmostEqual(mem.calibrate_confidence("tech", 0.75), 0.75)


if __name__ == '__main__':
    unittest.main()

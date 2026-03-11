# -*- coding: utf-8 -*-
"""Tests for async-friendly bot dispatcher execution."""

import sys
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

# Keep tests runnable when optional deps are missing.
try:
    import litellm  # noqa: F401
except ModuleNotFoundError:
    sys.modules["litellm"] = MagicMock()

from bot.commands.base import BotCommand
from bot.dispatcher import CommandDispatcher
from bot.models import BotMessage, BotResponse, ChatType


class DummyCommand(BotCommand):
    @property
    def name(self) -> str:
        return "dummy"

    @property
    def aliases(self):
        return []

    @property
    def description(self) -> str:
        return "dummy command"

    @property
    def usage(self) -> str:
        return "/dummy"

    def execute(self, message: BotMessage, args: list[str]) -> BotResponse:
        return BotResponse.text_response("dummy-ok")


def _make_message(content: str, mentioned: bool = False) -> BotMessage:
    return BotMessage(
        platform="feishu",
        message_id="m1",
        user_id="u1",
        user_name="tester",
        chat_id="c1",
        chat_type=ChatType.PRIVATE,
        content=content,
        raw_content=content,
        mentioned=mentioned,
        timestamp=datetime.now(),
    )


class TestBotCommandAsync(unittest.IsolatedAsyncioTestCase):
    async def test_execute_async_uses_to_thread(self):
        cmd = DummyCommand()
        message = _make_message("/dummy")

        with patch(
            "bot.commands.base.asyncio.to_thread",
            new=AsyncMock(return_value=BotResponse.text_response("ok")),
        ) as to_thread:
            result = await cmd.execute_async(message, [])

        self.assertEqual(result.text, "ok")
        to_thread.assert_awaited_once()


class TestCommandDispatcherAsync(unittest.IsolatedAsyncioTestCase):
    async def test_dispatch_async_awaits_command_execute_async(self):
        dispatcher = CommandDispatcher()
        command = DummyCommand()
        command.execute_async = AsyncMock(return_value=BotResponse.text_response("async-ok"))
        dispatcher.register(command)

        result = await dispatcher.dispatch_async(_make_message("/dummy"))

        self.assertEqual(result.text, "async-ok")
        command.execute_async.assert_awaited_once()

    async def test_parse_intent_via_llm_offloads_to_thread(self):
        fake_response = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content='{"intent":"analysis","codes":["600519"],"strategy":null}')
                )
            ]
        )
        config = SimpleNamespace(litellm_model="gemini/test-model")

        with patch(
            "bot.dispatcher.asyncio.to_thread",
            new=AsyncMock(return_value=fake_response),
        ) as to_thread:
            result = await CommandDispatcher._parse_intent_via_llm("分析600519", config)

        self.assertEqual(result["intent"], "analysis")
        self.assertEqual(result["codes"], ["600519"])
        to_thread.assert_awaited_once()

    async def test_try_nl_routing_uses_async_command_execution(self):
        dispatcher = CommandDispatcher()
        ask_command = DummyCommand()
        ask_command.execute_async = AsyncMock(return_value=BotResponse.text_response("ask-ok"))
        dispatcher.register(ask_command)
        dispatcher._commands["ask"] = ask_command

        config = SimpleNamespace(
            agent_nl_routing=True,
            is_agent_available=lambda: True,
            litellm_model="gemini/test-model",
        )

        with patch("src.config.get_config", return_value=config):
            with patch.object(dispatcher, "_parse_intent_via_llm", new=AsyncMock(return_value={
                "intent": "analysis",
                "codes": ["600519"],
                "strategy": "缠论",
            })):
                result = await dispatcher._try_nl_routing(_make_message("帮我分析600519", mentioned=True))

        self.assertIsNotNone(result)
        self.assertEqual(result.text, "ask-ok")
        ask_command.execute_async.assert_awaited_once()


class TestCommandDispatcherSyncCompatibility(unittest.TestCase):
    def test_dispatch_sync_wrapper_still_works(self):
        dispatcher = CommandDispatcher()
        dispatcher.register(DummyCommand())

        result = dispatcher.dispatch(_make_message("/dummy"))

        self.assertEqual(result.text, "dummy-ok")


class TestHandleWebhookAsync(unittest.IsolatedAsyncioTestCase):
    """Test the async webhook handler path."""

    async def test_handle_webhook_async_dispatches_via_async(self):
        from bot.handler import handle_webhook_async

        fake_platform = MagicMock()
        fake_message = _make_message("/dummy")
        fake_platform.handle_webhook.return_value = (fake_message, None)
        fake_platform.format_response.return_value = MagicMock(text="ok-response")

        fake_config = MagicMock()
        fake_config.bot_enabled = True

        with patch("src.config.get_config", return_value=fake_config), \
             patch("bot.handler.get_platform", return_value=fake_platform), \
             patch("bot.handler.get_dispatcher") as mock_get_disp:
            mock_dispatcher = MagicMock()
            mock_dispatcher.dispatch_async = AsyncMock(return_value=BotResponse.text_response("async-resp"))
            mock_get_disp.return_value = mock_dispatcher

            await handle_webhook_async("feishu", {}, b'{}')

        mock_dispatcher.dispatch_async.assert_awaited_once()

    async def test_handle_webhook_async_returns_success_when_bot_disabled(self):
        from bot.handler import handle_webhook_async

        fake_config = MagicMock()
        fake_config.bot_enabled = False

        with patch("src.config.get_config", return_value=fake_config):
            result = await handle_webhook_async("feishu", {}, b'{}')

        # WebhookResponse.success() returns status_code 200
        self.assertEqual(result.status_code, 200)


class TestResearchAgentFilteredRegistry(unittest.TestCase):
    """Test that ResearchAgent._filtered_registry delegates to BaseAgent's implementation."""

    def test_filtered_registry_delegates_to_base(self):
        from src.agent.research import ResearchAgent
        from src.agent.tools.registry import ToolRegistry

        registry = ToolRegistry()
        # Register a fake tool that matches one of ResearchAgent.tool_names
        fake_tool = MagicMock()
        fake_tool.name = "search_stock_news"
        registry.register(fake_tool)

        llm_adapter = MagicMock()
        agent = ResearchAgent(tool_registry=registry, llm_adapter=llm_adapter)

        filtered = agent._filtered_registry()
        self.assertIsInstance(filtered, ToolRegistry)
        # Should contain the tool we registered
        self.assertIsNotNone(filtered.get("search_stock_news"))


if __name__ == "__main__":
    unittest.main()

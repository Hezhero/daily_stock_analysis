# -*- coding: utf-8 -*-
"""
History command — display and manage conversation history for the current session.

Usage:
    /history           -> Show recent messages in the current session
    /history clear     -> Clear all messages in the current session
"""

import logging
from typing import List, Optional

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse
from src.config import get_config

logger = logging.getLogger(__name__)

_MAX_DISPLAY_MESSAGES = 10
_MAX_CONTENT_PREVIEW = 120


class HistoryCommand(BotCommand):
    """
    History command handler — view or clear the current chat session history.

    Usage:
        /history           -> Show recent messages in the current session
        /history clear     -> Clear the current session's conversation history
    """

    @property
    def name(self) -> str:
        return "history"

    @property
    def aliases(self) -> List[str]:
        return ["历史", "会话历史"]

    @property
    def description(self) -> str:
        return "查看或清除当前会话的对话历史"

    @property
    def usage(self) -> str:
        return "/history [clear]"

    def execute(self, message: BotMessage, args: List[str]) -> BotResponse:
        """Execute the history command."""
        config = get_config()

        if not config.agent_mode:
            return BotResponse.text_response(
                "⚠️ Agent 模式未开启，对话历史功能不可用。\n"
                "请在配置中设置 `AGENT_MODE=true`。"
            )

        session_id = f"{message.platform}_{message.user_id}"
        subcommand = args[0].lower() if args else ""

        if subcommand in ("clear", "清除", "清空"):
            return self._clear_history(session_id)

        return self._show_history(session_id)

    def _show_history(self, session_id: str) -> BotResponse:
        """Retrieve and format recent messages for the session."""
        try:
            from src.agent.conversation import conversation_manager

            messages = conversation_manager.get_history(session_id)
            if not messages:
                return BotResponse.text_response(
                    "📭 当前会话暂无历史记录。\n"
                    "使用 `/chat <问题>` 开始对话。"
                )

            # Show at most the last N messages
            recent = messages[-_MAX_DISPLAY_MESSAGES:]
            total = len(messages)

            lines = [
                f"📜 会话历史（最近 {len(recent)} 条，共 {total} 条）",
                "─" * 32,
            ]
            for i, msg in enumerate(recent, 1):
                role_label = "👤 用户" if msg["role"] == "user" else "🤖 助手"
                content = msg.get("content", "")
                preview = content[:_MAX_CONTENT_PREVIEW]
                if len(content) > _MAX_CONTENT_PREVIEW:
                    preview += "…"
                lines.append(f"{i}. {role_label}: {preview}")

            lines.append("─" * 32)
            lines.append("使用 `/history clear` 清除会话历史。")
            return BotResponse.text_response("\n".join(lines))

        except Exception as e:
            logger.error("[HistoryCommand] Failed to retrieve history: %s", e)
            return BotResponse.text_response(f"⚠️ 获取历史记录失败: {str(e)}")

    def _clear_history(self, session_id: str) -> BotResponse:
        """Clear the conversation history for the session."""
        try:
            from src.agent.conversation import conversation_manager
            from src.storage import get_db

            # Remove from in-memory manager
            conversation_manager.clear(session_id)

            # Remove persisted messages from database
            deleted = get_db().delete_conversation_session(session_id)

            logger.info(
                "[HistoryCommand] Cleared session %s (%d messages deleted)",
                session_id, deleted,
            )
            return BotResponse.text_response(
                f"🗑️ 会话历史已清除（共删除 {deleted} 条记录）。"
            )

        except Exception as e:
            logger.error("[HistoryCommand] Failed to clear history: %s", e)
            return BotResponse.text_response(f"⚠️ 清除历史记录失败: {str(e)}")

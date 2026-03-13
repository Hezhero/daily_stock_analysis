# -*- coding: utf-8 -*-
"""
Chat command for free-form conversation with the Agent.
"""

import logging
from typing import List, Optional

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse
from src.config import get_config

logger = logging.getLogger(__name__)


def _resolve_chat_session_id(message: BotMessage) -> str:
    """Prefer the legacy bot chat session id when prior history already exists."""
    legacy_session_id = f"{message.platform}_{message.user_id}"
    session_id = f"{legacy_session_id}:chat"

    try:
        from src.storage import get_db

        db = get_db()
        legacy_exists = db.conversation_session_exists(legacy_session_id)
        current_exists = db.conversation_session_exists(session_id)
        if legacy_exists and not current_exists:
            return legacy_session_id
    except Exception as exc:
        logger.debug("Chat session compatibility check failed: %s", exc)

    return session_id

class ChatCommand(BotCommand):
    """
    Chat command handler.
    
    Usage: /chat <message>
    Example: /chat 帮我分析一下茅台最近的走势
    """
    
    @property
    def name(self) -> str:
        return "chat"
        
    @property
    def description(self) -> str:
        return "与 AI 助手进行自由对话 (需开启 Agent 模式)"
        
    @property
    def usage(self) -> str:
        return "/chat <问题>"
        
    @property
    def aliases(self) -> list[str]:
        return ["c", "问"]

    def validate_args(self, args: List[str]) -> Optional[str]:
        """Require at least one argument (the question)."""
        if not args:
            return "请提供要询问的问题。\n用法: `/chat <问题>`\n示例: `/chat 帮我分析一下茅台最近的走势`"
        return None

    def execute(self, message: BotMessage, args: list[str]) -> BotResponse:
        """Execute the chat command."""
        config = get_config()
        
        if not config.is_agent_available():
            return BotResponse.text_response(
                "⚠️ Agent 模式不可用，无法使用对话功能。\n请配置 `LITELLM_MODEL` 或设置 `AGENT_MODE=true`。"
            )
            
        if not args:
            return BotResponse.text_response(
                "⚠️ 请提供要询问的问题。\n用法: `/chat <问题>`\n示例: `/chat 帮我分析一下茅台最近的走势`"
            )
            
        user_message = " ".join(args)
        session_id = _resolve_chat_session_id(message)
        
        try:
            from src.agent.factory import build_agent_executor
            executor = build_agent_executor(config)
            result = executor.chat(message=user_message, session_id=session_id)
            
            if result.success:
                return BotResponse.text_response(result.content)
            else:
                return BotResponse.text_response(f"⚠️ 对话失败: {result.error}")
                
        except Exception as e:
            logger.error(f"Chat command failed: {e}")
            logger.exception("Chat error details:")
            return BotResponse.text_response(f"⚠️ 对话执行出错: {str(e)}")

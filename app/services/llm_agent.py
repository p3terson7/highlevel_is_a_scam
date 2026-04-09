from __future__ import annotations

"""Compatibility shim.

All imports from app.services.llm_agent resolve to the current tool-driven agent.
"""

from app.services.agent_v3 import (  # noqa: F401
    AgentAction,
    AgentResponse,
    LLMAgent,
    LLMAgentV3,
    LLMProvider,
    OpenAIProvider,
    ToolCall,
    build_llm_agent,
)

__all__ = [
    "AgentAction",
    "AgentResponse",
    "LLMAgent",
    "LLMAgentV3",
    "LLMProvider",
    "OpenAIProvider",
    "ToolCall",
    "build_llm_agent",
]

"""Compatibility shim: LLMAgent now routes to agent_v2."""

from app.services.agent_v2 import (
    AgentAction,
    AgentResponse,
    LLMAgent,
    LLMAgentV2,
    LLMProvider,
    OpenAIProvider,
    build_llm_agent,
)

__all__ = [
    "AgentAction",
    "AgentResponse",
    "LLMAgent",
    "LLMAgentV2",
    "LLMProvider",
    "OpenAIProvider",
    "build_llm_agent",
]

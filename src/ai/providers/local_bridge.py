"""
WebSocket bridge provider — routes AI requests to a local agent
running on the user's PC (e.g., connected to Ollama).
"""
import asyncio
import json
import logging
from src.ai.base import LLMProvider, AIMessage, AIResponse

logger = logging.getLogger(__name__)

# Registry of connected local agents: {user_id: websocket}
_local_agents: dict[int, object] = {}


def register_agent(user_id: int, websocket):
    _local_agents[user_id] = websocket
    logger.info(f"Local AI agent registered for user {user_id}")


def unregister_agent(user_id: int):
    _local_agents.pop(user_id, None)
    logger.info(f"Local AI agent disconnected for user {user_id}")


def is_agent_connected(user_id: int) -> bool:
    return user_id in _local_agents


class LocalBridgeProvider(LLMProvider):
    """Forwards AI requests to the user's local machine via WebSocket."""

    provider_name = "local_bridge"

    def __init__(self, user_id: int, default_model: str = "mistral:latest"):
        self.user_id = user_id
        self.default_model = default_model

    async def chat(self, messages: list[AIMessage], model: str | None = None) -> AIResponse:
        ws = _local_agents.get(self.user_id)
        if ws is None:
            raise ConnectionError(f"No local AI agent connected for user {self.user_id}")

        request = {
            "type": "chat",
            "model": model or self.default_model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
        }
        await ws.send_json(request)

        # Wait for response with timeout
        try:
            raw = await asyncio.wait_for(ws.receive_text(), timeout=120.0)
            data = json.loads(raw)
        except asyncio.TimeoutError:
            raise ConnectionError("Local AI agent timed out")

        if data.get("error"):
            raise RuntimeError(f"Local AI error: {data['error']}")

        return AIResponse(
            content=data["content"],
            model=data.get("model", model or self.default_model),
            provider=self.provider_name,
            tokens_used=data.get("tokens_used", 0),
        )

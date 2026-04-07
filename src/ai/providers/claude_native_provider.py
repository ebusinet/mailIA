import logging
import json as _json
import httpx
from src.ai.base import LLMProvider, AIMessage, AIResponse

logger = logging.getLogger(__name__)


class ClaudeNativeProvider(LLMProvider):
    """Provider that uses the Claude Code Native API (/claude/query)."""

    provider_name = "claude-native"

    def __init__(self, api_key: str, default_model: str = "claude-code-full",
                 base_url: str = "https://ia.expert-presta.com",
                 mcp_servers: dict | None = None):
        self.api_key = api_key
        self.default_model = default_model
        # Strip /claude/query suffix if user included it in the endpoint
        url = base_url.rstrip("/")
        if url.endswith("/claude/query"):
            url = url[: -len("/claude/query")]
        self.base_url = url
        self.mcp_servers = mcp_servers

    def _build_request(self, messages: list[AIMessage], model: str | None = None,
                       stream: bool = False) -> dict:
        system_prompt = ""
        user_prompt = ""
        for msg in messages:
            if msg.role == "system":
                system_prompt = msg.content
            elif msg.role == "user":
                user_prompt = msg.content

        body = {
            "prompt": user_prompt,
            "stream": stream,
            "options": {
                "model": model or self.default_model,
                "maxTurns": 100,
                "permissionMode": "dontAsk",
            },
        }
        if system_prompt:
            body["options"]["systemPrompt"] = system_prompt
        if self.mcp_servers:
            body["options"]["mcpServers"] = self.mcp_servers
            body["options"]["allowedTools"] = [
                f"mcp__{name}__*" for name in self.mcp_servers.keys()
            ]
        return body

    async def chat(self, messages: list[AIMessage], model: str | None = None) -> AIResponse:
        url = f"{self.base_url}/claude/query"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        body = self._build_request(messages, model, stream=False)

        async with httpx.AsyncClient(timeout=httpx.Timeout(600.0)) as client:
            resp = await client.post(url, json=body, headers=headers)
            if resp.status_code != 200:
                raise RuntimeError(f"Claude Native API error {resp.status_code}: {resp.text}")
            data = resp.json()

        tokens = 0
        usage = data.get("usage", {})
        if usage:
            tokens = usage.get("inputTokens", 0) + usage.get("outputTokens", 0)

        return AIResponse(
            content=data.get("result", ""),
            model=model or self.default_model,
            provider=self.provider_name,
            tokens_used=tokens,
        )

    async def stream_chat(self, messages: list[AIMessage], model: str | None = None):
        url = f"{self.base_url}/claude/query"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        body = self._build_request(messages, model, stream=True)

        yielded = False
        has_streamed_text = False

        async with httpx.AsyncClient(timeout=httpx.Timeout(600.0)) as client:
            async with client.stream("POST", url, json=body, headers=headers) as resp:
                if resp.status_code != 200:
                    error_text = ""
                    async for chunk in resp.aiter_text():
                        error_text += chunk
                    raise RuntimeError(f"Claude Native API error {resp.status_code}: {error_text}")

                buffer = ""
                last_assistant_text = ""
                async for raw in resp.aiter_text():
                    buffer += raw
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.strip()
                        if not line or not line.startswith("data: "):
                            continue
                        payload = line[6:]
                        if payload == "[DONE]":
                            return

                        try:
                            data = _json.loads(payload)
                        except _json.JSONDecodeError:
                            continue

                        msg_type = data.get("type", "")

                        if msg_type == "assistant":
                            message = data.get("message", {})
                            for block in message.get("content", []):
                                if block.get("type") == "text":
                                    # Text comes via stream_event deltas;
                                    # assistant events duplicate it as full snapshot.
                                    # Only use assistant text if no stream deltas seen.
                                    if not has_streamed_text:
                                        text = block.get("text", "")
                                        if text:
                                            if text.startswith(last_assistant_text):
                                                new_text = text[len(last_assistant_text):]
                                            else:
                                                new_text = text
                                            last_assistant_text = text
                                            if new_text:
                                                yielded = True
                                                yield new_text
                                elif block.get("type") == "tool_use":
                                    tool_name = block.get("name", "unknown")
                                    yield f"[[tool:{tool_name}]]"

                        elif msg_type == "stream_event":
                            event = data.get("event", {})
                            event_type = event.get("type", "")
                            if event_type == "content_block_delta":
                                delta = event.get("delta", {})
                                if delta.get("type") == "text_delta":
                                    text = delta.get("text", "")
                                    if text:
                                        has_streamed_text = True
                                        yielded = True
                                        yield text

                        elif msg_type == "result":
                            cost = data.get("total_cost_usd", 0)
                            turns = data.get("num_turns", 0)
                            if cost or turns:
                                logger.info(f"[NATIVE-RESULT] turns={turns}, cost=${cost:.4f}")
                            # Only yield result text if we got nothing from streaming
                            if not has_streamed_text:
                                result_text = data.get("result", "")
                                if result_text:
                                    yield result_text
                                    yielded = True

        if not yielded:
            try:
                response = await self.chat(messages, model)
                if response.content:
                    yield response.content
            except Exception:
                logger.error("Non-streaming fallback also failed", exc_info=True)
                raise

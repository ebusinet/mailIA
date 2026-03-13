import logging
import openai
from src.ai.base import LLMProvider, EmbeddingProvider, AIMessage, AIResponse

logger = logging.getLogger(__name__)


class OpenAIProvider(LLMProvider):
    provider_name = "openai"

    def __init__(self, api_key: str, default_model: str = "gpt-4o",
                 base_url: str | None = None,
                 mcp_servers: dict | None = None):
        kwargs = {"api_key": api_key}
        if base_url:
            kwargs["base_url"] = base_url
            kwargs["timeout"] = 300.0  # MCP tool calls can take a while
        self.client = openai.AsyncOpenAI(**kwargs)
        self.default_model = default_model
        self.mcp_servers = mcp_servers

    async def chat(self, messages: list[AIMessage], model: str | None = None) -> AIResponse:
        extra = {}
        if self.mcp_servers:
            extra["extra_body"] = {"mcp_servers": self.mcp_servers}
        response = await self.client.chat.completions.create(
            model=model or self.default_model,
            messages=[{"role": m.role, "content": m.content} for m in messages],
            max_tokens=4096,
            **extra,
        )
        choice = response.choices[0]
        return AIResponse(
            content=choice.message.content or "",
            model=response.model,
            provider=self.provider_name,
            tokens_used=(response.usage.prompt_tokens + response.usage.completion_tokens)
            if response.usage else 0,
        )

    async def stream_chat(self, messages: list[AIMessage], model: str | None = None):
        extra = {}
        if self.mcp_servers:
            extra["extra_body"] = {"mcp_servers": self.mcp_servers}
        yielded = False

        # Use raw SSE parsing if talking to our proxy (to capture tool_activity events)
        if self.client.base_url and "expert-presta" in str(self.client.base_url):
            async for chunk_text in self._raw_stream(messages, model, extra):
                yielded = True
                yield chunk_text
        else:
            try:
                stream = await self.client.chat.completions.create(
                    model=model or self.default_model,
                    messages=[{"role": m.role, "content": m.content} for m in messages],
                    max_tokens=4096,
                    stream=True,
                    **extra,
                )
                async for chunk in stream:
                    if chunk.choices and chunk.choices[0].delta.content:
                        yielded = True
                        yield chunk.choices[0].delta.content
            except openai.APIError as e:
                logger.warning(f"Streaming failed ({e}), falling back to non-streaming")
        if not yielded:
            try:
                response = await self.chat(messages, model)
                if response.content:
                    yield response.content
            except Exception:
                logger.error("Non-streaming fallback also failed", exc_info=True)
                raise

    async def _raw_stream(self, messages: list[AIMessage], model: str | None, extra: dict):
        """Raw SSE stream that captures tool_activity events from our proxy."""
        import httpx
        import json as _json

        url = str(self.client.base_url).rstrip("/") + "/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.client.api_key:
            headers["Authorization"] = f"Bearer {self.client.api_key}"

        body = {
            "model": model or self.default_model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "max_tokens": 16384,
            "stream": True,
        }
        if extra.get("extra_body"):
            body.update(extra["extra_body"])

        # Log conversation context sent to proxy
        msg_summary = [(m["role"], len(m["content"])) for m in body["messages"]]
        total_chars = sum(l for _, l in msg_summary)
        logger.info(f"[PROXY-REQ] Sending {len(body['messages'])} messages ({total_chars} chars) to {url}: {msg_summary}")

        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0)) as client:
            async with client.stream("POST", url, json=body, headers=headers) as resp:
                if resp.status_code != 200:
                    error_text = ""
                    async for chunk in resp.aiter_text():
                        error_text += chunk
                    raise openai.APIError(f"Proxy error {resp.status_code}: {error_text}", response=None, body=None)

                buffer = ""
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
                            # Tool activity event from proxy
                            if "tool_activity" in data:
                                tool = data["tool_activity"]
                                yield f"[[tool:{tool.get('name', 'unknown')}]]"
                                continue
                            # Normal text delta
                            choices = data.get("choices", [])
                            if choices:
                                content = choices[0].get("delta", {}).get("content")
                                if content:
                                    yield content
                        except _json.JSONDecodeError:
                            continue


class OpenAIEmbeddingProvider(EmbeddingProvider):
    def __init__(self, api_key: str, model: str = "text-embedding-3-small"):
        self.client = openai.AsyncOpenAI(api_key=api_key)
        self.model = model
        self._dimension = 1536

    async def embed(self, texts: list[str]) -> list[list[float]]:
        response = await self.client.embeddings.create(model=self.model, input=texts)
        return [item.embedding for item in response.data]

    @property
    def dimension(self) -> int:
        return self._dimension

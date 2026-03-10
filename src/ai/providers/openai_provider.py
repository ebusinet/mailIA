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
            # Streaming produced no text — fallback to non-streaming
            try:
                response = await self.chat(messages, model)
                if response.content:
                    yield response.content
            except Exception:
                logger.error("Non-streaming fallback also failed", exc_info=True)
                raise


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

import openai
from src.ai.base import LLMProvider, EmbeddingProvider, AIMessage, AIResponse


class OpenAIProvider(LLMProvider):
    provider_name = "openai"

    def __init__(self, api_key: str, default_model: str = "gpt-4o"):
        self.client = openai.AsyncOpenAI(api_key=api_key)
        self.default_model = default_model

    async def chat(self, messages: list[AIMessage], model: str | None = None) -> AIResponse:
        response = await self.client.chat.completions.create(
            model=model or self.default_model,
            messages=[{"role": m.role, "content": m.content} for m in messages],
            max_tokens=2048,
        )
        choice = response.choices[0]
        return AIResponse(
            content=choice.message.content or "",
            model=response.model,
            provider=self.provider_name,
            tokens_used=(response.usage.prompt_tokens + response.usage.completion_tokens)
            if response.usage else 0,
        )


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

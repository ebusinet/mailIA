import httpx
from src.ai.base import LLMProvider, EmbeddingProvider, AIMessage, AIResponse


class OllamaProvider(LLMProvider):
    provider_name = "ollama"

    def __init__(self, endpoint: str = "http://localhost:11434", default_model: str = "mistral:latest"):
        self.endpoint = endpoint.rstrip("/")
        self.default_model = default_model

    async def chat(self, messages: list[AIMessage], model: str | None = None) -> AIResponse:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{self.endpoint}/api/chat",
                json={
                    "model": model or self.default_model,
                    "messages": [{"role": m.role, "content": m.content} for m in messages],
                    "stream": False,
                },
            )
            response.raise_for_status()
            data = response.json()

        return AIResponse(
            content=data["message"]["content"],
            model=data.get("model", model or self.default_model),
            provider=self.provider_name,
            tokens_used=data.get("eval_count", 0),
        )


class OllamaEmbeddingProvider(EmbeddingProvider):
    def __init__(self, endpoint: str = "http://localhost:11434", model: str = "nomic-embed-text"):
        self.endpoint = endpoint.rstrip("/")
        self.model = model
        self._dimension = 768  # nomic-embed-text default

    async def embed(self, texts: list[str]) -> list[list[float]]:
        results = []
        async with httpx.AsyncClient(timeout=60.0) as client:
            for text in texts:
                response = await client.post(
                    f"{self.endpoint}/api/embeddings",
                    json={"model": self.model, "prompt": text},
                )
                response.raise_for_status()
                results.append(response.json()["embedding"])
        return results

    @property
    def dimension(self) -> int:
        return self._dimension

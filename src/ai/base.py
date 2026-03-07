from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class AIMessage:
    role: str  # system, user, assistant
    content: str


@dataclass
class AIResponse:
    content: str
    model: str
    provider: str
    tokens_used: int = 0


class LLMProvider(ABC):
    """Base class for all LLM providers."""

    provider_name: str = "base"

    @abstractmethod
    async def chat(self, messages: list[AIMessage], model: str | None = None) -> AIResponse:
        """Send a chat completion request."""

    async def classify(self, text: str, categories: list[str], context: str = "") -> str:
        """Classify text into one of the given categories."""
        cats = ", ".join(categories)
        messages = [
            AIMessage("system", f"Classify the following email into exactly one category: {cats}. "
                      f"Reply with ONLY the category name, nothing else. {context}"),
            AIMessage("user", text),
        ]
        response = await self.chat(messages)
        return response.content.strip()

    async def summarize(self, text: str, max_sentences: int = 3) -> str:
        """Summarize text."""
        messages = [
            AIMessage("system", f"Summarize the following email in {max_sentences} sentences max. "
                      "Be concise and focus on action items. Reply in the same language as the email."),
            AIMessage("user", text),
        ]
        response = await self.chat(messages)
        return response.content.strip()

    async def extract_info(self, text: str, fields: list[str]) -> dict:
        """Extract structured information from text."""
        fields_str = ", ".join(fields)
        messages = [
            AIMessage("system", f"Extract the following fields from this email: {fields_str}. "
                      "Reply as JSON with those exact keys. Use null if not found."),
            AIMessage("user", text),
        ]
        response = await self.chat(messages)
        import json
        try:
            return json.loads(response.content)
        except json.JSONDecodeError:
            return {f: None for f in fields}

    async def evaluate_rule(self, email_text: str, rule_condition: str) -> bool:
        """Ask the AI if an email matches a rule condition."""
        messages = [
            AIMessage("system",
                      "You are an email sorting assistant. Evaluate whether the email matches "
                      "the given condition. Reply ONLY 'yes' or 'no'."),
            AIMessage("user", f"Condition: {rule_condition}\n\nEmail:\n{email_text}"),
        ]
        response = await self.chat(messages)
        return response.content.strip().lower().startswith("y")


class EmbeddingProvider(ABC):
    """Base class for embedding providers."""

    @abstractmethod
    async def embed(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for a list of texts."""

    @property
    @abstractmethod
    def dimension(self) -> int:
        """Return the embedding dimension."""

import anthropic
from src.ai.base import LLMProvider, AIMessage, AIResponse


class ClaudeProvider(LLMProvider):
    provider_name = "claude"

    def __init__(self, api_key: str, default_model: str = "claude-sonnet-4-20250514"):
        self.client = anthropic.AsyncAnthropic(api_key=api_key)
        self.default_model = default_model

    async def chat(self, messages: list[AIMessage], model: str | None = None) -> AIResponse:
        system_msg = ""
        chat_messages = []
        for msg in messages:
            if msg.role == "system":
                system_msg = msg.content
            else:
                chat_messages.append({"role": msg.role, "content": msg.content})

        response = await self.client.messages.create(
            model=model or self.default_model,
            max_tokens=2048,
            system=system_msg or anthropic.NOT_GIVEN,
            messages=chat_messages,
        )
        return AIResponse(
            content=response.content[0].text,
            model=response.model,
            provider=self.provider_name,
            tokens_used=response.usage.input_tokens + response.usage.output_tokens,
        )

"""
AI Router — resolves which AI provider to use for a given user + context.
"""
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.ai.base import LLMProvider
from src.ai.providers.claude import ClaudeProvider
from src.ai.providers.ollama import OllamaProvider
from src.ai.providers.openai_provider import OpenAIProvider
from src.ai.providers.local_bridge import LocalBridgeProvider, is_agent_connected
from src.db.models import AIProvider, SystemSetting, User
from src.security import decrypt_value
from src.config import get_settings

logger = logging.getLogger(__name__)


async def get_llm_for_user(
    db: AsyncSession,
    user: User,
    provider_id: int | None = None,
) -> LLMProvider:
    """Resolve the LLM provider for a user.

    Priority:
    1. Explicit provider_id (from a rule or request)
    2. User's default provider
    3. System fallback (Claude API if key set)
    """
    settings = get_settings()

    query = select(AIProvider).where(AIProvider.user_id == user.id)
    if provider_id:
        query = query.where(AIProvider.id == provider_id)
    else:
        query = query.where(AIProvider.is_default.is_(True))
    result = await db.execute(query)
    provider_config = result.scalar_one_or_none()

    if provider_config is None:
        # Fallback: try user's first provider, then system default
        result = await db.execute(
            select(AIProvider).where(AIProvider.user_id == user.id).limit(1)
        )
        provider_config = result.scalar_one_or_none()

    if provider_config:
        return _build_provider(provider_config, user.id)

    # Last resort: system-level keys (DB settings first, then .env)
    api_key = await _get_system_api_key(db, "anthropic_api_key") or settings.anthropic_api_key
    if api_key:
        model = await _get_system_setting(db, "default_ai_model") or "claude-sonnet-4-20250514"
        return ClaudeProvider(api_key=api_key, default_model=model)

    api_key = await _get_system_api_key(db, "openai_api_key") or settings.openai_api_key
    if api_key:
        model = await _get_system_setting(db, "default_ai_model") or "gpt-4o"
        return OpenAIProvider(api_key=api_key, default_model=model)

    raise RuntimeError(f"No AI provider configured for user {user.id} and no system fallback")


def _build_provider(config: AIProvider, user_id: int) -> LLMProvider:
    ptype = config.provider_type.lower()

    if ptype == "ollama":
        return OllamaProvider(
            endpoint=config.endpoint or "http://localhost:11434",
            default_model=config.model,
        )

    if ptype == "local" or config.is_local:
        if is_agent_connected(user_id):
            return LocalBridgeProvider(user_id=user_id, default_model=config.model)
        # Fallback: check if there's a non-local provider
        logger.warning(f"Local agent not connected for user {user_id}, using direct Ollama endpoint")
        if config.endpoint:
            return OllamaProvider(endpoint=config.endpoint, default_model=config.model)
        raise ConnectionError(f"Local AI agent not connected for user {user_id}")

    api_key = decrypt_value(config.api_key_encrypted) if config.api_key_encrypted else ""

    if ptype == "claude":
        return ClaudeProvider(api_key=api_key, default_model=config.model)

    if ptype == "openai":
        return OpenAIProvider(api_key=api_key, default_model=config.model)

    raise ValueError(f"Unknown provider type: {ptype}")


async def _get_system_setting(db, key: str) -> str | None:
    result = await db.execute(select(SystemSetting).where(SystemSetting.key == key))
    s = result.scalar_one_or_none()
    if s and s.value:
        return decrypt_value(s.value) if s.is_encrypted else s.value
    return None


async def _get_system_api_key(db, key: str) -> str | None:
    return await _get_system_setting(db, key)

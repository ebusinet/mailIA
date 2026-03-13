"""
AI Router — resolves which AI provider to use for a given user + context.
"""
import logging
import time
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.ai.base import LLMProvider
from src.ai.providers.claude import ClaudeProvider
from src.ai.providers.claude_native_provider import ClaudeNativeProvider
from src.ai.providers.ollama import OllamaProvider
from src.ai.providers.openai_provider import OpenAIProvider
from src.ai.providers.local_bridge import LocalBridgeProvider, is_agent_connected
from src.db.models import AIProvider, SystemSetting, User
from src.security import decrypt_value
from src.config import get_settings

logger = logging.getLogger(__name__)

_provider_cache = {}
_CACHE_TTL = 300  # 5 minutes


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
    cache_key = (user.id, provider_id)
    cached = _provider_cache.get(cache_key)
    if cached and time.time() - cached[1] < _CACHE_TTL:
        return cached[0]

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
        provider = _build_provider(provider_config, user.id)
        _provider_cache[cache_key] = (provider, time.time())
        return provider

    # Last resort: system-level keys (DB settings first, then .env)
    api_key = await _get_system_api_key(db, "anthropic_api_key") or settings.anthropic_api_key
    if api_key:
        model = await _get_system_setting(db, "default_ai_model") or "claude-sonnet-4-20250514"
        provider = ClaudeProvider(api_key=api_key, default_model=model)
        _provider_cache[cache_key] = (provider, time.time())
        return provider

    api_key = await _get_system_api_key(db, "openai_api_key") or settings.openai_api_key
    if api_key:
        model = await _get_system_setting(db, "default_ai_model") or "gpt-4o"
        provider = OpenAIProvider(api_key=api_key, default_model=model)
        _provider_cache[cache_key] = (provider, time.time())
        return provider

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
        mcp_servers = None
        if config.endpoint:
            settings = get_settings()
            if settings.mcp_sse_url:
                mcp_servers = {"mailia": {"type": "sse", "url": settings.mcp_sse_url}}
        return OpenAIProvider(api_key=api_key, default_model=config.model,
                              base_url=config.endpoint, mcp_servers=mcp_servers)

    if ptype == "claude-native":
        mcp_servers = None
        settings = get_settings()
        if settings.mcp_sse_url:
            mcp_servers = {"mailia": {"type": "sse", "url": settings.mcp_sse_url}}
        return ClaudeNativeProvider(
            api_key=api_key,
            default_model=config.model,
            base_url=config.endpoint or "https://ia.expert-presta.com",
            mcp_servers=mcp_servers,
        )

    raise ValueError(f"Unknown provider type: {ptype}")


def _build_provider_from_params(
    provider_type: str,
    endpoint: str | None,
    api_key: str | None,
    model: str,
) -> LLMProvider:
    """Build a provider from raw parameters (for testing without saving)."""
    ptype = provider_type.lower()
    key = api_key or ""
    settings = get_settings()

    if ptype == "ollama":
        return OllamaProvider(endpoint=endpoint or "http://localhost:11434", default_model=model)

    if ptype == "claude":
        return ClaudeProvider(api_key=key, default_model=model)

    if ptype == "openai":
        mcp_servers = None
        if endpoint and settings.mcp_sse_url:
            mcp_servers = {"mailia": {"type": "sse", "url": settings.mcp_sse_url}}
        return OpenAIProvider(api_key=key, default_model=model, base_url=endpoint, mcp_servers=mcp_servers)

    if ptype == "claude-native":
        mcp_servers = None
        if settings.mcp_sse_url:
            mcp_servers = {"mailia": {"type": "sse", "url": settings.mcp_sse_url}}
        return ClaudeNativeProvider(
            api_key=key, default_model=model,
            base_url=endpoint or "https://ia.expert-presta.com",
            mcp_servers=mcp_servers,
        )

    raise ValueError(f"Unknown provider type: {ptype}")


async def _get_system_setting(db, key: str) -> str | None:
    result = await db.execute(select(SystemSetting).where(SystemSetting.key == key))
    s = result.scalar_one_or_none()
    if s and s.value:
        return decrypt_value(s.value) if s.is_encrypted else s.value
    return None


async def _get_system_api_key(db, key: str) -> str | None:
    return await _get_system_setting(db, key)

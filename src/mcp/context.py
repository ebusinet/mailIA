"""
Shared async context for MCP tools — DB session, ES client, IMAP connections.
Reused across tool calls within the same MCP server process.
"""
import logging
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from elasticsearch import AsyncElasticsearch

from src.config import get_settings
from src.db.models import User, MailAccount, AIProvider, AIRule, ProcessingLog
from src.imap.manager import IMAPManager, IMAPConfig
from src.security import decrypt_value

logger = logging.getLogger(__name__)

_engine = None
_session_factory = None


def _get_engine():
    global _engine, _session_factory
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(settings.database_url, echo=False, pool_size=5, max_overflow=5)
        _session_factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    return _engine, _session_factory


@asynccontextmanager
async def get_db():
    _, factory = _get_engine()
    async with factory() as session:
        yield session


async def get_es() -> AsyncElasticsearch:
    return AsyncElasticsearch(get_settings().elasticsearch_url)


def get_imap(account: MailAccount) -> IMAPManager:
    config = IMAPConfig(
        host=account.imap_host,
        port=account.imap_port,
        ssl=account.imap_ssl,
        user=account.imap_user,
        password=decrypt_value(account.imap_password_encrypted),
    )
    return IMAPManager(config)

"""
Shared helpers for MCP tools — account resolution, user lookup, etc.
"""
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.models import User, MailAccount


async def get_user(db: AsyncSession, user_id: int) -> User:
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise ValueError(f"User {user_id} not found")
    return user


async def get_account(db: AsyncSession, user_id: int, account_id: int) -> MailAccount:
    result = await db.execute(
        select(MailAccount).where(MailAccount.id == account_id, MailAccount.user_id == user_id)
    )
    account = result.scalar_one_or_none()
    if not account:
        raise ValueError(f"Account {account_id} not found for user {user_id}")
    return account


async def list_user_accounts(db: AsyncSession, user_id: int) -> list[MailAccount]:
    result = await db.execute(
        select(MailAccount).where(MailAccount.user_id == user_id).order_by(MailAccount.id)
    )
    return list(result.scalars().all())


async def resolve_account(db: AsyncSession, user_id: int, account_id: int | None) -> MailAccount:
    """Resolve account_id or pick the first one if None."""
    if account_id:
        return await get_account(db, user_id, account_id)
    accounts = await list_user_accounts(db, user_id)
    if not accounts:
        raise ValueError("No mail accounts configured")
    return accounts[0]

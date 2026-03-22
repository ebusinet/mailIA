"""
Celery tasks — IMAP sync, indexing, AI rule processing.
"""
import asyncio
import imaplib
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from src.worker.app import app
from src.db.models import MailAccount, AIRule, ClassicRule, ProcessingLog, User, SpamWhitelist, SpamBlacklist
from src.imap.manager import IMAPManager, IMAPConfig, _decode_imap_utf7
from src.search.indexer import get_es_client, ensure_index, index_email, bulk_index_emails
from src.rules.parser import parse_rules_markdown
from src.rules.engine import evaluate_rules, EmailContext
from src.ai.router import get_llm_for_user
from src.security import decrypt_value
from src.config import get_settings

logger = logging.getLogger(__name__)


def _run_async(coro):
    """Run an async function in a sync Celery task."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@asynccontextmanager
async def _worker_session():
    """Create a fresh async engine + session per task invocation.

    Each Celery task runs in its own event loop, so we need a fresh
    asyncpg connection pool to avoid 'another operation in progress' errors.
    """
    engine = create_async_engine(get_settings().database_url, echo=False, pool_size=2, max_overflow=0)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with factory() as session:
            yield session
    finally:
        await engine.dispose()


@app.task(name="src.worker.tasks.sync_all_accounts")
def sync_all_accounts():
    """Periodic task: sync all active mail accounts."""
    _run_async(_sync_all_accounts())


async def _sync_all_accounts():
    async with _worker_session() as db:
        result = await db.execute(
            select(MailAccount).where(MailAccount.sync_enabled.is_(True))
        )
        accounts = result.scalars().all()
        for account in accounts:
            try:
                await _sync_account(db, account)
            except Exception as e:
                logger.error(f"Sync failed for account {account.id} ({account.name}): {e}")


@app.task(name="src.worker.tasks.sync_account")
def sync_account(account_id: int):
    """Sync a specific mail account."""
    _run_async(_sync_account_by_id(account_id))


async def _sync_account_by_id(account_id: int):
    async with _worker_session() as db:
        result = await db.execute(select(MailAccount).where(MailAccount.id == account_id))
        account = result.scalar_one_or_none()
        if account:
            await _sync_account(db, account)


async def _sync_account(db, account: MailAccount):
    """Sync a single mail account: fetch new emails, index, apply rules."""
    config = IMAPConfig(
        host=account.imap_host,
        port=account.imap_port,
        ssl=account.imap_ssl,
        user=account.imap_user,
        password=decrypt_value(account.imap_password_encrypted),
    )

    es = await get_es_client()
    await ensure_index(es, account.user_id)

    # Get user's active AI rules
    rules_result = await db.execute(
        select(AIRule).where(
            AIRule.user_id == account.user_id,
            AIRule.is_active.is_(True),
        ).order_by(AIRule.priority)
    )
    ai_rules = rules_result.scalars().all()
    parsed_rules = []
    for ar in ai_rules:
        parsed_rules.extend(parse_rules_markdown(ar.rules_markdown))

    # Get LLM if rules need AI
    llm = None
    needs_ai = any(r.condition.needs_ai for r in parsed_rules)
    if needs_ai:
        try:
            user_result = await db.execute(select(User).where(User.id == account.user_id))
            user = user_result.scalar_one()
            llm = await get_llm_for_user(db, user)
        except Exception as e:
            logger.warning(f"Could not get LLM for user {account.user_id}: {e}")

    # Get user's active classic rules
    classic_result = await db.execute(
        select(ClassicRule).where(
            ClassicRule.user_id == account.user_id,
            ClassicRule.is_active.is_(True),
        ).order_by(ClassicRule.priority)
    )
    classic_rules = classic_result.scalars().all()

    # Load spam whitelist/blacklist for classic rule evaluation
    wl_set = set()
    bl_set = set()
    if classic_rules:
        wl_result = await db.execute(
            select(SpamWhitelist).where(SpamWhitelist.account_id == account.id)
        )
        wl_set = {e.value for e in wl_result.scalars().all()}
        bl_result = await db.execute(
            select(SpamBlacklist).where(SpamBlacklist.account_id == account.id)
        )
        bl_set = {e.value for e in bl_result.scalars().all()}

    # Per-folder UID tracking (replaces UNKEYWORD which OVH doesn't support)
    sync_state = dict(account.sync_state or {})

    imap = IMAPManager(config)
    imap.connect()

    try:
        folder_entries = imap.list_folders()

        BATCH_SIZE = 50  # emails fetched from IMAP per batch
        MAX_PER_FOLDER = 2000  # max emails per folder per sync cycle

        for entry in folder_entries:
            imap_folder = entry["name"] if isinstance(entry, dict) else entry
            display_folder = entry.get("display_name", imap_folder) if isinstance(entry, dict) else imap_folder
            folder = imap_folder  # Use IMAP UTF-7 name for IMAP operations
            try:
                last_uid = sync_state.get(folder)
                uids = imap.get_uids(folder, since_uid=last_uid)
                if not uids:
                    continue

                # Process oldest first so sync_state advances progressively
                uids_to_process = uids[:MAX_PER_FOLDER]
                logger.info(f"Account {account.name}: {len(uids)} pending in {folder}, processing {len(uids_to_process)}")

                # Process in batches for bulk ES indexing
                for batch_start in range(0, len(uids_to_process), BATCH_SIZE):
                    batch_uids = uids_to_process[batch_start:batch_start + BATCH_SIZE]
                    batch_contexts = []

                    for uid in batch_uids:
                        try:
                            email_ctx = imap.fetch_email(uid, folder)
                            if email_ctx:
                                email_ctx.folder = display_folder  # Store UTF-8 in ES
                                batch_contexts.append(email_ctx)
                        except Exception as e:
                            logger.error(f"Error fetching UID {uid} in {folder}: {e}")

                    # Bulk index the batch
                    if batch_contexts:
                        try:
                            await bulk_index_emails(es, account.user_id, account.id, batch_contexts)
                        except Exception as e:
                            logger.error(f"Bulk index error in {folder}: {e}")
                            # Fallback to individual indexing
                            for ctx in batch_contexts:
                                try:
                                    await index_email(es, account.user_id, account.id, ctx)
                                except Exception:
                                    pass

                    # Apply AI rules
                    if parsed_rules:
                        for ctx in batch_contexts:
                            try:
                                matches = await evaluate_rules(ctx, parsed_rules, llm)
                                for match in matches:
                                    await _execute_actions(imap, db, account, ctx, match)
                            except Exception as e:
                                logger.error(f"Rule error for UID {ctx.uid} in {folder}: {e}")

                    # Apply classic rules
                    if classic_rules:
                        try:
                            from src.api.routes.rules import apply_classic_rules_on_sync
                            result = apply_classic_rules_on_sync(
                                imap, folder, batch_uids, classic_rules,
                                whitelist=wl_set, blacklist=bl_set,
                            )
                            if result["matched"]:
                                logger.info(
                                    f"Classic rules: {result['matched']} matched, "
                                    f"{result['actions_taken']} actions in {folder}"
                                )
                        except Exception as e:
                            logger.error(f"Classic rule error in {folder}: {e}")

                    # Save progress after each batch
                    sync_state[folder] = batch_uids[-1]

            except (ConnectionError, OSError, imaplib.IMAP4.abort) as e:
                logger.warning(f"IMAP connection lost at folder {folder}: {e}")
                try:
                    imap.disconnect()
                    imap.connect()
                except Exception:
                    logger.error(f"Failed to reconnect IMAP for account {account.name}")
                    break

            except Exception as e:
                logger.error(f"Error processing folder {folder}: {e}")

    finally:
        imap.disconnect()

    # Persist sync state
    account.sync_state = sync_state
    account.last_sync_at = datetime.utcnow()
    await db.commit()
    await es.close()


async def _execute_actions(imap, db, account, email_ctx: EmailContext, match):
    """Execute the actions from a matched rule."""
    rule = match.rule

    for action in rule.actions:
        try:
            if action.action_type == "move":
                imap.move_email(email_ctx.uid, email_ctx.folder, action.target)
            elif action.action_type == "flag":
                imap.flag_email(email_ctx.uid, email_ctx.folder, action.target)
            elif action.action_type == "mark_read":
                imap.mark_read(email_ctx.uid, email_ctx.folder)

            # Log the action
            log = ProcessingLog(
                user_id=account.user_id,
                mail_account_id=account.id,
                mail_uid=email_ctx.uid,
                folder=email_ctx.folder,
                action_taken=action.action_type,
                action_detail={"target": action.target, "rule": rule.name},
                ai_response=match.ai_explanation,
            )
            db.add(log)

        except Exception as e:
            logger.error(f"Failed to execute action {action.action_type} for UID {email_ctx.uid}: {e}")

    await db.commit()

    # Telegram notification
    if rule.notify:
        _send_notification.delay(account.user_id, email_ctx.subject, email_ctx.from_addr, rule.name)


@app.task(name="src.worker.tasks.send_notification")
def _send_notification(user_id: int, subject: str, from_addr: str, rule_name: str):
    """Send a Telegram notification (delegated to avoid import cycles)."""
    _run_async(_async_notify(user_id, subject, from_addr, rule_name))


async def _async_notify(user_id: int, subject: str, from_addr: str, rule_name: str):
    async with _worker_session() as db:
        result = await db.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        if user and user.telegram_chat_id:
            from src.telegram_bot.main import send_notification
            await send_notification(
                user.telegram_chat_id,
                f"[{rule_name}] {from_addr}\n{subject}",
            )

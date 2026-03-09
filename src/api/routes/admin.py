from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from src.db.session import get_db
from src.db.models import User, SystemSetting, MailAccount, ProcessingLog
from src.api.deps import get_current_admin
from src.security import encrypt_value, decrypt_value
from src.config import get_settings

router = APIRouter()

SENSITIVE_KEYS = {"anthropic_api_key", "openai_api_key", "telegram_bot_token"}

DEFAULT_SETTINGS = [
    ("anthropic_api_key", True, "Cle API Anthropic (Claude)"),
    ("openai_api_key", True, "Cle API OpenAI"),
    ("telegram_bot_token", True, "Token du bot Telegram"),
    ("default_ai_provider", False, "Provider IA par defaut (claude, openai)"),
    ("default_ai_model", False, "Modele IA par defaut"),
    ("app_name", False, "Nom de l'application"),
    ("max_users", False, "Nombre maximum d'utilisateurs (0 = illimite)"),
]


class SettingResponse(BaseModel):
    key: str
    value: str | None
    is_encrypted: bool
    description: str | None


class SettingUpdate(BaseModel):
    key: str
    value: str


class UserAdminResponse(BaseModel):
    id: int
    email: str
    username: str
    is_active: bool
    is_admin: bool
    created_at: str

    model_config = {"from_attributes": True}


class UserAdminUpdate(BaseModel):
    is_active: bool | None = None
    is_admin: bool | None = None


# --- Settings ---

@router.get("/settings", response_model=list[SettingResponse])
async def list_settings(
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(SystemSetting))
    settings = {s.key: s for s in result.scalars().all()}

    # Ensure all defaults exist
    out = []
    for key, encrypted, desc in DEFAULT_SETTINGS:
        if key in settings:
            s = settings[key]
            val = ""
            if s.value:
                if s.is_encrypted:
                    try:
                        decrypted = decrypt_value(s.value)
                        val = decrypted[:3] + "***" + decrypted[-3:] if len(decrypted) > 6 else "***"
                    except Exception:
                        val = "***"
                else:
                    val = s.value
            out.append(SettingResponse(key=s.key, value=val, is_encrypted=s.is_encrypted, description=s.description))
        else:
            out.append(SettingResponse(key=key, value="", is_encrypted=encrypted, description=desc))
    return out


@router.put("/settings")
async def update_settings(
    updates: list[SettingUpdate],
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    valid_keys = {k for k, _, _ in DEFAULT_SETTINGS}
    for upd in updates:
        if upd.key not in valid_keys:
            raise HTTPException(status_code=400, detail=f"Unknown setting: {upd.key}")

        result = await db.execute(select(SystemSetting).where(SystemSetting.key == upd.key))
        setting = result.scalar_one_or_none()

        is_enc = upd.key in SENSITIVE_KEYS
        stored_value = encrypt_value(upd.value) if is_enc and upd.value else upd.value

        if setting:
            setting.value = stored_value
            setting.is_encrypted = is_enc
        else:
            desc = next((d for k, _, d in DEFAULT_SETTINGS if k == upd.key), None)
            db.add(SystemSetting(key=upd.key, value=stored_value, is_encrypted=is_enc, description=desc))

    await db.commit()
    return {"status": "ok", "updated": len(updates)}


# --- Users ---

@router.get("/users", response_model=list[UserAdminResponse])
async def list_users(
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(User).order_by(User.id))
    users = result.scalars().all()
    return [UserAdminResponse(
        id=u.id, email=u.email, username=u.username,
        is_active=u.is_active, is_admin=u.is_admin,
        created_at=u.created_at.isoformat() if u.created_at else "",
    ) for u in users]


@router.put("/users/{user_id}")
async def update_user(
    user_id: int,
    req: UserAdminUpdate,
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent removing own admin
    if user.id == admin.id and req.is_admin is False:
        raise HTTPException(status_code=400, detail="Cannot remove your own admin rights")

    if req.is_active is not None:
        user.is_active = req.is_active
    if req.is_admin is not None:
        user.is_admin = req.is_admin

    await db.commit()
    return {"status": "ok"}


# --- System info ---

@router.get("/info")
async def system_info(
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    user_count = (await db.execute(select(User))).scalars().all()
    return {
        "total_users": len(user_count),
        "active_users": sum(1 for u in user_count if u.is_active),
        "admin_users": sum(1 for u in user_count if u.is_admin),
    }


# --- System Status ---

@router.get("/status")
async def system_status(
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Aggregated system status: accounts, sync, ES indexing, processing logs."""
    from elasticsearch import AsyncElasticsearch

    settings = get_settings()

    # 1. Mail accounts with sync state + IMAP folder counts
    result = await db.execute(select(MailAccount).order_by(MailAccount.id))
    accounts = result.scalars().all()

    # 2. Elasticsearch — get index-level info + per-folder doc counts
    es_status = []
    es_folder_counts = {}  # {account_id: {folder: count}}
    try:
        es = AsyncElasticsearch(settings.elasticsearch_url)
        indices_info = await es.cat.indices(index="mailia-*", format="json")
        for idx_info in indices_info:
            es_status.append({
                "index": idx_info.get("index", ""),
                "docs_count": int(idx_info.get("docs.count", 0)),
                "store_size": idx_info.get("store.size", "0b"),
                "health": idx_info.get("health", "unknown"),
                "status": idx_info.get("status", "unknown"),
            })

        # Per-account, per-folder doc counts from ES
        for acct in accounts:
            index_name = f"mailia-{acct.user_id}"
            try:
                agg_result = await es.search(
                    index=index_name,
                    body={
                        "size": 0,
                        "query": {"term": {"account_id": acct.id}},
                        "aggs": {"folders": {"terms": {"field": "folder", "size": 500}}},
                    },
                )
                buckets = agg_result.get("aggregations", {}).get("folders", {}).get("buckets", [])
                es_folder_counts[acct.id] = {b["key"]: b["doc_count"] for b in buckets}
            except Exception:
                es_folder_counts[acct.id] = {}

        await es.close()
    except Exception as e:
        es_status = [{"error": str(e)}]

    # 3. IMAP folder message counts (lightweight STATUS command)
    accounts_status = []
    for acct in accounts:
        sync_state = acct.sync_state or {}
        es_counts = es_folder_counts.get(acct.id, {})

        imap_counts = {}
        imap_total = 0
        folder_display_map = {}
        try:
            from src.imap.manager import IMAPManager, IMAPConfig, _imap_quote
            from src.security import decrypt_value as _decrypt
            config = IMAPConfig(
                host=acct.imap_host, port=acct.imap_port, ssl=acct.imap_ssl,
                user=acct.imap_user, password=_decrypt(acct.imap_password_encrypted),
            )
            imap = IMAPManager(config)
            imap.connect()
            try:
                folder_entries = imap.list_folders()
                folder_display_map = {}
                for entry in folder_entries:
                    fname = entry["name"] if isinstance(entry, dict) else entry
                    if isinstance(entry, dict) and "display_name" in entry:
                        folder_display_map[fname] = entry["display_name"]
                    try:
                        status, data = imap._conn.status(_imap_quote(fname), "(MESSAGES)")
                        if status == "OK" and data and data[0]:
                            import re
                            m = re.search(rb"MESSAGES\s+(\d+)", data[0])
                            if m:
                                count = int(m.group(1))
                                imap_counts[fname] = count
                                imap_total += count
                    except Exception:
                        pass
            finally:
                imap.disconnect()
        except Exception as e:
            imap_counts = {"_error": str(e)}

        es_total = sum(es_counts.values())

        # Build per-folder comparison
        all_folders = sorted(set(list(sync_state.keys()) + list(imap_counts.keys()) + list(es_counts.keys())) - {"_error"})
        folder_details = []
        for f in all_folders:
            folder_details.append({
                "folder": folder_display_map.get(f, f),
                "folder_path": f,
                "imap_messages": imap_counts.get(f),
                "es_indexed": es_counts.get(f, 0),
                "last_uid": sync_state.get(f),
            })

        accounts_status.append({
            "id": acct.id,
            "name": acct.name,
            "user_id": acct.user_id,
            "imap_user": acct.imap_user,
            "sync_enabled": acct.sync_enabled,
            "last_sync_at": acct.last_sync_at.isoformat() if acct.last_sync_at else None,
            "folders_synced": len(sync_state),
            "imap_total": imap_total,
            "es_total": es_total,
            "folder_details": folder_details,
            "imap_error": imap_counts.get("_error"),
        })

    # 3. Recent processing logs
    log_result = await db.execute(
        select(ProcessingLog)
        .order_by(ProcessingLog.created_at.desc())
        .limit(50)
    )
    logs = log_result.scalars().all()
    from src.imap.manager import _decode_imap_utf7
    recent_logs = [{
        "id": log.id,
        "user_id": log.user_id,
        "account_id": log.mail_account_id,
        "mail_uid": log.mail_uid,
        "folder": _decode_imap_utf7(log.folder) if log.folder else log.folder,
        "action": log.action_taken,
        "detail": log.action_detail,
        "ai_response": log.ai_response[:200] if log.ai_response else None,
        "created_at": log.created_at.isoformat() if log.created_at else None,
    } for log in logs]

    # 4. Processing log stats
    total_logs = await db.execute(select(func.count(ProcessingLog.id)))
    total_count = total_logs.scalar() or 0

    # 5. Celery worker status (via Redis inspection)
    worker_status = {"status": "unknown"}
    try:
        from src.worker.app import app as celery_app
        inspector = celery_app.control.inspect(timeout=2.0)
        active = inspector.active()
        scheduled = inspector.scheduled()
        registered = inspector.registered()
        worker_status = {
            "status": "online" if active else "offline",
            "active_tasks": {k: len(v) for k, v in active.items()} if active else {},
            "scheduled_tasks": {k: len(v) for k, v in scheduled.items()} if scheduled else {},
            "registered_tasks": list(list(registered.values())[0]) if registered else [],
        }
    except Exception as e:
        worker_status = {"status": "error", "error": str(e)}

    return {
        "accounts": accounts_status,
        "elasticsearch": es_status,
        "worker": worker_status,
        "processing_logs": {
            "total": total_count,
            "recent": recent_logs,
        },
    }

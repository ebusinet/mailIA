import imaplib
import logging
import time as _time_mod

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import Response
from starlette.concurrency import iterate_in_threadpool
from starlette.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, cast, String

from src.db.session import get_db
from src.db.models import User, MailAccount, LocalFolder, LocalEmail, SpamWhitelist, SpamBlacklist
from src.api.deps import get_current_user
from src.security import encrypt_value
from src.imap.manager import _imap_quote

logger = logging.getLogger(__name__)
router = APIRouter()


class MailAccountCreate(BaseModel):
    name: str
    imap_host: str
    imap_port: int = 993
    imap_ssl: bool = True
    imap_user: str
    imap_password: str
    smtp_host: str | None = None
    smtp_port: int = 465
    smtp_ssl: bool = True
    smtp_user: str | None = None
    smtp_password: str | None = None


class MailAccountUpdate(BaseModel):
    name: str | None = None
    imap_host: str | None = None
    imap_port: int | None = None
    imap_ssl: bool | None = None
    imap_user: str | None = None
    imap_password: str | None = None
    smtp_host: str | None = None
    smtp_port: int | None = None
    smtp_ssl: bool | None = None
    smtp_user: str | None = None
    smtp_password: str | None = None
    sync_enabled: bool | None = None


class MailAccountResponse(BaseModel):
    id: int
    name: str
    imap_host: str
    imap_port: int
    imap_ssl: bool
    imap_user: str
    smtp_host: str | None
    smtp_port: int | None
    smtp_ssl: bool
    smtp_user: str | None
    sync_enabled: bool
    last_sync_at: str | None

    model_config = {"from_attributes": True}


class SendEmailRequest(BaseModel):
    to: list[str]
    cc: list[str] = []
    bcc: list[str] = []
    subject: str = ""
    body_text: str = ""
    body_html: str = ""
    attachments: list[dict] = []  # [{"filename": str, "data_base64": str}]
    in_reply_to: str | None = None
    references: str | None = None
    priority: str | None = None  # "high", "normal", "low"
    request_read_receipt: bool = False
    request_delivery_receipt: bool = False


class SaveDraftRequest(BaseModel):
    to: list[str] = []
    cc: list[str] = []
    bcc: list[str] = []
    subject: str = ""
    body_text: str = ""
    body_html: str = ""
    attachments: list[dict] = []
    priority: str | None = None
    in_reply_to: str | None = None
    references: str | None = None


class FlagRequest(BaseModel):
    flag: str  # "seen", "flagged"
    action: str = "add"  # "add" or "remove"


class SpamScanRequest(BaseModel):
    folders: list[str] = []  # Empty = all folders


class WhitelistAddRequest(BaseModel):
    entry_type: str  # "email" or "domain"
    value: str

class WhitelistEntry(BaseModel):
    id: int
    entry_type: str
    value: str


class MoveRequest(BaseModel):
    target_folder: str


class BulkDeleteRequest(BaseModel):
    uids: list[str]
    folder: str


# ---------------------------------------------------------------------------
# Account CRUD
# ---------------------------------------------------------------------------

@router.get("/", response_model=list[MailAccountResponse])
async def list_accounts(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(MailAccount).where(MailAccount.user_id == user.id)
    )
    accounts = result.scalars().all()
    return [_to_response(a) for a in accounts]


@router.post("/", response_model=MailAccountResponse)
async def create_account(
    req: MailAccountCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = MailAccount(
        user_id=user.id,
        name=req.name,
        imap_host=req.imap_host,
        imap_port=req.imap_port,
        imap_ssl=req.imap_ssl,
        imap_user=req.imap_user,
        imap_password_encrypted=encrypt_value(req.imap_password),
        smtp_host=req.smtp_host,
        smtp_port=req.smtp_port,
        smtp_user=req.smtp_user,
        smtp_password_encrypted=encrypt_value(req.smtp_password) if req.smtp_password else None,
    )
    db.add(account)
    await db.commit()
    await db.refresh(account)

    return _to_response(account)


class TestCredentials(BaseModel):
    imap_host: str
    imap_port: int = 993
    imap_ssl: bool = True
    imap_user: str
    imap_password: str
    smtp_host: str | None = None
    smtp_port: int = 465
    test_type: str = "imap"  # "imap" or "smtp"


@router.post("/test-credentials")
async def test_credentials(
    req: TestCredentials,
    user: User = Depends(get_current_user),
):
    """Test IMAP or SMTP credentials without saving."""
    if req.test_type == "imap":
        from src.imap.manager import IMAPManager, IMAPConfig
        config = IMAPConfig(
            host=req.imap_host, port=req.imap_port, ssl=req.imap_ssl,
            user=req.imap_user, password=req.imap_password,
        )
        try:
            with IMAPManager(config) as imap:
                folders = imap.list_folders()
            return {"status": "ok", "message": f"Connexion IMAP reussie — {len(folders)} dossiers trouves"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    else:
        if not req.smtp_host:
            return {"status": "error", "message": "Aucun serveur SMTP renseigne"}
        import smtplib
        import ssl as ssl_mod
        try:
            if req.smtp_port in (465,):
                server = smtplib.SMTP_SSL(req.smtp_host, req.smtp_port, timeout=10)
            else:
                server = smtplib.SMTP(req.smtp_host, req.smtp_port, timeout=10)
                server.starttls(context=ssl_mod.create_default_context())
            server.login(req.imap_user, req.imap_password)
            server.quit()
            return {"status": "ok", "message": f"Connexion SMTP reussie ({req.smtp_host}:{req.smtp_port})"}
        except Exception as e:
            return {"status": "error", "message": str(e)}


@router.put("/{account_id}", response_model=MailAccountResponse)
async def update_account(
    account_id: int,
    req: MailAccountUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(MailAccount).where(MailAccount.id == account_id, MailAccount.user_id == user.id)
    )
    account = result.scalar_one_or_none()
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    for field in ("name", "imap_host", "imap_port", "imap_ssl", "imap_user",
                  "smtp_host", "smtp_port", "smtp_user", "sync_enabled"):
        val = getattr(req, field, None)
        if val is not None:
            setattr(account, field, val)
    if req.imap_password is not None:
        account.imap_password_encrypted = encrypt_value(req.imap_password)
    if req.smtp_password is not None:
        account.smtp_password_encrypted = encrypt_value(req.smtp_password)
    if req.smtp_ssl is not None:
        if hasattr(account, "smtp_ssl"):
            account.smtp_ssl = req.smtp_ssl

    await db.commit()
    await db.refresh(account)
    return _to_response(account)


@router.post("/{account_id}/test-imap")
async def test_imap(
    account_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )
    try:
        with IMAPManager(config) as imap:
            folders = imap.list_folders()
        return {"status": "ok", "message": f"Connexion IMAP reussie — {len(folders)} dossiers trouves"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/{account_id}/test-smtp")
async def test_smtp(
    account_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = await _get_account(account_id, user, db)
    if not account.smtp_host:
        return {"status": "error", "message": "Aucun serveur SMTP configure pour ce compte"}

    from src.security import decrypt_value as _dec
    import smtplib
    import ssl as ssl_mod

    smtp_password = _dec(account.smtp_password_encrypted) if account.smtp_password_encrypted else _dec(account.imap_password_encrypted)
    smtp_user = account.smtp_user or account.imap_user

    try:
        if account.smtp_port in (465,):
            server = smtplib.SMTP_SSL(account.smtp_host, account.smtp_port, timeout=10)
        else:
            server = smtplib.SMTP(account.smtp_host, account.smtp_port, timeout=10)
            server.starttls(context=ssl_mod.create_default_context())
        server.login(smtp_user, smtp_password)
        server.quit()
        return {"status": "ok", "message": f"Connexion SMTP reussie ({account.smtp_host}:{account.smtp_port})"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/{account_id}/sync")
async def sync_account(
    account_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = await _get_account(account_id, user, db)
    from src.worker.tasks import sync_account as sync_task
    sync_task.delay(account.id)
    return {"status": "sync_started", "account_id": account.id}


@router.delete("/{account_id}")
async def delete_account(
    account_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = await _get_account(account_id, user, db)
    await db.delete(account)
    await db.commit()
    return {"status": "deleted"}


# ---------------------------------------------------------------------------
# Folder browsing — using query params to avoid URL encoding issues with /
# ---------------------------------------------------------------------------

# In-memory caches
_folder_list_cache: dict[int, tuple[float, list]] = {}  # {account_id: (ts, raw_folders)}
_folder_counts_cache: dict[int, tuple[float, dict]] = {}  # {account_id: (ts, counts)}
_FOLDER_LIST_TTL = 120  # folder structure changes rarely
_FOLDER_COUNTS_TTL = 60  # counts can update more frequently


@router.get("/{account_id}/folders")
async def list_folders(
    account_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List IMAP folders — fast, no STATUS calls. Use /folders-counts for message counts."""
    account = await _get_account(account_id, user, db)

    now = _time_mod.time()
    cached = _folder_list_cache.get(account_id)
    if cached and (now - cached[0]) < _FOLDER_LIST_TTL:
        raw_folders = cached[1]
    else:
        from src.imap.manager import IMAPManager, IMAPConfig
        from src.security import decrypt_value as _dec
        config = IMAPConfig(
            host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
            user=account.imap_user, password=_dec(account.imap_password_encrypted),
        )
        try:
            with IMAPManager(config) as imap:
                raw_folders = imap.list_folders()
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"IMAP connection failed: {e}")
        _folder_list_cache[account_id] = (now, raw_folders)

    separator = raw_folders[0]["separator"] if raw_folders else "."
    tree = _build_folder_tree(raw_folders)

    def _tag_imap(nodes):
        for n in nodes:
            n["storage"] = "imap"
            n["count"] = 0
            if n.get("children"):
                _tag_imap(n["children"])
    _tag_imap(tree)

    # Query local folders
    local_result = await db.execute(
        select(LocalFolder).where(LocalFolder.account_id == account_id).order_by(LocalFolder.path)
    )
    local_folders_db = local_result.scalars().all()
    local_tree = []
    for lf in local_folders_db:
        local_tree.append({"name": lf.name, "path": lf.path, "storage": "local", "children": []})

    return {"account_id": account_id, "account_name": account.name, "folders": tree, "local_folders": local_tree, "separator": separator}


@router.get("/{account_id}/folders-counts")
async def list_folders_counts(
    account_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get message counts per folder — separate endpoint loaded asynchronously."""
    account = await _get_account(account_id, user, db)

    now = _time_mod.time()
    cached = _folder_counts_cache.get(account_id)
    if cached and (now - cached[0]) < _FOLDER_COUNTS_TTL:
        return {"counts": cached[1]}

    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    import re as _re
    import concurrent.futures

    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )

    def _fetch_counts():
        counts = {}
        with IMAPManager(config) as imap:
            raw_folders = imap.list_folders()
            conn = imap._conn
            for f in raw_folders:
                fname = f["name"]
                try:
                    st, sdata = conn.status(_imap_quote(fname), "(MESSAGES)")
                    if st == "OK" and sdata:
                        m = _re.search(r'MESSAGES\s+(\d+)', sdata[0].decode())
                        if m:
                            counts[fname] = int(m.group(1))
                except Exception:
                    pass
        return counts

    import asyncio
    loop = asyncio.get_event_loop()
    try:
        folder_counts = await loop.run_in_executor(None, _fetch_counts)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    _folder_counts_cache[account_id] = (now, folder_counts)
    return {"counts": folder_counts}


@router.post("/{account_id}/spam-scan")
async def spam_scan(
    account_id: int,
    req: SpamScanRequest,
    stream: bool = Query(False),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Scan specified folders (or all) for spam emails using SpamAssassin headers."""
    account = await _get_account(account_id, user, db)
    wl_result = await db.execute(
        select(SpamWhitelist).where(SpamWhitelist.account_id == account_id)
    )
    _wl_set = {e.value for e in wl_result.scalars().all()}
    bl_result = await db.execute(
        select(SpamBlacklist).where(SpamBlacklist.account_id == account_id)
    )
    _bl_set = {e.value for e in bl_result.scalars().all()}
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    import email as email_mod
    import re as _re

    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )

    target_folders = req.folders

    def _do_scan_stream():
        import json
        from src.imap.manager import IMAPManager, IMAPConfig
        from src.security import decrypt_value as _dec
        import email as email_mod
        import re as _re

        config = IMAPConfig(
            host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
            user=account.imap_user, password=_dec(account.imap_password_encrypted),
        )
        try:
            total_scanned = 0
            total_spam = 0
            with IMAPManager(config) as imap:
                conn = imap._conn
                if not target_folders:
                    raw_folders = imap.list_folders()
                    folders_to_scan = [f["name"] for f in raw_folders]
                else:
                    folders_to_scan = target_folders

                for fname in folders_to_scan:
                    try:
                        st, _ = _select_folder(conn, fname, readonly=True)
                        if st != "OK":
                            continue
                        st2, data = conn.uid("SEARCH", None, "ALL")
                        if st2 != "OK" or not data[0]:
                            yield json.dumps({"type": "folder_done", "folder": fname, "total": 0, "spam_count": 0, "spam_uids": [], "spam_details": []}) + "\n"
                            continue
                        all_uids = data[0].split()
                        folder_total = len(all_uids)
                        yield json.dumps({"type": "folder_start", "folder": fname, "total": folder_total}) + "\n"
                        spam_uids = []
                        spam_details = []

                        batch_size = 200
                        for i in range(0, len(all_uids), batch_size):
                            batch = all_uids[i:i+batch_size]
                            uid_range = b",".join(batch)
                            st3, fdata = conn.uid(
                                "FETCH", uid_range,
                                "(UID FLAGS BODY.PEEK[HEADER.FIELDS (FROM SUBJECT X-Spam-Status X-Spam-Flag X-Spam-Score Authentication-Results Precedence X-Mailer Message-ID X-VR-SPAMSCORE)])"
                            )
                            if st3 != "OK" or not fdata:
                                continue
                            j = 0
                            while j < len(fdata):
                                item = fdata[j]
                                if isinstance(item, tuple) and len(item) == 2:
                                    meta_line = item[0].decode() if isinstance(item[0], bytes) else str(item[0])
                                    header_bytes = item[1]
                                    uid = ""
                                    flags_str = ""
                                    um = _re.search(r'UID (\d+)', meta_line)
                                    if um:
                                        uid = um.group(1)
                                    fm = _re.search(r'FLAGS \(([^)]*)\)', meta_line)
                                    if fm:
                                        flags_str = fm.group(1)
                                    msg = email_mod.message_from_bytes(header_bytes)
                                    analysis = _spam_analysis(msg, flags_str, whitelist=_wl_set, blacklist=_bl_set)
                                    if analysis["is_spam"]:
                                        from_hdr = _decode_header(msg.get("From", "") or "")
                                        subj_hdr = _decode_header(msg.get("Subject", "") or "")
                                        spam_uids.append(uid)
                                        spam_details.append({
                                            "uid": uid,
                                            "from": from_hdr[:80],
                                            "subject": subj_hdr[:100],
                                            "score": analysis["score"],
                                            "reasons": analysis["reasons"],
                                        })
                                j += 1

                            scanned = min(i + batch_size, folder_total)
                            yield json.dumps({"type": "progress", "folder": fname, "scanned": scanned, "total": folder_total, "spam_found": len(spam_uids)}) + "\n"

                        total_scanned += folder_total
                        total_spam += len(spam_uids)
                        yield json.dumps({"type": "folder_done", "folder": fname, "total": folder_total, "spam_count": len(spam_uids), "spam_uids": spam_uids, "spam_details": spam_details}) + "\n"
                    except Exception as e:
                        logger.warning(f"Spam scan stream failed for folder {fname}: {e}")
                        yield json.dumps({"type": "folder_done", "folder": fname, "total": 0, "spam_count": 0, "spam_uids": [], "spam_details": [], "error": str(e)}) + "\n"

            yield json.dumps({"type": "result", "total_scanned": total_scanned, "total_spam": total_spam}) + "\n"
        except Exception as e:
            logger.error(f"Spam scan stream error: {e}", exc_info=True)
            yield json.dumps({"type": "error", "detail": str(e)}) + "\n"

    def _do_scan():
        results = {}
        total_scanned = 0
        total_spam = 0
        with IMAPManager(config) as imap:
            conn = imap._conn
            # If no folders specified, list all folders
            if not target_folders:
                raw_folders = imap.list_folders()
                folders_to_scan = [f["name"] for f in raw_folders]
            else:
                folders_to_scan = target_folders

            for fname in folders_to_scan:
                try:
                    st, _ = _select_folder(conn, fname, readonly=True)
                    if st != "OK":
                        continue
                    # Search all messages
                    st2, data = conn.uid("SEARCH", None, "ALL")
                    if st2 != "OK" or not data[0]:
                        results[fname] = {"total": 0, "spam_count": 0, "spam_uids": []}
                        continue
                    all_uids = data[0].split()
                    folder_total = len(all_uids)
                    spam_uids = []
                    spam_details = []

                    # Fetch in batches of 200
                    batch_size = 200
                    for i in range(0, len(all_uids), batch_size):
                        batch = all_uids[i:i+batch_size]
                        uid_range = b",".join(batch)
                        st3, fdata = conn.uid(
                            "FETCH", uid_range,
                            "(UID FLAGS BODY.PEEK[HEADER.FIELDS (FROM SUBJECT X-Spam-Status X-Spam-Flag X-Spam-Score Authentication-Results Precedence X-Mailer Message-ID X-VR-SPAMSCORE)])"
                        )
                        if st3 != "OK" or not fdata:
                            continue
                        j = 0
                        while j < len(fdata):
                            item = fdata[j]
                            if isinstance(item, tuple) and len(item) == 2:
                                meta_line = item[0].decode() if isinstance(item[0], bytes) else str(item[0])
                                header_bytes = item[1]
                                uid = ""
                                flags_str = ""
                                um = _re.search(r'UID (\d+)', meta_line)
                                if um:
                                    uid = um.group(1)
                                fm = _re.search(r'FLAGS \(([^)]*)\)', meta_line)
                                if fm:
                                    flags_str = fm.group(1)
                                msg = email_mod.message_from_bytes(header_bytes)
                                analysis = _spam_analysis(msg, flags_str, whitelist=_wl_set, blacklist=_bl_set)
                                if analysis["is_spam"]:
                                    from_hdr = _decode_header(msg.get("From", "") or "")
                                    subj_hdr = _decode_header(msg.get("Subject", "") or "")
                                    spam_uids.append(uid)
                                    spam_details.append({
                                        "uid": uid,
                                        "from": from_hdr[:80],
                                        "subject": subj_hdr[:100],
                                        "score": analysis["score"],
                                        "reasons": analysis["reasons"],
                                    })
                            j += 1

                    total_scanned += folder_total
                    total_spam += len(spam_uids)
                    results[fname] = {
                        "total": folder_total,
                        "spam_count": len(spam_uids),
                        "spam_uids": spam_uids,
                        "spam_details": spam_details,
                    }
                except Exception as e:
                    logger.warning(f"Spam scan failed for folder {fname}: {e}")
                    results[fname] = {"total": 0, "spam_count": 0, "spam_uids": [], "error": str(e)}
        return {"folders": results, "total_scanned": total_scanned, "total_spam": total_spam}

    if stream:
        return StreamingResponse(
            iterate_in_threadpool(_do_scan_stream()),
            media_type="application/x-ndjson",
        )

    import asyncio
    loop = asyncio.get_event_loop()
    try:
        scan_result = await loop.run_in_executor(None, _do_scan)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    return scan_result


@router.get("/{account_id}/spam-whitelist")
async def get_spam_whitelist(
    account_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get spam whitelist entries for an account."""
    account = await _get_account(account_id, user, db)
    result = await db.execute(
        select(SpamWhitelist).where(SpamWhitelist.account_id == account_id).order_by(SpamWhitelist.value)
    )
    entries = result.scalars().all()
    return [{"id": e.id, "entry_type": e.entry_type, "value": e.value} for e in entries]


@router.post("/{account_id}/spam-whitelist")
async def add_to_spam_whitelist(
    account_id: int,
    req: WhitelistAddRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Add an email or domain to the spam whitelist."""
    account = await _get_account(account_id, user, db)
    val = req.value.strip().lower()
    if req.entry_type not in ("email", "domain"):
        raise HTTPException(status_code=400, detail="entry_type must be 'email' or 'domain'")
    if not val:
        raise HTTPException(status_code=400, detail="value cannot be empty")
    # Check for duplicate
    existing = await db.execute(
        select(SpamWhitelist).where(
            SpamWhitelist.account_id == account_id,
            SpamWhitelist.entry_type == req.entry_type,
            SpamWhitelist.value == val,
        )
    )
    if existing.scalar_one_or_none():
        return {"status": "already_exists"}
    entry = SpamWhitelist(account_id=account_id, entry_type=req.entry_type, value=val)
    db.add(entry)
    await db.commit()
    await db.refresh(entry)
    return {"id": entry.id, "entry_type": entry.entry_type, "value": entry.value}


@router.delete("/{account_id}/spam-whitelist/by-sender")
async def remove_whitelist_by_sender(
    account_id: int,
    email_addr: str = Query(..., alias="email"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove all whitelist entries matching a sender email or its domain."""
    account = await _get_account(account_id, user, db)
    addr = email_addr.strip().lower()
    domain = addr.split("@")[-1] if "@" in addr else ""
    conditions = [SpamWhitelist.account_id == account_id]
    from sqlalchemy import or_
    match_conds = [SpamWhitelist.value == addr]
    if domain:
        match_conds.append(SpamWhitelist.value == domain)
    result = await db.execute(
        select(SpamWhitelist).where(*conditions, or_(*match_conds))
    )
    entries = result.scalars().all()
    for e in entries:
        await db.delete(e)
    if entries:
        await db.commit()
    return {"status": "deleted", "count": len(entries)}


@router.delete("/{account_id}/spam-whitelist/{entry_id}")
async def remove_from_spam_whitelist(
    account_id: int,
    entry_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove an entry from the spam whitelist."""
    account = await _get_account(account_id, user, db)
    result = await db.execute(
        select(SpamWhitelist).where(SpamWhitelist.id == entry_id, SpamWhitelist.account_id == account_id)
    )
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(status_code=404, detail="Whitelist entry not found")
    await db.delete(entry)
    await db.commit()
    return {"status": "deleted"}


# ---------------------------------------------------------------------------
# Spam blacklist CRUD
# ---------------------------------------------------------------------------

@router.get("/{account_id}/spam-blacklist")
async def get_spam_blacklist(
    account_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = await _get_account(account_id, user, db)
    result = await db.execute(
        select(SpamBlacklist).where(SpamBlacklist.account_id == account_id).order_by(SpamBlacklist.value)
    )
    entries = result.scalars().all()
    return [{"id": e.id, "entry_type": e.entry_type, "value": e.value} for e in entries]


@router.post("/{account_id}/spam-blacklist")
async def add_to_spam_blacklist(
    account_id: int,
    req: WhitelistAddRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = await _get_account(account_id, user, db)
    val = req.value.strip().lower()
    if req.entry_type not in ("email", "domain"):
        raise HTTPException(status_code=400, detail="entry_type must be 'email' or 'domain'")
    if not val:
        raise HTTPException(status_code=400, detail="value cannot be empty")
    existing = await db.execute(
        select(SpamBlacklist).where(
            SpamBlacklist.account_id == account_id,
            SpamBlacklist.entry_type == req.entry_type,
            SpamBlacklist.value == val,
        )
    )
    if existing.scalar_one_or_none():
        return {"status": "already_exists"}
    entry = SpamBlacklist(account_id=account_id, entry_type=req.entry_type, value=val)
    db.add(entry)
    await db.commit()
    await db.refresh(entry)
    return {"id": entry.id, "entry_type": entry.entry_type, "value": entry.value}


@router.delete("/{account_id}/spam-blacklist/by-sender")
async def remove_blacklist_by_sender(
    account_id: int,
    email_addr: str = Query(..., alias="email"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = await _get_account(account_id, user, db)
    addr = email_addr.strip().lower()
    domain = addr.split("@")[-1] if "@" in addr else ""
    from sqlalchemy import or_
    match_conds = [SpamBlacklist.value == addr]
    if domain:
        match_conds.append(SpamBlacklist.value == domain)
    result = await db.execute(
        select(SpamBlacklist).where(SpamBlacklist.account_id == account_id, or_(*match_conds))
    )
    entries = result.scalars().all()
    for e in entries:
        await db.delete(e)
    if entries:
        await db.commit()
    return {"status": "deleted", "count": len(entries)}


@router.delete("/{account_id}/spam-blacklist/{entry_id}")
async def remove_from_spam_blacklist(
    account_id: int,
    entry_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account = await _get_account(account_id, user, db)
    result = await db.execute(
        select(SpamBlacklist).where(SpamBlacklist.id == entry_id, SpamBlacklist.account_id == account_id)
    )
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(status_code=404, detail="Blacklist entry not found")
    await db.delete(entry)
    await db.commit()
    return {"status": "deleted"}


class CreateFolderRequest(BaseModel):
    folder_name: str


@router.post("/{account_id}/create-folder")
async def create_folder(
    account_id: int,
    req: CreateFolderRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new IMAP folder."""
    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )
    try:
        with IMAPManager(config) as imap:
            ok = imap.create_folder(req.folder_name)
            if not ok:
                raise HTTPException(status_code=400, detail="Failed to create folder")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")
    _folder_list_cache.pop(account_id, None)
    _folder_counts_cache.pop(account_id, None)
    return {"status": "created", "folder": req.folder_name}


class DeleteFolderRequest(BaseModel):
    folder_name: str
    force: bool = False


@router.post("/{account_id}/delete-folder")
async def delete_folder(
    account_id: int,
    req: DeleteFolderRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete an IMAP folder."""
    protected = {"INBOX", "Sent", "Drafts", "Trash", "Junk", "Spam"}
    base_name = req.folder_name.rsplit(".", 1)[-1].rsplit("/", 1)[-1]
    if base_name in protected or req.folder_name.upper() == "INBOX":
        raise HTTPException(status_code=400, detail=f"Cannot delete system folder: {req.folder_name}")

    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )
    try:
        with IMAPManager(config) as imap:
            from src.imap.manager import _imap_quote

            def _empty_folder(conn, folder):
                st, resp = conn.select(_imap_quote(folder))
                if st != "OK":
                    return
                st2, data = conn.uid("SEARCH", None, "ALL")
                if st2 == "OK" and data[0]:
                    uids = data[0].decode().split()
                    for i in range(0, len(uids), 50):
                        batch = ",".join(uids[i:i + 50])
                        conn.uid("STORE", batch, "+FLAGS", "(\\Deleted)")
                    conn.expunge()
                conn.close()

            conn = imap._conn

            def _count_folder(conn, folder):
                st, resp = conn.select(_imap_quote(folder), readonly=True)
                c = 0
                if st == "OK" and resp and resp[0]:
                    c = int(resp[0])
                conn.close()
                return c

            # Count messages in main folder
            msg_count = _count_folder(conn, req.folder_name)

            # List subfolders
            import re
            st, folder_list = conn.list(_imap_quote(req.folder_name))
            subfolders = []
            if st == "OK" and folder_list:
                for item in folder_list:
                    if item is None:
                        continue
                    line = item.decode() if isinstance(item, bytes) else str(item)
                    match = re.search(r'"([^"]*)" "?([^"]*)"?\s*$', line)
                    if match:
                        fname = match.group(2).strip().strip('"')
                        if fname != req.folder_name:
                            subfolders.append(fname)

            # Count messages in all subfolders
            sub_counts = {}
            for sf in subfolders:
                sub_counts[sf] = _count_folder(conn, sf)
            total_emails = msg_count + sum(sub_counts.values())

            has_content = total_emails > 0 or len(subfolders) > 0
            if has_content and not req.force:
                import json as _json
                from src.imap.manager import _decode_imap_utf7
                def _dec_name(n):
                    try: return _decode_imap_utf7(n)
                    except Exception: return n
                info = {"total_emails": total_emails, "folder_emails": msg_count,
                        "subfolders": len(subfolders), "name": _dec_name(req.folder_name), "details": {}}
                if msg_count > 0:
                    info["details"][_dec_name(req.folder_name)] = msg_count
                for sf, sc in sub_counts.items():
                    if sc > 0:
                        info["details"][_dec_name(sf)] = sc
                raise HTTPException(status_code=409, detail=_json.dumps(info, ensure_ascii=False))

            if req.force:
                # Delete subfolders deepest first
                subfolders.sort(key=lambda x: x.count('/') + x.count('.'), reverse=True)
                for sf in subfolders:
                    _empty_folder(conn, sf)
                    imap.delete_folder(sf)
                # Empty the main folder
                if msg_count > 0:
                    _empty_folder(conn, req.folder_name)

            ok = imap.delete_folder(req.folder_name)
            if not ok:
                raise HTTPException(status_code=400, detail="Failed to delete folder")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")
    _folder_list_cache.pop(account_id, None)
    _folder_counts_cache.pop(account_id, None)
    return {"status": "deleted", "folder": req.folder_name}


class EmptyFolderRequest(BaseModel):
    folder_name: str


@router.post("/{account_id}/empty-folder")
async def empty_folder(
    account_id: int,
    req: EmptyFolderRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Permanently delete all emails in a folder (for spam/drafts purge)."""
    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig, _imap_quote
    from src.security import decrypt_value as _dec
    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )
    try:
        with IMAPManager(config) as imap:
            conn = imap._conn
            status, _ = conn.select(_imap_quote(req.folder_name))
            if status != "OK":
                raise HTTPException(status_code=400, detail=f"Cannot select folder: {req.folder_name}")
            status, data = conn.uid("SEARCH", None, "ALL")
            if status != "OK" or not data[0]:
                return {"status": "empty", "deleted": 0}
            uids = data[0].decode().split()
            for i in range(0, len(uids), 50):
                batch = ",".join(uids[i:i+50])
                conn.uid("STORE", batch, "+FLAGS", "(\\Deleted)")
            conn.expunge()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")
    _folder_list_cache.pop(account_id, None)
    _folder_counts_cache.pop(account_id, None)
    return {"status": "emptied", "deleted": len(uids)}


class RenameFolderRequest(BaseModel):
    old_name: str
    new_name: str


@router.post("/{account_id}/rename-folder")
async def rename_folder(
    account_id: int,
    req: RenameFolderRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Rename/move an IMAP folder."""
    protected = {"INBOX", "Sent", "Drafts", "Trash", "Junk", "Spam"}
    base_name = req.old_name.rsplit(".", 1)[-1].rsplit("/", 1)[-1]
    if base_name in protected or req.old_name.upper() == "INBOX":
        raise HTTPException(status_code=400, detail=f"Cannot move system folder: {req.old_name}")

    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )
    try:
        with IMAPManager(config) as imap:
            ok = imap.rename_folder(req.old_name, req.new_name)
            if not ok:
                raise HTTPException(status_code=400, detail="Failed to rename folder")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")
    _folder_list_cache.pop(account_id, None)
    _folder_counts_cache.pop(account_id, None)
    return {"status": "renamed", "old_name": req.old_name, "new_name": req.new_name}


@router.get("/{account_id}/folders-raw")
async def list_folders_raw(
    account_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Debug endpoint: raw IMAP folder list without tree transformation."""
    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )
    try:
        with IMAPManager(config) as imap:
            raw_folders = imap.list_folders()
            # Also get the raw LIST response for debugging
            status, raw_data = imap._conn.list()
            raw_lines = []
            for item in (raw_data or []):
                if isinstance(item, bytes):
                    raw_lines.append(item.decode("utf-8", errors="replace"))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    return {
        "account_id": account_id,
        "parsed_folders": raw_folders,
        "raw_list_response": raw_lines,
    }


@router.get("/{account_id}/messages")
async def list_messages(
    account_id: int,
    folder: str = Query(..., description="IMAP folder path"),
    q: str = Query("", description="Search query (IMAP TEXT search)"),
    page: int = 0,
    size: int = 50,
    storage: str = Query("imap"),
    filter_from: str = Query("", description="Filter by sender (substring)"),
    filter_to: str = Query("", description="Filter by recipient (substring)"),
    filter_subject: str = Query("", description="Filter by subject (substring)"),
    filter_date: str = Query("", description="Filter by date (substring)"),
    filter_replied: bool = Query(False, description="Filter only replied messages"),
    filter_attachments: bool = Query(False, description="Filter only messages with attachments"),
    filter_spam: bool = Query(False, description="Filter only spam messages"),
    sort_by: str = Query("date", description="Sort column: date, from, subject"),
    sort_order: str = Query("desc", description="Sort order: asc or desc"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List messages in an IMAP or local folder — folder passed as query param to handle / in names."""
    if storage == "local":
        from sqlalchemy import func as sa_func
        folder_result = await db.execute(
            select(LocalFolder).where(
                LocalFolder.account_id == account_id,
                LocalFolder.path == folder
            )
        )
        local_folder = folder_result.scalar_one_or_none()
        if not local_folder:
            raise HTTPException(status_code=404, detail="Local folder not found")

        local_filters = [LocalEmail.folder_id == local_folder.id]
        if filter_from:
            local_filters.append(LocalEmail.from_addr.ilike(f"%{filter_from}%"))
        if filter_to:
            local_filters.append(LocalEmail.to_addr.ilike(f"%{filter_to}%"))
        if filter_subject:
            local_filters.append(LocalEmail.subject.ilike(f"%{filter_subject}%"))
        if filter_date:
            local_filters.append(cast(LocalEmail.date, String).ilike(f"%{filter_date}%"))
        if filter_replied:
            local_filters.append(LocalEmail.answered == True)
        if filter_attachments:
            local_filters.append(LocalEmail.has_attachments == True)

        count_result = await db.execute(
            select(sa_func.count()).where(*local_filters)
        )
        total = count_result.scalar()

        _local_sort_map = {"date": LocalEmail.date, "from": LocalEmail.from_addr, "subject": LocalEmail.subject}
        _sort_col = _local_sort_map.get(sort_by, LocalEmail.date)
        _order = _sort_col.asc() if sort_order == "asc" else _sort_col.desc()
        emails_result = await db.execute(
            select(LocalEmail).where(*local_filters)
            .order_by(_order)
            .offset(page * size).limit(size)
        )
        emails = emails_result.scalars().all()

        messages = []
        for em in emails:
            messages.append({
                "uid": f"L{em.id}",
                "from": em.from_addr or "",
                "to": em.to_addr or "",
                "subject": em.subject or "",
                "date": em.date.strftime("%Y-%m-%d %H:%M") if em.date else "",
                "seen": em.seen,
                "flagged": em.flagged,
                "answered": em.answered,
                "has_attachments": em.has_attachments,
                "spam": False,
            })
        return {"folder": folder, "total": total, "page": page, "size": size, "messages": messages, "storage": "local"}

    account = await _get_account(account_id, user, db)

    # Load spam whitelist and blacklist for this account
    wl_result = await db.execute(
        select(SpamWhitelist).where(SpamWhitelist.account_id == account_id)
    )
    _whitelist_entries = {e.value for e in wl_result.scalars().all()}
    bl_result = await db.execute(
        select(SpamBlacklist).where(SpamBlacklist.account_id == account_id)
    )
    _blacklist_entries = {e.value for e in bl_result.scalars().all()}

    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    import email as email_mod
    import email.utils
    import email.header
    import re

    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )

    try:
        with IMAPManager(config) as imap:
            conn = imap._conn
            status, select_data = _select_folder(conn, folder, readonly=True)

            # Get EXISTS count from SELECT response for validation
            exists_count = 0
            try:
                exists_count = int(select_data[0])
            except (IndexError, ValueError, TypeError):
                pass

            # Build IMAP SEARCH criteria from q and column filters
            search_parts = []
            if q.strip():
                import re as _re
                raw = q.strip()
                for match in _re.finditer(r'(from|subject):(\S+)', raw):
                    field = match.group(1).upper()
                    val = match.group(2).replace('"', '\\"')
                    search_parts.append(f'{field} "{val}"')
                    raw = raw.replace(match.group(0), '')
                remainder = raw.strip().replace('"', '\\"')
                if remainder:
                    search_parts.append(f'TEXT "{remainder}"')

            if filter_from:
                search_parts.append(f'FROM "{filter_from.replace(chr(34), "")}"')
            if filter_to:
                search_parts.append(f'TO "{filter_to.replace(chr(34), "")}"')
            if filter_subject:
                search_parts.append(f'SUBJECT "{filter_subject.replace(chr(34), "")}"')
            if filter_replied:
                search_parts.append('ANSWERED')

            if search_parts:
                criteria = ' '.join(search_parts)
                charset = None
                try:
                    criteria.encode('ascii')
                except UnicodeEncodeError:
                    charset = 'UTF-8'
                status, data = conn.uid("SEARCH", charset, criteria)
                search_uids = set(data[0].decode().split()) if data[0] else set()
            else:
                search_uids = None  # means "all"

            # Fetch all UIDs + INTERNALDATE for date-based pagination
            status, data = conn.uid("FETCH", "1:*", "(UID INTERNALDATE)")
            uid_dates = []
            if status == "OK" and data:
                for item in data:
                    if isinstance(item, tuple) and len(item) == 2:
                        meta = item[0].decode() if isinstance(item[0], bytes) else str(item[0])
                    elif isinstance(item, bytes):
                        meta = item.decode(errors="replace")
                    else:
                        continue
                    uid_match = re.search(r'UID (\d+)', meta)
                    date_match = re.search(r'INTERNALDATE "([^"]+)"', meta)
                    if uid_match and date_match:
                        uid = uid_match.group(1)
                        if search_uids is not None and uid not in search_uids:
                            continue
                        idate = date_match.group(1)
                        uid_dates.append((uid, idate))

            # Sort by INTERNALDATE descending (newest first)
            from email.utils import parsedate_to_datetime as _pdt
            from datetime import datetime as _dt
            def _parse_idate(s):
                try:
                    return _pdt(s)
                except Exception:
                    pass
                try:
                    # INTERNALDATE format: "09-Mar-2026 17:24:42 +0100"
                    return _dt.strptime(s.strip(), "%d-%b-%Y %H:%M:%S %z")
                except Exception:
                    return _dt.min.replace(tzinfo=None)

            uid_dates.sort(key=lambda x: _parse_idate(x[1]), reverse=True)

            # Apply date filter (substring match on formatted date)
            if filter_date:
                fd_lower = filter_date.lower()
                uid_dates = [
                    (uid, idate) for uid, idate in uid_dates
                    if fd_lower in _parse_idate(idate).strftime("%Y-%m-%d %H:%M").lower()
                ]

            # Server-side sort by from/subject requires fetching headers for all UIDs
            if sort_by in ("from", "subject") and uid_dates:
                header_field = "FROM" if sort_by == "from" else "SUBJECT"
                all_uids = [e[0] for e in uid_dates]
                uid_sort_keys = {}
                # Fetch in chunks of 200
                for ci in range(0, len(all_uids), 200):
                    chunk = all_uids[ci:ci + 200]
                    uid_set = ",".join(chunk)
                    st_h, dt_h = conn.uid("FETCH", uid_set, f"(UID BODY.PEEK[HEADER.FIELDS ({header_field})])")
                    if st_h == "OK" and dt_h:
                        hi = 0
                        while hi < len(dt_h):
                            h_item = dt_h[hi]
                            if isinstance(h_item, tuple) and len(h_item) == 2:
                                h_meta = h_item[0].decode() if isinstance(h_item[0], bytes) else str(h_item[0])
                                h_bytes = h_item[1]
                                h_uid_m = re.search(r'UID (\d+)', h_meta)
                                if h_uid_m:
                                    h_msg = email_mod.message_from_bytes(h_bytes)
                                    val = _decode_header(h_msg.get(header_field, ""))
                                    uid_sort_keys[h_uid_m.group(1)] = val.lower()
                            hi += 1
                reverse = sort_order == "desc"
                uid_dates.sort(key=lambda x: uid_sort_keys.get(x[0], ""), reverse=reverse)
            elif sort_by == "date" and sort_order == "asc":
                uid_dates.reverse()

            total = len(uid_dates)

            if not q and not filter_from and not filter_to and not filter_subject and not filter_date and not filter_replied and exists_count and total != exists_count:
                logger.warning("FETCH returned %d UIDs but SELECT EXISTS=%d for folder %s", total, exists_count, folder)

            def _fetch_messages(uids_to_fetch, idate_lookup):
                """Fetch full message data for a list of UIDs."""
                result = []
                if not uids_to_fetch:
                    return result
                uid_range = ",".join(uids_to_fetch)
                st, dt = conn.uid("FETCH", uid_range, "(UID FLAGS BODYSTRUCTURE BODY.PEEK[HEADER.FIELDS (FROM TO SUBJECT DATE X-Spam-Status X-Spam-Flag X-Spam-Score Authentication-Results X-VR-SPAMSCORE)])")
                if st == "OK" and dt:
                    j = 0
                    while j < len(dt):
                        item = dt[j]
                        if isinstance(item, tuple) and len(item) == 2:
                            meta_line = item[0].decode() if isinstance(item[0], bytes) else str(item[0])
                            header_bytes = item[1]
                            uid = ""
                            flags_str = ""
                            um = re.search(r'UID (\d+)', meta_line)
                            if um:
                                uid = um.group(1)
                            fm = re.search(r'FLAGS \(([^)]*)\)', meta_line)
                            if fm:
                                flags_str = fm.group(1)
                            msg = email_mod.message_from_bytes(header_bytes)
                            from_addr = _decode_header(msg.get("From", ""))
                            to_addr = _decode_header(msg.get("To", ""))
                            subject = _decode_header(msg.get("Subject", ""))
                            date_str = ""
                            idate_raw = idate_lookup.get(uid, "")
                            if idate_raw:
                                try:
                                    idt = _parse_idate(idate_raw)
                                    date_str = idt.strftime("%Y-%m-%d %H:%M")
                                except Exception:
                                    date_str = idate_raw
                            seen = "\\Seen" in flags_str
                            flagged = "\\Flagged" in flags_str
                            answered = "\\Answered" in flags_str
                            has_att = 'attachment' in meta_line.lower() or '"attachment"' in meta_line.lower()
                            spam = _spam_analysis(msg, flags_str, whitelist=_whitelist_entries, blacklist=_blacklist_entries)["is_spam"]
                            result.append({
                                "uid": uid, "from": from_addr, "to": to_addr,
                                "subject": subject, "date": date_str,
                                "seen": seen, "flagged": flagged,
                                "answered": answered, "has_attachments": has_att,
                                "spam": spam,
                            })
                        j += 1
                uid_order = {u: idx for idx, u in enumerate(uids_to_fetch)}
                result.sort(key=lambda m: uid_order.get(m["uid"], 999))
                return result

            # Build post-fetch filter for precise display-value matching
            def _display_name(addr):
                """Extract display name from 'Name <email>' or just return as-is."""
                if '<' in addr:
                    name = addr[:addr.index('<')].strip().strip('"').strip("'")
                    if name:
                        return name
                return addr

            _post_filters = []
            if filter_from:
                _ff = filter_from.lower()
                _post_filters.append(lambda m, _f=_ff: _f in _display_name(m["from"]).lower())
            if filter_to:
                _ft = filter_to.lower()
                _post_filters.append(lambda m, _f=_ft: _f in _display_name(m["to"]).lower())
            if filter_subject:
                _fsub = filter_subject.lower()
                _post_filters.append(lambda m, _f=_fsub: _f in m["subject"].lower())
            if filter_attachments:
                _post_filters.append(lambda m: m["has_attachments"])

            def _post_filter(m):
                return all(f(m) for f in _post_filters)

            if filter_spam:
                # Two-pass approach for spam: fast scan then full fetch
                # Pass 1: lightweight FETCH to identify spam UIDs
                _spam_headers = "(UID FLAGS BODY.PEEK[HEADER.FIELDS (FROM SUBJECT X-Spam-Status X-Spam-Flag X-Spam-Score Authentication-Results X-VR-SPAMSCORE Precedence X-Mailer Message-ID)])"
                spam_uids_set = set()
                scan_batch = 500
                all_uid_list = [e[0] for e in uid_dates]
                for i in range(0, len(all_uid_list), scan_batch):
                    chunk = all_uid_list[i:i + scan_batch]
                    uid_range = ",".join(chunk)
                    st_s, dt_s = conn.uid("FETCH", uid_range, _spam_headers)
                    if st_s != "OK" or not dt_s:
                        continue
                    j = 0
                    while j < len(dt_s):
                        item = dt_s[j]
                        if isinstance(item, tuple) and len(item) == 2:
                            meta_l = item[0].decode() if isinstance(item[0], bytes) else str(item[0])
                            hdr = item[1]
                            um = re.search(r'UID (\d+)', meta_l)
                            if um:
                                u = um.group(1)
                                fm2 = re.search(r'FLAGS \(([^)]*)\)', meta_l)
                                fs2 = fm2.group(1) if fm2 else ""
                                msg_s = email_mod.message_from_bytes(hdr)
                                if _spam_analysis(msg_s, fs2, whitelist=_whitelist_entries, blacklist=_blacklist_entries)["is_spam"]:
                                    spam_uids_set.add(u)
                        j += 1
                # Filter uid_dates to only spam UIDs, preserving sort order
                uid_dates = [e for e in uid_dates if e[0] in spam_uids_set]
                total = len(uid_dates)
                # Apply remaining post-filters on paginated results if any
                if _post_filters:
                    page_start = page * size
                    page_entries = uid_dates[page_start:page_start + size]
                    page_uids = [e[0] for e in page_entries]
                    idate_map = {e[0]: e[1] for e in page_entries}
                    all_msgs = _fetch_messages(page_uids, idate_map)
                    messages = [m for m in all_msgs if _post_filter(m)]
                    total = len(uid_dates)  # approximate
                else:
                    # Pass 2: full fetch only for the page
                    page_start = page * size
                    page_entries = uid_dates[page_start:page_start + size]
                    page_uids = [e[0] for e in page_entries]
                    idate_map = {e[0]: e[1] for e in page_entries}
                    messages = _fetch_messages(page_uids, idate_map)
            elif _post_filters:
                # Scan batches to find messages passing all post-filters
                batch_size = 100
                messages = []
                filtered_total = 0
                skip = page * size
                for batch_start in range(0, total, batch_size):
                    batch_entries = uid_dates[batch_start:batch_start + batch_size]
                    batch_uids = [e[0] for e in batch_entries]
                    idate_map = {e[0]: e[1] for e in batch_entries}
                    batch_msgs = _fetch_messages(batch_uids, idate_map)
                    for m in batch_msgs:
                        if _post_filter(m):
                            filtered_total += 1
                            if filtered_total > skip and len(messages) < size:
                                messages.append(m)
                    if len(messages) >= size and filtered_total > skip + size:
                        break
                total = filtered_total
            else:
                # Standard pagination — no post-filters needed
                page_start = page * size
                page_entries = uid_dates[page_start:page_start + size]
                page_uids = [e[0] for e in page_entries]
                idate_map = {e[0]: e[1] for e in page_entries}
                messages = _fetch_messages(page_uids, idate_map)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    return {
        "folder": folder,
        "total": total,
        "page": page,
        "size": size,
        "messages": messages,
    }


class MultiSearchRequest(BaseModel):
    q: str
    folders: list[str]
    max_per_folder: int = 100


@router.post("/{account_id}/search-multi")
async def search_multi_folders(
    account_id: int,
    req: MultiSearchRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Search for emails across multiple IMAP folders."""
    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    import email as email_mod
    import email.utils
    import re

    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )

    search_term = req.q.strip().replace('"', '\\"')
    charset = None
    criteria = f'TEXT "{search_term}"'
    try:
        search_term.encode('ascii')
    except UnicodeEncodeError:
        charset = 'UTF-8'

    from src.imap.manager import _decode_imap_utf7
    results = []
    errors = []

    try:
        with IMAPManager(config) as imap:
            conn = imap._conn
            for folder in req.folders:
                try:
                    status, _ = _select_folder(conn, folder, readonly=True)
                    if status != "OK":
                        continue
                    status, data = conn.uid("SEARCH", charset, criteria)
                    if status != "OK" or not data[0]:
                        continue
                    uids = data[0].decode().split()
                    if not uids:
                        continue
                    # Fetch latest N
                    fetch_uids = uids[-req.max_per_folder:]
                    uid_range = ",".join(fetch_uids)
                    status, fdata = conn.uid("FETCH", uid_range, "(UID FLAGS BODY.PEEK[HEADER.FIELDS (FROM TO SUBJECT DATE X-Spam-Status X-Spam-Flag X-Spam-Score Authentication-Results X-VR-SPAMSCORE)])")
                    if status != "OK" or not fdata:
                        continue
                    i = 0
                    while i < len(fdata):
                        item = fdata[i]
                        if isinstance(item, tuple) and len(item) == 2:
                            meta_line = item[0].decode() if isinstance(item[0], bytes) else str(item[0])
                            header_bytes = item[1]
                            uid = ""
                            flags_str = ""
                            uid_match = re.search(r'UID (\d+)', meta_line)
                            if uid_match:
                                uid = uid_match.group(1)
                            flags_match = re.search(r'FLAGS \(([^)]*)\)', meta_line)
                            if flags_match:
                                flags_str = flags_match.group(1)
                            msg = email_mod.message_from_bytes(header_bytes)
                            from_addr = _decode_header(msg.get("From", ""))
                            to_addr = _decode_header(msg.get("To", ""))
                            subject = _decode_header(msg.get("Subject", ""))
                            date_raw = msg.get("Date", "")
                            date_str = ""
                            try:
                                dt = email.utils.parsedate_to_datetime(date_raw)
                                date_str = dt.strftime("%Y-%m-%d %H:%M")
                            except Exception:
                                date_str = date_raw
                            seen = "\\Seen" in flags_str
                            flagged = "\\Flagged" in flags_str
                            answered = "\\Answered" in flags_str
                            spam = _detect_spam_from_headers(msg, flags_str)
                            results.append({
                                "uid": uid,
                                "folder": folder,
                                "folder_display": _decode_imap_utf7(folder),
                                "from": from_addr,
                                "to": to_addr,
                                "subject": subject,
                                "date": date_str,
                                "seen": seen,
                                "flagged": flagged,
                                "answered": answered,
                                "spam": spam,
                            })
                        i += 1
                except Exception as e:
                    errors.append(f"{folder}: {e}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    # Sort by date descending
    results.sort(key=lambda m: m.get("date", ""), reverse=True)
    return {"total": len(results), "messages": results, "errors": errors}


@router.get("/{account_id}/message/{uid}")
async def get_message(
    account_id: int,
    uid: str,
    folder: str = Query(..., description="IMAP folder path"),
    storage: str = Query("imap"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch full email content by UID — live from IMAP or local DB."""
    if storage == "local":
        email_id = int(uid.replace("L", ""))
        result = await db.execute(
            select(LocalEmail).where(LocalEmail.id == email_id)
        )
        em = result.scalar_one_or_none()
        if not em:
            raise HTTPException(status_code=404, detail="Email not found")
        attachments = []
        if em.raw_message:
            import email as email_mod
            msg = email_mod.message_from_bytes(em.raw_message)
            idx = 0
            for part in msg.walk():
                disp = str(part.get("Content-Disposition", ""))
                if "attachment" in disp:
                    attachments.append({
                        "index": idx,
                        "filename": _decode_header(part.get_filename() or "unnamed"),
                        "content_type": part.get_content_type(),
                        "size": len(part.get_payload(decode=True) or b""),
                    })
                    idx += 1
        return {
            "uid": f"L{em.id}", "folder": folder, "storage": "local",
            "from": em.from_addr or "", "to": em.to_addr or "",
            "cc": em.cc_addr or "", "reply_to": "",
            "subject": em.subject or "",
            "date": em.date.strftime("%Y-%m-%d %H:%M") if em.date else "",
            "body_text": em.body_text or "", "body_html": em.body_html or "",
            "seen": em.seen, "flagged": em.flagged, "answered": em.answered,
            "priority": "normal", "message_id": em.message_id_header or "",
            "in_reply_to": "", "references": "",
            "attachments": attachments, "has_attachments": em.has_attachments,
        }

    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    import email as email_mod
    import email.utils

    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )

    try:
        with IMAPManager(config) as imap:
            conn = imap._conn
            status, _ = _select_folder(conn, folder, readonly=True)

            status, data = conn.uid("FETCH", uid, "(RFC822 FLAGS)")
            if status != "OK" or not data or data[0] is None:
                raise HTTPException(status_code=404, detail="Message not found")

            # Extract flags from response
            flags_str = ""
            raw = None
            for item in data:
                if isinstance(item, tuple) and len(item) == 2:
                    meta = item[0].decode() if isinstance(item[0], bytes) else str(item[0])
                    import re
                    fm = re.search(r'FLAGS \(([^)]*)\)', meta)
                    if fm:
                        flags_str = fm.group(1)
                    raw = item[1]
                elif isinstance(item, bytes) and not flags_str:
                    s = item.decode(errors='replace')
                    import re
                    fm = re.search(r'FLAGS \(([^)]*)\)', s)
                    if fm:
                        flags_str = fm.group(1)

            if raw is None:
                raise HTTPException(status_code=404, detail="Message not found")

            msg = email_mod.message_from_bytes(raw)

            from_addr = _decode_header(msg.get("From", ""))
            to_addr = _decode_header(msg.get("To", ""))
            cc_addr = _decode_header(msg.get("Cc", ""))
            reply_to = _decode_header(msg.get("Reply-To", ""))
            subject = _decode_header(msg.get("Subject", ""))
            message_id = msg.get("Message-ID", "")
            in_reply_to = msg.get("In-Reply-To", "")
            references = msg.get("References", "")
            date_raw = msg.get("Date", "")
            date_str = ""
            try:
                dt = email.utils.parsedate_to_datetime(date_raw)
                date_str = dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                date_str = date_raw

            # Priority
            priority = "normal"
            x_priority = msg.get("X-Priority", "")
            importance = msg.get("Importance", "").lower()
            if x_priority in ("1", "2") or importance == "high":
                priority = "high"
            elif x_priority in ("4", "5") or importance == "low":
                priority = "low"

            seen = "\\Seen" in flags_str
            flagged = "\\Flagged" in flags_str
            answered = "\\Answered" in flags_str

            body_text = ""
            body_html = ""
            attachments = []

            for part in msg.walk():
                content_type = part.get_content_type()
                disposition = str(part.get("Content-Disposition", ""))

                if "attachment" in disposition:
                    filename = part.get_filename() or "unnamed"
                    payload = part.get_payload(decode=True) or b""
                    attachments.append({
                        "filename": _decode_header(filename),
                        "content_type": content_type,
                        "size": len(payload),
                    })
                elif content_type == "text/plain" and not body_text:
                    payload = part.get_payload(decode=True)
                    if payload:
                        charset = part.get_content_charset() or "utf-8"
                        try:
                            body_text = payload.decode(charset, errors="replace")
                        except (UnicodeDecodeError, LookupError):
                            body_text = payload.decode("utf-8", errors="replace")
                elif content_type == "text/html" and not body_html:
                    payload = part.get_payload(decode=True)
                    if payload:
                        charset = part.get_content_charset() or "utf-8"
                        try:
                            body_html = payload.decode(charset, errors="replace")
                        except (UnicodeDecodeError, LookupError):
                            body_html = payload.decode("utf-8", errors="replace")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    # Load whitelist and blacklist for spam analysis
    wl_result = await db.execute(
        select(SpamWhitelist).where(SpamWhitelist.account_id == account_id)
    )
    _wl_entries = {e.value for e in wl_result.scalars().all()}
    bl_result = await db.execute(
        select(SpamBlacklist).where(SpamBlacklist.account_id == account_id)
    )
    _bl_entries = {e.value for e in bl_result.scalars().all()}

    analysis = _spam_analysis(msg, flags_str, subject=subject, from_addr=from_addr, whitelist=_wl_entries, blacklist=_bl_entries)
    spam = analysis["is_spam"]
    spam_score = str(analysis["score"])
    spam_status_full = ", ".join(analysis["reasons"]) if analysis["reasons"] else ""

    # Extract technical headers for detail view
    _tech_header_names = [
        "Return-Path", "Received", "Authentication-Results",
        "Received-SPF", "DKIM-Signature", "ARC-Authentication-Results",
        "X-Spam-Status", "X-Spam-Flag", "X-Spam-Score",
        "X-VR-SPAMSCORE", "X-Mailer", "User-Agent",
        "Content-Type", "MIME-Version", "X-Originating-IP",
        "X-MS-Exchange-Organization-SCL",
    ]
    tech_headers = []
    for hname in _tech_header_names:
        values = msg.get_all(hname)
        if values:
            for v in values:
                tech_headers.append({"name": hname, "value": str(v).strip()})

    return {
        "uid": uid,
        "folder": folder,
        "from": from_addr,
        "to": to_addr,
        "cc": cc_addr,
        "reply_to": reply_to,
        "subject": subject,
        "date": date_str,
        "message_id": message_id,
        "in_reply_to": in_reply_to,
        "references": references,
        "priority": priority,
        "seen": seen,
        "flagged": flagged,
        "answered": answered,
        "spam": spam,
        "spam_score": spam_score,
        "spam_details": spam_status_full,
        "body_text": body_text,
        "body_html": body_html,
        "attachments": attachments,
        "tech_headers": tech_headers,
    }


# ---------------------------------------------------------------------------
# Email actions: send, draft, flags, move, delete, attachments
# ---------------------------------------------------------------------------

@router.post("/{account_id}/send")
async def send_email(
    account_id: int,
    req: SendEmailRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Send an email via SMTP and save to Sent folder."""
    account = await _get_account(account_id, user, db)

    if not account.smtp_host:
        raise HTTPException(status_code=400, detail="Aucun serveur SMTP configure pour ce compte")

    from src.security import decrypt_value as _dec
    smtp_password = _dec(account.smtp_password_encrypted) if account.smtp_password_encrypted else _dec(account.imap_password_encrypted)
    smtp_user = account.smtp_user or account.imap_user
    from_addr = account.imap_user

    raw_msg = _build_mime_message(
        from_addr=from_addr,
        to=req.to,
        cc=req.cc,
        bcc=req.bcc,
        subject=req.subject,
        body_text=req.body_text,
        body_html=req.body_html,
        attachments=req.attachments,
        in_reply_to=req.in_reply_to,
        references=req.references,
        priority=req.priority,
        request_read_receipt=req.request_read_receipt,
        request_delivery_receipt=req.request_delivery_receipt,
        from_display=from_addr,
    )

    import smtplib
    import ssl as ssl_mod
    all_recipients = list(req.to) + list(req.cc) + list(req.bcc)
    try:
        smtp_ssl = getattr(account, 'smtp_ssl', True)
        if account.smtp_port in (465,) or (smtp_ssl and account.smtp_port != 587):
            server = smtplib.SMTP_SSL(account.smtp_host, account.smtp_port, timeout=30)
        else:
            server = smtplib.SMTP(account.smtp_host, account.smtp_port, timeout=30)
            server.starttls(context=ssl_mod.create_default_context())
        server.login(smtp_user, smtp_password)
        server.sendmail(from_addr, all_recipients, raw_msg.as_string())
        server.quit()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Erreur SMTP: {e}")

    # Save to Sent folder
    try:
        from src.imap.manager import IMAPManager, IMAPConfig
        config = IMAPConfig(
            host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
            user=account.imap_user, password=_dec(account.imap_password_encrypted),
        )
        with IMAPManager(config) as imap:
            imap.save_to_sent(raw_msg.as_bytes())
    except Exception:
        pass

    return {"status": "sent", "message_id": raw_msg["Message-ID"]}


@router.post("/{account_id}/save-draft")
async def save_draft(
    account_id: int,
    req: SaveDraftRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Save a draft email to the Drafts IMAP folder."""
    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec

    from_addr = account.imap_user
    raw_msg = _build_mime_message(
        from_addr=from_addr, to=req.to, cc=req.cc, bcc=req.bcc,
        subject=req.subject, body_text=req.body_text, body_html=req.body_html,
        attachments=req.attachments, priority=req.priority,
        in_reply_to=req.in_reply_to, references=req.references,
    )

    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )
    try:
        with IMAPManager(config) as imap:
            ok = imap.save_draft(raw_msg.as_bytes())
        if not ok:
            raise HTTPException(status_code=502, detail="Impossible de sauvegarder le brouillon")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    return {"status": "draft_saved"}


@router.post("/{account_id}/message/{uid}/flags")
async def update_flags(
    account_id: int,
    uid: str,
    req: FlagRequest,
    folder: str = Query(...),
    storage: str = Query("imap"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Add or remove a flag on an email."""
    if storage == "local":
        email_id = int(uid.replace("L", ""))
        result = await db.execute(select(LocalEmail).where(LocalEmail.id == email_id))
        em = result.scalar_one_or_none()
        if not em:
            raise HTTPException(status_code=404, detail="Email not found")
        flag_map = {"seen": "seen", "read": "seen", "flagged": "flagged", "important": "flagged", "answered": "answered"}
        attr = flag_map.get(req.flag.lower())
        if attr:
            setattr(em, attr, req.action == "add")
            await db.commit()
        return {"status": "ok", "flag": req.flag, "action": req.action}

    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )
    try:
        with IMAPManager(config) as imap:
            if req.action == "add":
                ok = imap.flag_email(uid, folder, req.flag)
            else:
                ok = imap.unflag_email(uid, folder, req.flag)
        if not ok:
            raise HTTPException(status_code=502, detail="Flag operation failed")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    return {"status": "ok", "flag": req.flag, "action": req.action}


@router.post("/{account_id}/message/{uid}/move")
async def move_message(
    account_id: int,
    uid: str,
    req: MoveRequest,
    folder: str = Query(...),
    storage: str = Query("imap"),
    target_storage: str = Query("imap"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Move an email to another folder (supports imap/local cross-moves)."""
    if storage == "local" and target_storage == "local":
        email_id = int(uid.replace("L", ""))
        target_folder_result = await db.execute(
            select(LocalFolder).where(LocalFolder.account_id == account_id, LocalFolder.path == req.target_folder)
        )
        target = target_folder_result.scalar_one_or_none()
        if not target:
            raise HTTPException(status_code=404, detail="Target local folder not found")
        result = await db.execute(select(LocalEmail).where(LocalEmail.id == email_id))
        em = result.scalar_one_or_none()
        if not em:
            raise HTTPException(status_code=404, detail="Email not found")
        em.folder_id = target.id
        await db.commit()
        return {"status": "moved", "target_folder": req.target_folder}

    elif storage == "imap" and target_storage == "local":
        account = await _get_account(account_id, user, db)
        from src.imap.manager import IMAPManager, IMAPConfig
        from src.security import decrypt_value as _dec
        config = IMAPConfig(host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
                            user=account.imap_user, password=_dec(account.imap_password_encrypted))
        target_folder_result = await db.execute(
            select(LocalFolder).where(LocalFolder.account_id == account_id, LocalFolder.path == req.target_folder)
        )
        target = target_folder_result.scalar_one_or_none()
        if not target:
            raise HTTPException(status_code=404, detail="Target local folder not found")
        with IMAPManager(config) as imap:
            raw = imap.fetch_raw(uid, folder)
            if not raw:
                raise HTTPException(status_code=404, detail="Message not found on IMAP")
            import email as email_mod, email.utils
            msg = email_mod.message_from_bytes(raw)
            body_text, body_html, has_att = _parse_email_body(msg)
            date_val = None
            try:
                date_val = email.utils.parsedate_to_datetime(msg.get("Date", ""))
            except Exception:
                pass
            local_email = LocalEmail(
                folder_id=target.id,
                message_id_header=msg.get("Message-ID", ""),
                from_addr=_decode_header(msg.get("From", "")),
                to_addr=_decode_header(msg.get("To", "")),
                cc_addr=_decode_header(msg.get("Cc", "")),
                subject=_decode_header(msg.get("Subject", "")),
                date=date_val, seen=True, has_attachments=has_att,
                body_text=body_text, body_html=body_html,
                raw_message=raw,
            )
            db.add(local_email)
            await db.commit()
            imap.delete_email(uid, folder)
        return {"status": "moved", "target_folder": req.target_folder}

    elif storage == "local" and target_storage == "imap":
        email_id = int(uid.replace("L", ""))
        result = await db.execute(select(LocalEmail).where(LocalEmail.id == email_id))
        em = result.scalar_one_or_none()
        if not em or not em.raw_message:
            raise HTTPException(status_code=404, detail="Email not found or no raw data")
        account = await _get_account(account_id, user, db)
        from src.imap.manager import IMAPManager, IMAPConfig
        from src.security import decrypt_value as _dec
        config = IMAPConfig(host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
                            user=account.imap_user, password=_dec(account.imap_password_encrypted))
        import time
        with IMAPManager(config) as imap:
            imap._conn.create(_imap_quote(req.target_folder))
            status, _ = imap._conn.append(
                _imap_quote(req.target_folder), "\\Seen",
                imaplib.Time2Internaldate(time.time()), em.raw_message
            )
            if status != "OK":
                raise HTTPException(status_code=502, detail="IMAP append failed")
        await db.delete(em)
        await db.commit()
        return {"status": "moved", "target_folder": req.target_folder}

    # Default: imap -> imap
    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )
    try:
        with IMAPManager(config) as imap:
            ok = imap.move_email(uid, folder, req.target_folder)
        if not ok:
            raise HTTPException(status_code=502, detail="Move failed")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    return {"status": "moved", "target_folder": req.target_folder}


@router.delete("/{account_id}/message/{uid}")
async def delete_message(
    account_id: int,
    uid: str,
    folder: str = Query(...),
    storage: str = Query("imap"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete an email (move to Trash or delete from local DB)."""
    if storage == "local":
        email_id = int(uid.replace("L", ""))
        result = await db.execute(select(LocalEmail).where(LocalEmail.id == email_id))
        em = result.scalar_one_or_none()
        if not em:
            raise HTTPException(status_code=404, detail="Email not found")
        await db.delete(em)
        await db.commit()
        return {"status": "deleted"}

    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )
    try:
        with IMAPManager(config) as imap:
            ok = imap.delete_email(uid, folder)
        if not ok:
            raise HTTPException(status_code=502, detail="Delete failed")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    return {"status": "deleted"}


@router.post("/{account_id}/delete-bulk")
async def delete_bulk(
    account_id: int,
    req: BulkDeleteRequest,
    storage: str = Query("imap"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete multiple emails in one batch (single IMAP connection)."""
    if storage == "local":
        deleted = 0
        for uid in req.uids:
            email_id = int(uid.replace("L", ""))
            result = await db.execute(select(LocalEmail).where(LocalEmail.id == email_id))
            em = result.scalar_one_or_none()
            if em:
                await db.delete(em)
                deleted += 1
        await db.commit()
        return {"deleted": deleted, "failed": len(req.uids) - deleted}

    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )
    try:
        with IMAPManager(config) as imap:
            result = imap.delete_emails_bulk(req.uids, req.folder)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    return result


@router.get("/{account_id}/message/{uid}/attachment/{index}")
async def download_attachment(
    account_id: int,
    uid: str,
    index: int,
    folder: str = Query(...),
    storage: str = Query("imap"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Download an attachment by index from an email."""
    if storage == "local":
        email_id = int(uid.replace("L", ""))
        result = await db.execute(select(LocalEmail).where(LocalEmail.id == email_id))
        em = result.scalar_one_or_none()
        if not em or not em.raw_message:
            raise HTTPException(status_code=404, detail="Email not found or no raw data")
        import email as email_mod
        msg = email_mod.message_from_bytes(em.raw_message)
        att_idx = 0
        for part in msg.walk():
            disp = str(part.get("Content-Disposition", ""))
            if "attachment" in disp:
                if att_idx == index:
                    payload = part.get_payload(decode=True) or b""
                    filename = _decode_header(part.get_filename() or "unnamed")
                    content_type = part.get_content_type()
                    from urllib.parse import quote
                    filename_encoded = quote(filename)
                    return Response(
                        content=payload,
                        media_type=content_type,
                        headers={
                            "Content-Disposition": f"attachment; filename*=UTF-8''{filename_encoded}",
                        },
                    )
                att_idx += 1
        raise HTTPException(status_code=404, detail="Attachment not found")

    account = await _get_account(account_id, user, db)
    from src.imap.manager import IMAPManager, IMAPConfig
    from src.security import decrypt_value as _dec
    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )
    try:
        with IMAPManager(config) as imap:
            att = imap.get_attachment_data(uid, folder, index)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    if not att:
        raise HTTPException(status_code=404, detail="Attachment not found")

    from urllib.parse import quote
    filename_encoded = quote(att["filename"])
    return Response(
        content=att["data"],
        media_type=att["content_type"],
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{filename_encoded}",
        },
    )


class CreateLocalFolderRequest(BaseModel):
    name: str
    parent_path: str | None = None


@router.post("/{account_id}/local-folders")
async def create_local_folder(account_id: int, req: CreateLocalFolderRequest,
                               user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    await _get_account(account_id, user, db)
    path = f"{req.parent_path}/{req.name}" if req.parent_path else req.name
    folder = LocalFolder(account_id=account_id, name=req.name, path=path, parent_path=req.parent_path)
    db.add(folder)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise HTTPException(status_code=409, detail="Folder already exists")
    return {"id": folder.id, "name": folder.name, "path": folder.path, "storage": "local"}


@router.delete("/{account_id}/local-folders/{folder_id}")
async def delete_local_folder(account_id: int, folder_id: int,
                               user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    await _get_account(account_id, user, db)
    result = await db.execute(select(LocalFolder).where(LocalFolder.id == folder_id, LocalFolder.account_id == account_id))
    folder = result.scalar_one_or_none()
    if not folder:
        raise HTTPException(status_code=404, detail="Folder not found")
    await db.delete(folder)
    await db.commit()
    return {"status": "deleted"}


@router.post("/{account_id}/import-mbox")
async def import_mbox(
    account_id: int,
    file: UploadFile = File(...),
    folder: str | None = Query(None, description="Target IMAP folder (if empty, derive from file)"),
    storage: str = Query("imap"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Import emails from mbox/ZIP. Streams upload to disk, runs import in background."""
    import os, shutil, threading
    from src.import_jobs import create_job, update_job, get_job_file_dir

    account = await _get_account(account_id, user, db)

    config = None
    if storage == "imap":
        from src.imap.manager import IMAPManager, IMAPConfig
        from src.security import decrypt_value as _dec
        config = IMAPConfig(
            host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
            user=account.imap_user, password=_dec(account.imap_password_encrypted),
        )

    filename = file.filename or "upload.mbox"
    job = create_job(user.id, account_id, filename, source="upload")
    job_dir = get_job_file_dir(job["id"])
    file_path = str(job_dir / filename)

    try:
        with open(file_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
    except Exception as e:
        update_job(job["id"], status="error", error=f"Upload failed: {e}")
        shutil.rmtree(job_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")

    update_job(job["id"], status="queued")

    is_zip = filename.lower().endswith(".zip")
    if not is_zip:
        with open(file_path, "rb") as f:
            is_zip = f.read(4) == b"PK\x03\x04"

    threading.Thread(
        target=_run_import_job,
        args=(job["id"], config, file_path, is_zip, folder, None, storage, account_id),
        daemon=True,
    ).start()

    return {"job_id": job["id"], "status": "queued"}


@router.post("/{account_id}/import-path")
async def import_from_path(
    account_id: int,
    path: str = Query(..., description="Server filesystem path to mbox or ZIP file"),
    folder: str | None = Query(None, description="Target IMAP folder"),
    storage: str = Query("imap"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Import from a file already on the server. Runs in background."""
    import os, threading
    from src.import_jobs import create_job, update_job

    account = await _get_account(account_id, user, db)

    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Only admins can import from server paths")

    if not os.path.isfile(path):
        raise HTTPException(status_code=400, detail=f"File not found: {path}")

    config = None
    if storage == "imap":
        from src.imap.manager import IMAPManager, IMAPConfig
        from src.security import decrypt_value as _dec
        config = IMAPConfig(
            host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
            user=account.imap_user, password=_dec(account.imap_password_encrypted),
        )

    filename = os.path.basename(path)
    job = create_job(user.id, account_id, filename, source="path")
    update_job(job["id"], status="queued")

    is_zip = filename.lower().endswith(".zip")
    if not is_zip:
        with open(path, "rb") as f:
            is_zip = f.read(4) == b"PK\x03\x04"

    threading.Thread(
        target=_run_import_job,
        args=(job["id"], config, path, is_zip, folder, None, storage, account_id),
        daemon=True,
    ).start()

    return {"job_id": job["id"], "status": "queued"}


@router.post("/{account_id}/import-resume/{job_id}")
async def resume_import(
    account_id: int,
    job_id: str,
    storage: str = Query("imap"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Resume an interrupted import job."""
    import os, threading
    from src.import_jobs import get_job, update_job, get_job_file_dir

    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["user_id"] != user.id:
        raise HTTPException(status_code=403, detail="Not your job")
    if job["status"] not in ("interrupted", "error"):
        raise HTTPException(status_code=400, detail=f"Job status is {job['status']}, cannot resume")

    account = await _get_account(account_id, user, db)

    config = None
    if storage == "imap":
        from src.imap.manager import IMAPManager, IMAPConfig
        from src.security import decrypt_value as _dec
        config = IMAPConfig(
            host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
            user=account.imap_user, password=_dec(account.imap_password_encrypted),
        )

    # Find the file
    if job["source"] == "path":
        file_path = None
        for f in get_job_file_dir(job_id).iterdir():
            file_path = str(f)
            break
        if not file_path:
            raise HTTPException(status_code=400, detail="Source file no longer available")
    else:
        job_dir = get_job_file_dir(job_id)
        files = list(job_dir.iterdir())
        if not files:
            raise HTTPException(status_code=400, detail="Uploaded file no longer available. Please re-upload.")
        file_path = str(files[0])

    is_zip = job["filename"].lower().endswith(".zip")
    if not is_zip:
        with open(file_path, "rb") as f:
            is_zip = f.read(4) == b"PK\x03\x04"

    # Skip already-processed folders
    done_folders = {f["folder"] for f in job.get("folders_done", [])}
    update_job(job_id, status="queued")

    threading.Thread(
        target=_run_import_job,
        args=(job_id, config, file_path, is_zip, None, done_folders, storage, account_id),
        daemon=True,
    ).start()

    return {"job_id": job_id, "status": "resumed"}


# Import job status endpoints (mounted at router level for convenience)
@router.get("/import-jobs")
async def list_import_jobs_fallback(
    user: User = Depends(get_current_user),
):
    """List import jobs for the current user."""
    from src.import_jobs import list_jobs
    return list_jobs(user.id)


@router.get("/import-jobs/{job_id}")
async def get_import_job_fallback(
    job_id: str,
    user: User = Depends(get_current_user),
):
    """Get import job status."""
    from src.import_jobs import get_job
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["user_id"] != user.id:
        raise HTTPException(status_code=403, detail="Not your job")
    return job


def _run_import_job(job_id: str, config, file_path: str, is_zip: bool, folder: str | None,
                    skip_folders: set | None = None, storage: str = "imap", account_id: int | None = None):
    """Background thread: run the import and update job status."""
    from src.import_jobs import update_job, add_folder_done, cleanup_job_files
    import os

    try:
        update_job(job_id, status="importing")

        if storage == "local":
            if is_zip:
                _run_zip_import_local(job_id, account_id, file_path, skip_folders, folder)
            else:
                _run_single_import_local(job_id, account_id, file_path, os.path.basename(file_path), folder)
        else:
            if is_zip:
                _run_zip_import(job_id, config, file_path, skip_folders, folder)
            else:
                _run_single_import(job_id, config, file_path, os.path.basename(file_path), folder)

    except Exception as e:
        logger.exception(f"Import job {job_id} failed: {e}")
        update_job(job_id, status="error", error=str(e))


def _run_single_import(job_id: str, config, file_path: str, filename: str, folder: str | None):
    """Import a single mbox file, updating job progress."""
    import mailbox, os
    from src.import_jobs import update_job, add_folder_done
    from src.imap.manager import IMAPManager

    if not folder:
        basename = os.path.splitext(filename or "INBOX")[0]
        folder = _thunderbird_folder_name(basename)

    total_emails = _count_mbox_messages(file_path)
    update_job(job_id, progress={"total": total_emails, "current_folder": folder})

    mbox = mailbox.mbox(file_path)
    imported = skipped = errors = current = 0

    with IMAPManager(config) as imap:
        _ensure_imap_folder(imap, folder)
        existing_msgids = _fetch_existing_msgids(imap, folder)

        for msg in mbox:
            result = _import_one_message(imap, msg, folder, existing_msgids)
            imported += result[0]
            skipped += result[1]
            errors += result[2]
            current += 1
            if current % 10 == 0 or current == total_emails:
                update_job(job_id, progress={
                    "current": current, "total": total_emails,
                    "imported": imported, "skipped": skipped, "errors": errors,
                    "current_folder": folder,
                })
    mbox.close()

    add_folder_done(job_id, {"folder": folder, "imported": imported, "skipped": skipped, "errors": errors})
    update_job(job_id, status="done", progress={
        "current": current, "total": total_emails,
        "imported": imported, "skipped": skipped, "errors": errors,
        "current_folder": "",
    })


def _run_zip_import(job_id: str, config, zip_path: str, skip_folders: set | None = None,
                    target_folder: str | None = None):
    """Import a Thunderbird ZIP profile, updating job progress.

    If target_folder is set, the entire tree is nested under it
    (e.g. target_folder="Archives" => "Client/PJS" becomes "Archives/Client/PJS").
    Existing emails are preserved (dedup by Message-ID = merge).
    """
    import mailbox, tempfile, zipfile, shutil, os
    from src.import_jobs import update_job, add_folder_done
    from src.imap.manager import IMAPManager

    update_job(job_id, status="extracting")
    tmp_dir = tempfile.mkdtemp(prefix="tb_import_")
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmp_dir)

        mbox_files = _discover_mbox_files(tmp_dir)
        if not mbox_files:
            update_job(job_id, status="error", error="No mbox files found in ZIP archive")
            return

        total_emails = sum(_count_mbox_messages(p) for p, _ in mbox_files)
        update_job(job_id, status="importing", progress={"total": total_emails})

        total_imported = total_skipped = total_errors = global_current = 0

        with IMAPManager(config) as imap:
            for mbox_path, folder_name in mbox_files:
                # Prefix with target folder if specified
                effective_folder = f"{target_folder}/{folder_name}" if target_folder else folder_name
                if skip_folders and effective_folder in skip_folders:
                    logger.info(f"Job {job_id}: skipping already-done folder {effective_folder}")
                    continue
                try:
                    _ensure_imap_folder(imap, effective_folder)
                    existing_msgids = _fetch_existing_msgids(imap, effective_folder)

                    mbox = mailbox.mbox(mbox_path)
                    f_imported = f_skipped = f_errors = 0
                    for msg in mbox:
                        result = _import_one_message(imap, msg, effective_folder, existing_msgids)
                        f_imported += result[0]
                        f_skipped += result[1]
                        f_errors += result[2]
                        global_current += 1
                        if global_current % 10 == 0:
                            update_job(job_id, progress={
                                "current": global_current, "total": total_emails,
                                "imported": total_imported + f_imported,
                                "skipped": total_skipped + f_skipped,
                                "errors": total_errors + f_errors,
                                "current_folder": effective_folder,
                            })
                    mbox.close()

                    total_imported += f_imported
                    total_skipped += f_skipped
                    total_errors += f_errors
                    add_folder_done(job_id, {
                        "folder": effective_folder,
                        "imported": f_imported, "skipped": f_skipped, "errors": f_errors,
                    })
                except Exception as e:
                    logger.warning(f"Job {job_id}: failed folder {effective_folder}: {e}")
                    total_errors += 1
                    add_folder_done(job_id, {
                        "folder": effective_folder, "imported": 0, "skipped": 0,
                        "errors": 1, "error_detail": str(e),
                    })

        update_job(job_id, status="done", progress={
            "current": global_current, "total": total_emails,
            "imported": total_imported, "skipped": total_skipped,
            "errors": total_errors, "current_folder": "",
        })
    except Exception as e:
        update_job(job_id, status="error", error=str(e))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _count_mbox_messages(path: str) -> int:
    """Quick count of messages in an mbox file by counting 'From ' lines."""
    count = 0
    try:
        with open(path, "rb") as f:
            for line in f:
                if line.startswith(b"From "):
                    count += 1
    except Exception:
        pass
    return count


# --- Import helpers ---

_TB_FOLDER_MAP = {
    "inbox": "INBOX",
    "sent": "Sent",
    "drafts": "Drafts",
    "trash": "Trash",
    "junk": "Junk",
    "spam": "Junk",
    "archives": "Archives",
    "templates": "Templates",
}


def _thunderbird_folder_name(name: str) -> str:
    return _TB_FOLDER_MAP.get(name.lower(), name)


def _discover_mbox_files(root_dir: str) -> list[tuple[str, str]]:
    """Walk a Thunderbird profile directory and return (filepath, imap_folder) pairs.

    Thunderbird structure:
    - Each folder is a file *without* extension (e.g. ``Inbox``, ``Sent``)
    - ``.msf`` files are index files (ignored)
    - ``.sbd`` directories contain subfolders
    - ``Mail/Local Folders/Name-user@domain`` are local copies (strip suffix)
    """
    import os, re

    IGNORE_EXT = {".msf", ".dat", ".json", ".html", ".js", ".css", ".sqlite",
                  ".sqlite-journal", ".log", ".png", ".jpg", ".gif", ".zip"}
    IGNORE_FILES = {"msgFilterRules.dat", "filterlog.html", "virtualFolders.dat",
                    "junkmail.html", "popstate.dat"}
    # Pattern to strip "-user@domain" suffix from Local Folders files
    EMAIL_SUFFIX_RE = re.compile(r'-[^-]+@[^-]+\.[a-z]{2,}$', re.IGNORECASE)

    results = []

    # Find mail roots: ImapMail/, Mail/, Local Folders/
    imap_roots = []
    local_roots = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        basename = os.path.basename(dirpath)
        if basename == "ImapMail":
            imap_roots.append(dirpath)
        elif basename in ("Mail", "Local Folders"):
            local_roots.append(dirpath)
        # Direct mbox files at root
        if dirpath == root_dir:
            for fn in filenames:
                ext = os.path.splitext(fn)[1].lower()
                if ext not in IGNORE_EXT and fn not in IGNORE_FILES:
                    fpath = os.path.join(dirpath, fn)
                    if _looks_like_mbox(fpath):
                        results.append((fpath, _thunderbird_folder_name(fn)))

    if not imap_roots and not local_roots and results:
        return results

    # If no standard TB structure, detect server hostname dirs
    # (directories whose name contains a dot, like imap.server.com, mail.domain.fr)
    HOSTNAME_RE = re.compile(r'^[a-zA-Z0-9._-]+\.[a-zA-Z]{2,}$')
    if not imap_roots and not local_roots:
        for dirpath, dirnames, _fns in os.walk(root_dir):
            for d in dirnames:
                if HOSTNAME_RE.match(d):
                    imap_roots.append(os.path.join(dirpath, d))
            break  # only scan first two levels
        if not imap_roots:
            # Check one level deeper
            for dirpath, dirnames, _fns in os.walk(root_dir):
                if dirpath == root_dir:
                    continue
                for d in dirnames:
                    if HOSTNAME_RE.match(d):
                        imap_roots.append(os.path.join(dirpath, d))
                break

    # Prefer ImapMail if available
    mail_roots = imap_roots or local_roots or [root_dir]

    seen_folders = set()
    for mail_root in mail_roots:
        for dirpath, dirnames, filenames in os.walk(mail_root):
            for fn in filenames:
                ext = os.path.splitext(fn)[1].lower()
                if ext in IGNORE_EXT or fn in IGNORE_FILES:
                    continue

                fpath = os.path.join(dirpath, fn)
                if not _looks_like_mbox(fpath):
                    continue

                # Build folder path from filesystem path relative to mail_root
                rel = os.path.relpath(dirpath, mail_root)
                parts = []
                for segment in rel.split(os.sep):
                    if segment == ".":
                        continue
                    if segment.lower().endswith(".sbd"):
                        segment = segment[:-4]
                    # Skip "Local Folders" and server hostnames (contain dots)
                    if segment in ("Local Folders",) or (not parts and "." in segment):
                        continue
                    parts.append(segment)

                # Strip "-user@domain" suffix from Local Folders filenames
                clean_fn = EMAIL_SUFFIX_RE.sub('', fn)
                parts.append(clean_fn)
                # INBOX subfolders use "." separator (INBOX.Drafts, INBOX.Sent, etc.)
                # All other folders use "/" (the standard IMAP hierarchy separator)
                sep = "." if parts and parts[0] == "INBOX" else "/"
                imap_folder = sep.join(_thunderbird_folder_name(p) for p in parts)

                # Skip duplicates (Local Folders copies of IMAP folders)
                if imap_folder in seen_folders:
                    continue
                seen_folders.add(imap_folder)

                results.append((fpath, imap_folder))

    return results


def _looks_like_mbox(filepath: str) -> bool:
    """Quick check: mbox files start with 'From ' on the first line."""
    try:
        with open(filepath, "rb") as f:
            first = f.read(5)
            return first == b"From "
    except Exception:
        return False


def _ensure_imap_folder(imap, folder: str):
    """Select the folder, creating it if needed. Handles non-ASCII names via IMAP UTF-7."""
    from src.imap.manager import _encode_imap_utf7
    encoded = _encode_imap_utf7(folder)
    try:
        st, _ = imap._conn.select(_imap_quote(encoded))
        if st == "OK":
            return
    except Exception:
        pass
    try:
        imap._conn.create(_imap_quote(encoded))
        imap._conn.subscribe(_imap_quote(encoded))
    except Exception:
        pass
    imap._conn.select(_imap_quote(encoded))


def _fetch_existing_msgids(imap, folder: str) -> set:
    """Fetch all Message-ID headers from a folder for deduplication."""
    existing = set()
    try:
        status, data = imap._conn.search(None, "ALL")
        if status == "OK" and data[0]:
            uids = data[0].split()
            # Fetch in batches to avoid command line too long
            batch_size = 500
            for i in range(0, len(uids), batch_size):
                batch = uids[i:i + batch_size]
                uid_range = b",".join(batch)
                status, hdr_data = imap._conn.fetch(
                    uid_range.decode(), "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])"
                )
                if status == "OK":
                    for item in hdr_data:
                        if isinstance(item, tuple) and len(item) > 1:
                            hdr = item[1].decode("utf-8", errors="replace")
                            for line in hdr.splitlines():
                                if line.lower().startswith("message-id:"):
                                    mid = line.split(":", 1)[1].strip()
                                    existing.add(mid)
    except Exception as e:
        logger.warning(f"Could not fetch existing Message-IDs for {folder}: {e}")
    return existing


def _import_one_message(imap, msg, folder: str, existing_msgids: set) -> tuple[int, int, int]:
    """Import a single message. Returns (imported, skipped, errors) counts.

    Skips fragments (messages without essential headers, caused by bad mbox splitting).
    """
    import time
    import email.utils
    from src.imap.manager import _encode_imap_utf7

    try:
        # Skip fragments from bad mbox splitting (no From/Date = not a real message)
        has_from = msg.get("From")
        has_date = msg.get("Date")
        has_received = msg.get("Received")
        if not has_from and not has_date and not has_received:
            return (0, 1, 0)  # count as skipped

        msg_id = msg.get("Message-ID", "").strip()
        if msg_id and msg_id in existing_msgids:
            return (0, 1, 0)

        raw = msg.as_bytes()
        imap_date = imaplib.Time2Internaldate(time.time())
        date_str = msg.get("Date")
        if date_str:
            parsed = email.utils.parsedate_tz(date_str)
            if parsed:
                imap_date = imaplib.Time2Internaldate(email.utils.mktime_tz(parsed))

        encoded_folder = _encode_imap_utf7(folder)
        status, _ = imap._conn.append(
            _imap_quote(encoded_folder), "\\Seen", imap_date, raw,
        )
        if status == "OK":
            if msg_id:
                existing_msgids.add(msg_id)
            return (1, 0, 0)
        return (0, 0, 1)
    except Exception as e:
        logger.warning(f"Failed to import message into {folder}: {e}")
        return (0, 0, 1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_mime_message(
    from_addr: str,
    to: list[str],
    cc: list[str] = None,
    bcc: list[str] = None,
    subject: str = "",
    body_text: str = "",
    body_html: str = "",
    attachments: list[dict] = None,
    in_reply_to: str = None,
    references: str = None,
    priority: str = None,
    request_read_receipt: bool = False,
    request_delivery_receipt: bool = False,
    from_display: str = None,
):
    """Build a MIME email message with full headers support."""
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    import email.utils
    import base64

    msg = MIMEMultipart("mixed")
    msg["From"] = from_display or from_addr
    msg["To"] = ", ".join(to) if to else ""
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = subject
    msg["Date"] = email.utils.formatdate(localtime=True)
    msg["Message-ID"] = email.utils.make_msgid(
        domain=from_addr.split("@")[-1] if "@" in from_addr else "mailia"
    )
    msg["MIME-Version"] = "1.0"

    # Threading headers
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = references or in_reply_to

    # Priority headers
    if priority == "high":
        msg["X-Priority"] = "1"
        msg["Importance"] = "High"
        msg["X-MSMail-Priority"] = "High"
    elif priority == "low":
        msg["X-Priority"] = "5"
        msg["Importance"] = "Low"
        msg["X-MSMail-Priority"] = "Low"

    # Read receipt (MDN)
    if request_read_receipt:
        msg["Disposition-Notification-To"] = from_addr

    # Delivery receipt (DSN)
    if request_delivery_receipt:
        msg["Return-Receipt-To"] = from_addr

    # Body
    if body_html:
        alt = MIMEMultipart("alternative")
        if body_text:
            alt.attach(MIMEText(body_text, "plain", "utf-8"))
        alt.attach(MIMEText(body_html, "html", "utf-8"))
        msg.attach(alt)
    else:
        msg.attach(MIMEText(body_text or "", "plain", "utf-8"))

    # Attachments
    for att in (attachments or []):
        try:
            file_data = base64.b64decode(att.get("data_base64", ""))
        except Exception:
            continue
        part = MIMEBase("application", "octet-stream")
        part.set_payload(file_data)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename=att.get("filename", "file"))
        msg.attach(part)

    return msg


async def _get_account(account_id: int, user: User, db: AsyncSession) -> MailAccount:
    """Get a mail account, raising 404 if not found."""
    result = await db.execute(
        select(MailAccount).where(MailAccount.id == account_id, MailAccount.user_id == user.id)
    )
    account = result.scalar_one_or_none()
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    return account


_DISPOSABLE_DOMAINS = {
    "mailinator.com", "guerrillamail.com", "tempmail.com", "throwaway.email",
    "yopmail.com", "sharklasers.com", "guerrillamailblock.com", "grr.la",
    "dispostable.com", "maildrop.cc", "10minutemail.com", "trashmail.com",
    "fakeinbox.com", "tempail.com", "mailnesia.com", "temp-mail.org",
    "getnada.com", "emailondeck.com", "mohmal.com", "burnermail.io",
}

_SPAM_SUBJECT_PATTERNS = None

def _get_spam_subject_patterns():
    global _SPAM_SUBJECT_PATTERNS
    if _SPAM_SUBJECT_PATTERNS is None:
        import re
        _SPAM_SUBJECT_PATTERNS = [
            (re.compile(r'\b(viagra|cialis|pharmacy|pills?|medication|prescription)\b', re.I), "pharma_keywords", 3.0),
            (re.compile(r'\b(lottery|winner|congratulations|prize|won|jackpot)\b', re.I), "lottery_scam", 3.0),
            (re.compile(r'\b(nigerian?|prince|inheritance|beneficiary|next.of.kin)\b', re.I), "419_scam", 3.5),
            (re.compile(r'\b(bitcoin|crypto|trading.signals?|guaranteed.returns?|invest.now)\b', re.I), "crypto_scam", 2.0),
            (re.compile(r'\b(urgent|immediate|act.now|limited.time|expires?.today|last.chance|don\'?t.miss)\b', re.I), "urgency", 1.0),
            (re.compile(r'\b(click.here|verify.your.account|confirm.your|update.your.payment|suspended)\b', re.I), "phishing", 2.5),
            (re.compile(r'\b(unsubscribe|bulk|mass.mail|dear.customer|dear.user|dear.friend)\b', re.I), "bulk_patterns", 0.5),
            (re.compile(r'[A-Z\s]{15,}', re.A), "excessive_caps", 1.5),
            (re.compile(r'[!]{3,}'), "excessive_exclamation", 1.5),
            (re.compile(r'[$€£¥]\s*\d[\d,.]+\s*(million|thousand|USD|EUR)', re.I), "money_mention", 2.0),
            (re.compile(r'(https?://)?(bit\.ly|tinyurl|t\.co|goo\.gl|is\.gd|buff\.ly|ow\.ly|rebrand\.ly)/\S+', re.I), "shortened_url", 1.5),
        ]
    return _SPAM_SUBJECT_PATTERNS


def _spam_analysis(msg, flags_str: str = "", subject: str = "", from_addr: str = "", whitelist: set = None, blacklist: set = None) -> dict:
    """Full spam analysis returning score, reasons, and verdict."""
    import re
    score = 0.0
    reasons = []

    # Extract sender info for whitelist/blacklist checks
    sender = from_addr or _decode_header(msg.get("From", "") or "")
    sender_lower = sender.lower()
    email_match = re.search(r'[\w.+-]+@[\w.-]+', sender_lower)
    sender_email = email_match.group(0) if email_match else sender_lower
    sender_domain = sender_email.split("@")[-1] if "@" in sender_email else ""

    # Check whitelist — if sender matches, skip all analysis
    if whitelist:
        for entry in whitelist:
            if entry == sender_email or entry == sender_domain:
                return {"is_spam": False, "score": 0, "reasons": ["whitelisted"]}

    # Check blacklist — if sender matches, force spam
    if blacklist:
        for entry in blacklist:
            if entry == sender_email or entry == sender_domain:
                return {"is_spam": True, "score": 10, "reasons": ["blacklisted"]}

    # --- Server-side signals (high confidence) ---

    # 1. IMAP $Junk keyword
    if "$Junk" in flags_str or "Junk" in flags_str:
        score += 5.0
        reasons.append("imap_junk_flag")

    # 2. X-Spam-Flag header
    spam_flag = (msg.get("X-Spam-Flag", "") or "").strip().upper()
    if spam_flag in ("YES", "TRUE"):
        score += 5.0
        reasons.append("x_spam_flag")

    # 3. X-Spam-Status header (SpamAssassin)
    spam_status = (msg.get("X-Spam-Status", "") or "").strip()
    if spam_status.upper().startswith("YES"):
        score += 5.0
        reasons.append("spamassassin_yes")

    # 4. X-Spam-Score
    try:
        sa_score = float((msg.get("X-Spam-Score", "0") or "0").strip())
        if sa_score >= 5.0:
            score += sa_score
            reasons.append(f"spam_score_{sa_score}")
        elif sa_score >= 3.0:
            score += sa_score * 0.5
            reasons.append(f"spam_score_warn_{sa_score}")
    except (ValueError, TypeError):
        pass

    # 5. X-VR-SPAMSCORE (OVH server-side spam score, 0-100)
    try:
        vr_score = int((msg.get("X-VR-SPAMSCORE", "0") or "0").strip())
        if vr_score >= 50:
            score += 5.0
            reasons.append(f"ovh_spam_{vr_score}")
        elif vr_score >= 30:
            score += 3.0
            reasons.append(f"ovh_suspicious_{vr_score}")
        elif vr_score >= 17:
            score += 1.5
            reasons.append(f"ovh_warn_{vr_score}")
    except (ValueError, TypeError):
        pass

    # --- Authentication checks ---

    auth_results = (msg.get("Authentication-Results", "") or "").strip()
    if auth_results:
        auth_lower = auth_results.lower()
        if "spf=fail" in auth_lower or "spf=softfail" in auth_lower:
            score += 2.0
            reasons.append("spf_fail")
        if "dkim=fail" in auth_lower:
            score += 2.0
            reasons.append("dkim_fail")
        if "dmarc=fail" in auth_lower:
            score += 2.5
            reasons.append("dmarc_fail")
        # Multiple auth failures compound
        auth_fails = sum(1 for r in reasons if r in ("spf_fail", "dkim_fail", "dmarc_fail"))
        if auth_fails >= 2:
            score += 1.5
            reasons.append("multi_auth_fail")

    # --- Subject heuristics ---

    subj = subject or _decode_header(msg.get("Subject", "") or "")
    if subj:
        for pattern, name, pts in _get_spam_subject_patterns():
            if pattern.search(subj):
                score += pts
                reasons.append(name)

    # --- Sender analysis ---

    sender = from_addr or _decode_header(msg.get("From", "") or "")
    if sender:
        # Extract domain
        domain_match = re.search(r'@([\w.-]+)', sender)
        if domain_match:
            domain = domain_match.group(1).lower()
            if domain in _DISPOSABLE_DOMAINS:
                score += 4.0
                reasons.append("disposable_domain")
        else:
            # No @ in from — suspicious
            score += 1.5
            reasons.append("no_sender_domain")

    # --- Header anomalies ---

    # Missing Message-ID (common in bulk spam)
    if not (msg.get("Message-ID", "") or "").strip():
        score += 1.0
        reasons.append("missing_message_id")

    # X-Mailer / User-Agent checks for known spam tools
    x_mailer = (msg.get("X-Mailer", "") or "").strip().lower()
    if x_mailer and any(k in x_mailer for k in ("mass", "bulk", "blast", "bomber")):
        score += 3.0
        reasons.append("spam_mailer")

    # Precedence: bulk
    precedence = (msg.get("Precedence", "") or "").strip().lower()
    if precedence == "bulk":
        score += 0.5
        reasons.append("precedence_bulk")

    is_spam = score >= 5.0
    return {"is_spam": is_spam, "score": round(score, 1), "reasons": reasons}


def _detect_spam_from_headers(msg, flags_str: str = "", whitelist: set = None, blacklist: set = None) -> bool:
    """Quick spam check — boolean wrapper around full analysis."""
    return _spam_analysis(msg, flags_str, whitelist=whitelist, blacklist=blacklist)["is_spam"]


def _decode_header(raw: str) -> str:
    """Decode an RFC2047-encoded email header."""
    import email.header
    parts = email.header.decode_header(raw)
    decoded = []
    for part, charset in parts:
        if isinstance(part, bytes):
            try:
                decoded.append(part.decode(charset or "utf-8", errors="replace"))
            except (UnicodeDecodeError, LookupError):
                decoded.append(part.decode("utf-8", errors="replace"))
        else:
            decoded.append(part)
    return " ".join(decoded)


def _select_folder(conn, folder: str, readonly: bool = True):
    """Select an IMAP folder with fallback strategies for maximum compatibility."""
    quoted = _imap_quote(folder)
    logger.debug("Selecting folder: raw=%r, quoted=%r, readonly=%s", folder, quoted, readonly)

    errors = []

    # Strategy 1: Quoted name with requested mode
    try:
        status, data = conn.select(quoted, readonly=readonly)
        if status == "OK":
            return status, data
        errors.append(f"quoted+{'EXAMINE' if readonly else 'SELECT'}: {status}")
    except imaplib.IMAP4.error as e:
        errors.append(f"quoted+{'EXAMINE' if readonly else 'SELECT'}: {e}")

    # Strategy 2: If EXAMINE failed, try SELECT (some servers have EXAMINE bugs)
    if readonly:
        try:
            status, data = conn.select(quoted, readonly=False)
            if status == "OK":
                logger.info("Fallback to SELECT worked for folder %r", folder)
                return status, data
            errors.append(f"quoted+SELECT: {status}")
        except imaplib.IMAP4.error as e:
            errors.append(f"quoted+SELECT: {e}")

    # Strategy 3: Unquoted (some servers don't want explicit quotes)
    try:
        status, data = conn.select(folder, readonly=readonly)
        if status == "OK":
            logger.info("Unquoted select worked for folder %r", folder)
            return status, data
        errors.append(f"unquoted: {status}")
    except imaplib.IMAP4.error as e:
        errors.append(f"unquoted: {e}")

    logger.error("All folder select strategies failed for %r: %s", folder, errors)
    raise Exception(f"Cannot open folder '{folder}': {'; '.join(errors)}")


def _build_folder_tree(folders: list[dict]) -> list[dict]:
    """Convert flat folder list into a nested tree.
    Each folder is {"name": "INBOX.Sent", "display_name": "...", "separator": "."}
    Preserves original IMAP folder names as path values.
    """
    if not folders:
        return []

    from src.imap.manager import _decode_imap_utf7

    sep = folders[0]["separator"]
    real_folders = {f["name"] for f in folders}
    # Map each raw segment to its decoded display name
    segment_display = {}
    for f in folders:
        parts = f["name"].split(f["separator"])
        display_parts = f.get("display_name", f["name"]).split(f["separator"])
        for raw, disp in zip(parts, display_parts):
            if raw not in segment_display:
                segment_display[raw] = _decode_imap_utf7(disp) if disp == raw else disp

    root: dict = {}
    for f in folders:
        imap_name = f["name"]
        parts = imap_name.split(f["separator"])
        node = root
        for part in parts:
            if part not in node:
                node[part] = {}
            node = node[part]

    def _to_list(d: dict, prefix: str = "", display_prefix: str = "") -> list[dict]:
        result = []
        for name, children in sorted(d.items()):
            tree_path = f"{prefix}{sep}{name}" if prefix else name
            display = segment_display.get(name, _decode_imap_utf7(name))
            display_path = f"{display_prefix}{sep}{display}" if display_prefix else display
            item = {"name": display, "path": tree_path, "display_path": display_path}
            if tree_path not in real_folders:
                item["noselect"] = True
            child_list = _to_list(children, tree_path, display_path)
            if child_list:
                item["children"] = child_list
            result.append(item)
        return result

    return _to_list(root)


def _parse_email_body(msg) -> tuple[str, str, bool]:
    """Parse email body and detect attachments. Returns (text, html, has_attachments)."""
    body_text = ""
    body_html = ""
    has_attachments = False
    for part in msg.walk():
        content_type = part.get_content_type()
        disposition = str(part.get("Content-Disposition", ""))
        if "attachment" in disposition:
            has_attachments = True
            continue
        payload = part.get_payload(decode=True)
        if not payload:
            continue
        charset = part.get_content_charset() or "utf-8"
        try:
            text = payload.decode(charset, errors="replace")
        except (UnicodeDecodeError, LookupError):
            text = payload.decode("utf-8", errors="replace")
        if content_type == "text/plain" and not body_text:
            body_text = text
        elif content_type == "text/html" and not body_html:
            body_html = text
    return body_text, body_html, has_attachments


def _import_one_message_local(msg, folder_id: int, existing_msgids: set, db_session) -> tuple[int, int, int]:
    """Import a single message to local DB storage."""
    import email.utils

    try:
        has_from = msg.get("From")
        has_date = msg.get("Date")
        has_received = msg.get("Received")
        if not has_from and not has_date and not has_received:
            return (0, 1, 0)

        msg_id = msg.get("Message-ID", "").strip()
        if msg_id and msg_id in existing_msgids:
            return (0, 1, 0)

        raw = msg.as_bytes()
        body_text, body_html, has_att = _parse_email_body(msg)

        date_val = None
        try:
            date_val = email.utils.parsedate_to_datetime(msg.get("Date", ""))
        except Exception:
            pass

        local_email = LocalEmail(
            folder_id=folder_id,
            message_id_header=msg_id,
            from_addr=_decode_header(msg.get("From", "")),
            to_addr=_decode_header(msg.get("To", "")),
            cc_addr=_decode_header(msg.get("Cc", "")),
            subject=_decode_header(msg.get("Subject", "")),
            date=date_val,
            has_attachments=has_att,
            body_text=body_text,
            body_html=body_html,
            raw_message=raw,
        )
        db_session.add(local_email)
        db_session.commit()

        if msg_id:
            existing_msgids.add(msg_id)
        return (1, 0, 0)
    except Exception as e:
        db_session.rollback()
        logger.warning(f"Failed to import local message: {e}")
        return (0, 0, 1)


def _get_or_create_local_folder(db_session, account_id: int, folder_name: str) -> int:
    """Get or create a local folder (and all parent folders), returning its ID.

    Creates intermediate folders if needed (e.g. "Archives/Client/PJS"
    creates "Archives", "Archives/Client", then "Archives/Client/PJS").
    Existing folders are reused (merge behavior).
    """
    from sqlalchemy import select as sa_select

    # First check if it already exists
    result = db_session.execute(
        sa_select(LocalFolder).where(
            LocalFolder.account_id == account_id,
            LocalFolder.path == folder_name,
        )
    )
    folder = result.scalar_one_or_none()
    if folder:
        return folder.id

    # Ensure all parent folders exist first
    parts = folder_name.split("/")
    for i in range(1, len(parts)):
        ancestor_path = "/".join(parts[:i])
        existing = db_session.execute(
            sa_select(LocalFolder).where(
                LocalFolder.account_id == account_id,
                LocalFolder.path == ancestor_path,
            )
        ).scalar_one_or_none()
        if not existing:
            ancestor_parent = "/".join(parts[:i-1]) if i > 1 else None
            ancestor = LocalFolder(
                account_id=account_id,
                name=parts[i-1],
                path=ancestor_path,
                parent_path=ancestor_parent,
            )
            db_session.add(ancestor)
            db_session.commit()

    # Create the leaf folder
    name = parts[-1]
    parent_path = "/".join(parts[:-1]) if len(parts) > 1 else None
    new_folder = LocalFolder(
        account_id=account_id,
        name=name,
        path=folder_name,
        parent_path=parent_path,
    )
    db_session.add(new_folder)
    db_session.commit()
    db_session.refresh(new_folder)
    return new_folder.id


def _fetch_existing_local_msgids(db_session, folder_id: int) -> set:
    """Fetch all Message-ID headers from a local folder for deduplication."""
    from sqlalchemy import select as sa_select
    result = db_session.execute(
        sa_select(LocalEmail.message_id_header).where(
            LocalEmail.folder_id == folder_id,
            LocalEmail.message_id_header.isnot(None),
            LocalEmail.message_id_header != "",
        )
    )
    return {row[0] for row in result.all()}


def _run_single_import_local(job_id: str, account_id: int, file_path: str, filename: str, folder: str | None):
    """Import a single mbox file to local DB storage, updating job progress."""
    import mailbox, os
    from src.import_jobs import update_job, add_folder_done
    from src.db.session import get_sync_session

    if not folder:
        basename = os.path.splitext(filename or "INBOX")[0]
        folder = _thunderbird_folder_name(basename)

    total_emails = _count_mbox_messages(file_path)
    update_job(job_id, progress={"total": total_emails, "current_folder": folder})

    db_session = get_sync_session()
    try:
        folder_id = _get_or_create_local_folder(db_session, account_id, folder)
        existing_msgids = _fetch_existing_local_msgids(db_session, folder_id)

        mbox = mailbox.mbox(file_path)
        imported = skipped = errors = current = 0

        for msg in mbox:
            result = _import_one_message_local(msg, folder_id, existing_msgids, db_session)
            imported += result[0]
            skipped += result[1]
            errors += result[2]
            current += 1
            if current % 10 == 0 or current == total_emails:
                update_job(job_id, progress={
                    "current": current, "total": total_emails,
                    "imported": imported, "skipped": skipped, "errors": errors,
                    "current_folder": folder,
                })
        mbox.close()

        add_folder_done(job_id, {"folder": folder, "imported": imported, "skipped": skipped, "errors": errors})
        update_job(job_id, status="done", progress={
            "current": current, "total": total_emails,
            "imported": imported, "skipped": skipped, "errors": errors,
            "current_folder": "",
        })
    finally:
        db_session.close()


def _run_zip_import_local(job_id: str, account_id: int, zip_path: str,
                          skip_folders: set | None = None, target_folder: str | None = None):
    """Import a Thunderbird ZIP profile to local DB storage, updating job progress.

    If target_folder is set, the entire tree is nested under it
    (e.g. target_folder="Archives" => "Client/PJS" becomes "Archives/Client/PJS").
    """
    import mailbox, tempfile, zipfile, shutil, os
    from src.import_jobs import update_job, add_folder_done
    from src.db.session import get_sync_session

    update_job(job_id, status="extracting")
    tmp_dir = tempfile.mkdtemp(prefix="tb_import_local_")
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmp_dir)

        mbox_files = _discover_mbox_files(tmp_dir)
        if not mbox_files:
            update_job(job_id, status="error", error="No mbox files found in ZIP archive")
            return

        total_emails = sum(_count_mbox_messages(p) for p, _ in mbox_files)
        update_job(job_id, status="importing", progress={"total": total_emails})

        total_imported = total_skipped = total_errors = global_current = 0

        db_session = get_sync_session()
        try:
            for mbox_path, folder_name in mbox_files:
                # Prefix with target folder if specified
                effective_folder = f"{target_folder}/{folder_name}" if target_folder else folder_name
                if skip_folders and effective_folder in skip_folders:
                    logger.info(f"Job {job_id}: skipping already-done folder {effective_folder}")
                    continue
                try:
                    folder_id = _get_or_create_local_folder(db_session, account_id, effective_folder)
                    existing_msgids = _fetch_existing_local_msgids(db_session, folder_id)

                    mbox = mailbox.mbox(mbox_path)
                    f_imported = f_skipped = f_errors = 0
                    for msg in mbox:
                        result = _import_one_message_local(msg, folder_id, existing_msgids, db_session)
                        f_imported += result[0]
                        f_skipped += result[1]
                        f_errors += result[2]
                        global_current += 1
                        if global_current % 10 == 0:
                            update_job(job_id, progress={
                                "current": global_current, "total": total_emails,
                                "imported": total_imported + f_imported,
                                "skipped": total_skipped + f_skipped,
                                "errors": total_errors + f_errors,
                                "current_folder": effective_folder,
                            })
                    mbox.close()

                    total_imported += f_imported
                    total_skipped += f_skipped
                    total_errors += f_errors
                    add_folder_done(job_id, {
                        "folder": effective_folder,
                        "imported": f_imported, "skipped": f_skipped, "errors": f_errors,
                    })
                except Exception as e:
                    logger.warning(f"Job {job_id}: failed folder {effective_folder}: {e}")
                    total_errors += 1
                    add_folder_done(job_id, {
                        "folder": effective_folder, "imported": 0, "skipped": 0,
                        "errors": 1, "error_detail": str(e),
                    })
        finally:
            db_session.close()

        update_job(job_id, status="done", progress={
            "current": global_current, "total": total_emails,
            "imported": total_imported, "skipped": total_skipped,
            "errors": total_errors, "current_folder": "",
        })
    except Exception as e:
        update_job(job_id, status="error", error=str(e))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _to_response(a: MailAccount) -> MailAccountResponse:
    return MailAccountResponse(
        id=a.id,
        name=a.name,
        imap_host=a.imap_host,
        imap_port=a.imap_port,
        imap_ssl=a.imap_ssl,
        imap_user=a.imap_user,
        smtp_host=a.smtp_host,
        smtp_port=a.smtp_port,
        smtp_ssl=getattr(a, 'smtp_ssl', True),
        smtp_user=a.smtp_user,
        sync_enabled=a.sync_enabled,
        last_sync_at=str(a.last_sync_at) if a.last_sync_at else None,
    )

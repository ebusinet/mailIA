import imaplib
import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.db.session import get_db
from src.db.models import User, MailAccount
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


class FlagRequest(BaseModel):
    flag: str  # "seen", "flagged"
    action: str = "add"  # "add" or "remove"


class MoveRequest(BaseModel):
    target_folder: str


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

@router.get("/{account_id}/folders")
async def list_folders(
    account_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List all IMAP folders for an account — live from the mail server."""
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
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP connection failed: {e}")

    separator = raw_folders[0]["separator"] if raw_folders else "."
    tree = _build_folder_tree(raw_folders)
    return {"account_id": account_id, "account_name": account.name, "folders": tree, "separator": separator}


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
    return {"status": "created", "folder": req.folder_name}


class DeleteFolderRequest(BaseModel):
    folder_name: str


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
            ok = imap.delete_folder(req.folder_name)
            if not ok:
                raise HTTPException(status_code=400, detail="Failed to delete folder (may not be empty)")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")
    return {"status": "deleted", "folder": req.folder_name}


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
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List messages in an IMAP folder — folder passed as query param to handle / in names."""
    account = await _get_account(account_id, user, db)
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
            status, _ = _select_folder(conn, folder, readonly=True)

            if q.strip():
                # IMAP SEARCH with TEXT criteria (searches headers + body)
                search_term = q.strip().replace('"', '\\"')
                charset = None
                criteria = f'TEXT "{search_term}"'
                # Try UTF-8 charset for non-ASCII queries
                try:
                    search_term.encode('ascii')
                except UnicodeEncodeError:
                    charset = 'UTF-8'
                    criteria = f'TEXT "{search_term}"'
                status, data = conn.uid("SEARCH", charset, criteria)
            else:
                status, data = conn.uid("SEARCH", None, "ALL")
            all_uids = data[0].decode().split() if data[0] else []
            total = len(all_uids)

            start = max(0, total - (page + 1) * size)
            end = total - page * size
            page_uids = all_uids[start:end]
            page_uids.reverse()

            messages = []
            if page_uids:
                uid_range = ",".join(page_uids[-100:])
                status, data = conn.uid("FETCH", uid_range, "(UID FLAGS BODY.PEEK[HEADER.FIELDS (FROM TO SUBJECT DATE)])")
                if status == "OK" and data:
                    i = 0
                    while i < len(data):
                        item = data[i]
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

                            messages.append({
                                "uid": uid,
                                "from": from_addr,
                                "subject": subject,
                                "date": date_str,
                                "seen": seen,
                                "flagged": flagged,
                                "answered": answered,
                            })
                        i += 1

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
                    status, fdata = conn.uid("FETCH", uid_range, "(UID FLAGS BODY.PEEK[HEADER.FIELDS (FROM TO SUBJECT DATE)])")
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
                            results.append({
                                "uid": uid,
                                "folder": folder,
                                "from": from_addr,
                                "subject": subject,
                                "date": date_str,
                                "seen": seen,
                                "flagged": flagged,
                                "answered": answered,
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
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch full email content by UID — live from IMAP."""
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
                    break

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
        "body_text": body_text,
        "body_html": body_html,
        "attachments": attachments,
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
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Add or remove a flag on an email."""
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
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Move an email to another folder."""
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
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete an email (move to Trash)."""
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


@router.get("/{account_id}/message/{uid}/attachment/{index}")
async def download_attachment(
    account_id: int,
    uid: str,
    index: int,
    folder: str = Query(...),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Download an attachment by index from an email."""
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


def _decode_header(raw: str) -> str:
    """Decode an RFC2047-encoded email header."""
    import email.header
    parts = email.header.decode_header(raw)
    decoded = []
    for part, charset in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(charset or "utf-8", errors="replace"))
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

    def _to_list(d: dict, prefix: str = "") -> list[dict]:
        result = []
        for name, children in sorted(d.items()):
            tree_path = f"{prefix}{sep}{name}" if prefix else name
            display = segment_display.get(name, _decode_imap_utf7(name))
            item = {"name": display, "path": tree_path}
            if tree_path not in real_folders:
                item["noselect"] = True
            child_list = _to_list(children, tree_path)
            if child_list:
                item["children"] = child_list
            result.append(item)
        return result

    return _to_list(root)


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

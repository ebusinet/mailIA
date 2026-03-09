"""
MailIA MCP Server — exposes email management tools for AI assistants.

Run with:  python -m src.mcp.server
Or:        fastmcp run src/mcp/server.py
"""
import json
import logging
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Annotated

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from src.config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-mailia")

# User ID is configured at startup via env var MCP_USER_ID
USER_ID: int = 0

mcp = FastMCP(
    name="MailIA",
    instructions=(
        "MailIA MCP server — manage emails via IMAP, search in Elasticsearch, "
        "apply AI rules, send emails via SMTP. All operations are scoped to the "
        "authenticated user."
    ),
)


def _user_id() -> int:
    if USER_ID == 0:
        raise ToolError("MCP_USER_ID not configured")
    return USER_ID


# ---------------------------------------------------------------------------
# SEARCH & INDEXATION (Elasticsearch)
# ---------------------------------------------------------------------------

@mcp.tool(tags={"search"})
async def search_emails(
    query: Annotated[str, Field(description="Search query (full-text)")] = "",
    account_id: Annotated[int | None, Field(description="Filter by account ID")] = None,
    folder: Annotated[str | None, Field(description="Filter by folder name")] = None,
    from_addr: Annotated[str | None, Field(description="Filter by sender (partial match)")] = None,
    date_from: Annotated[str | None, Field(description="Start date (YYYY-MM-DD)")] = None,
    date_to: Annotated[str | None, Field(description="End date (YYYY-MM-DD)")] = None,
    has_attachments: Annotated[bool | None, Field(description="Filter emails with attachments")] = None,
    size: Annotated[int, Field(description="Max results", ge=1, le=100)] = 20,
    page: Annotated[int, Field(description="Page number (0-based)", ge=0)] = 0,
) -> dict:
    """Search indexed emails using full-text search with optional filters.
    Returns matching emails with subject, sender, date, folder, and highlighted snippets."""
    from src.mcp.context import get_es
    from src.search.indexer import search_emails as _search

    uid = _user_id()
    es = await get_es()
    try:
        raw = await _search(
            es, uid, query=query, account_id=account_id, folder=folder,
            from_addr=from_addr, date_from=date_from, date_to=date_to,
            has_attachments=has_attachments, page=page, size=size,
        )
        hits = raw["hits"]
        total = hits["total"]["value"] if isinstance(hits["total"], dict) else hits["total"]
        results = []
        for hit in hits["hits"]:
            src = hit["_source"]
            results.append({
                "uid": src.get("uid"),
                "folder": src.get("folder"),
                "from": src.get("from_addr"),
                "subject": src.get("subject"),
                "date": src.get("date"),
                "has_attachments": src.get("has_attachments", False),
                "score": hit.get("_score", 0),
                "highlight": hit.get("highlight"),
            })
        return {"total": total, "page": page, "size": size, "results": results}
    finally:
        await es.close()


@mcp.tool(tags={"search"})
async def semantic_search(
    query: Annotated[str, Field(description="Natural language query for semantic similarity search")],
    size: Annotated[int, Field(description="Max results", ge=1, le=50)] = 10,
) -> dict:
    """Search emails by semantic similarity using vector embeddings.
    Best for finding conceptually related emails rather than exact keyword matches."""
    from src.mcp.context import get_es
    from src.search.indexer import semantic_search as _semantic

    uid = _user_id()
    # Generate embedding from query
    try:
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer("all-MiniLM-L6-v2")
        embedding = model.encode(query).tolist()
    except Exception as e:
        raise ToolError(f"Embedding generation failed: {e}")

    es = await get_es()
    try:
        raw = await _semantic(es, uid, query_embedding=embedding, size=size)
        hits = raw["hits"]["hits"]
        results = []
        for hit in hits:
            src = hit["_source"]
            results.append({
                "uid": src.get("uid"),
                "folder": src.get("folder"),
                "from": src.get("from_addr"),
                "subject": src.get("subject"),
                "date": src.get("date"),
                "score": hit.get("_score", 0),
            })
        return {"results": results}
    finally:
        await es.close()


@mcp.tool(tags={"search"})
async def count_emails(
    folder: Annotated[str | None, Field(description="Filter by folder")] = None,
    from_addr: Annotated[str | None, Field(description="Filter by sender")] = None,
    date_from: Annotated[str | None, Field(description="Start date (YYYY-MM-DD)")] = None,
    date_to: Annotated[str | None, Field(description="End date (YYYY-MM-DD)")] = None,
    account_id: Annotated[int | None, Field(description="Filter by account ID")] = None,
) -> dict:
    """Count indexed emails matching the given criteria."""
    from src.mcp.context import get_es
    from src.search.indexer import _index_name

    uid = _user_id()
    index = _index_name(uid)
    es = await get_es()
    try:
        filters = []
        if account_id:
            filters.append({"term": {"account_id": account_id}})
        if folder:
            filters.append({"term": {"folder": folder}})
        if from_addr:
            filters.append({"wildcard": {"from_addr": f"*{from_addr}*"}})
        if date_from or date_to:
            dr = {}
            if date_from:
                dr["gte"] = date_from
            if date_to:
                dr["lte"] = date_to
            filters.append({"range": {"date": dr}})

        body = {"query": {"bool": {"filter": filters}} if filters else {"match_all": {}}}
        result = await es.count(index=index, body=body)
        return {"count": result["count"]}
    except Exception as e:
        return {"count": 0, "error": str(e)}
    finally:
        await es.close()


@mcp.tool(tags={"search"})
async def get_folders_stats(
    account_id: Annotated[int | None, Field(description="Filter by account ID")] = None,
) -> dict:
    """Get per-folder email counts from the Elasticsearch index."""
    from src.mcp.context import get_es
    from src.search.indexer import _index_name

    uid = _user_id()
    index = _index_name(uid)
    es = await get_es()
    try:
        query = {"term": {"account_id": account_id}} if account_id else {"match_all": {}}
        result = await es.search(
            index=index,
            body={
                "size": 0,
                "query": query,
                "aggs": {"folders": {"terms": {"field": "folder", "size": 500}}},
            },
        )
        buckets = result.get("aggregations", {}).get("folders", {}).get("buckets", [])
        folders = {b["key"]: b["doc_count"] for b in buckets}
        return {"total": sum(folders.values()), "folders": folders}
    except Exception as e:
        return {"total": 0, "folders": {}, "error": str(e)}
    finally:
        await es.close()


# ---------------------------------------------------------------------------
# READ EMAILS (IMAP)
# ---------------------------------------------------------------------------

@mcp.tool(tags={"read"})
async def read_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="IMAP folder name")],
    uid: Annotated[str, Field(description="Email UID")],
) -> dict:
    """Read the full content of a specific email: headers, body text, and attachment list."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        ctx = imap.fetch_email(uid, folder)
        if not ctx:
            raise ToolError(f"Email UID {uid} not found in {folder}")
        return {
            "uid": ctx.uid,
            "folder": ctx.folder,
            "from": ctx.from_addr,
            "to": ctx.to_addr,
            "subject": ctx.subject,
            "date": ctx.date,
            "body": ctx.body_text[:10000],
            "has_attachments": ctx.has_attachments,
            "attachment_names": ctx.attachment_names,
        }
    finally:
        imap.disconnect()


@mcp.tool(tags={"read"})
async def list_emails(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="IMAP folder name")] = "INBOX",
    limit: Annotated[int, Field(description="Max emails to list", ge=1, le=100)] = 20,
) -> dict:
    """List recent emails in an IMAP folder with subject, sender, and date."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        uids = imap.get_uids(folder)
        # Take the most recent UIDs
        recent_uids = uids[-limit:] if len(uids) > limit else uids
        recent_uids.reverse()

        emails = []
        for u in recent_uids:
            try:
                ctx = imap.fetch_email(u, folder)
                if ctx:
                    emails.append({
                        "uid": ctx.uid,
                        "from": ctx.from_addr,
                        "subject": ctx.subject,
                        "date": ctx.date,
                        "has_attachments": ctx.has_attachments,
                    })
            except Exception:
                pass
        return {"folder": folder, "count": len(emails), "emails": emails}
    finally:
        imap.disconnect()


@mcp.tool(tags={"read"})
async def list_folders(
    account_id: Annotated[int, Field(description="Mail account ID")],
) -> dict:
    """List all IMAP folders for a mail account."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        folders = imap.list_folders()
        return {
            "account_id": account_id,
            "folders": [
                {"name": f["name"], "display_name": f.get("display_name", f["name"])}
                for f in folders
            ],
        }
    finally:
        imap.disconnect()


@mcp.tool(tags={"read"})
async def get_attachment(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="IMAP folder name")],
    uid: Annotated[str, Field(description="Email UID")],
    attachment_index: Annotated[int, Field(description="Attachment index (0-based)")] = 0,
) -> dict:
    """Get metadata and base64-encoded content of an email attachment."""
    import base64
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        data = imap.get_attachment_data(uid, folder, attachment_index)
        if not data:
            raise ToolError(f"Attachment {attachment_index} not found for UID {uid}")
        return {
            "filename": data["filename"],
            "content_type": data["content_type"],
            "size_bytes": len(data["data"]),
            "data_base64": base64.b64encode(data["data"]).decode()[:100000],
        }
    finally:
        imap.disconnect()


# ---------------------------------------------------------------------------
# ACTIONS ON EMAILS (IMAP)
# ---------------------------------------------------------------------------

@mcp.tool(tags={"action"})
async def move_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Current IMAP folder")],
    uid: Annotated[str, Field(description="Email UID")],
    target_folder: Annotated[str, Field(description="Target folder to move the email to")],
) -> dict:
    """Move an email to a different IMAP folder. Creates the target folder if needed."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        ok = imap.move_email(uid, folder, target_folder)
        if not ok:
            raise ToolError(f"Failed to move UID {uid} to {target_folder}")
        return {"status": "moved", "uid": uid, "from": folder, "to": target_folder}
    finally:
        imap.disconnect()


@mcp.tool(tags={"action"})
async def move_emails_bulk(
    account_id: Annotated[int, Field(description="Mail account ID")],
    moves: Annotated[list[dict], Field(description="List of {folder, uid, target_folder}")],
) -> dict:
    """Move multiple emails at once. Each move specifies folder, uid, and target_folder."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        results = {"moved": 0, "failed": 0, "details": []}
        for m in moves:
            try:
                ok = imap.move_email(m["uid"], m["folder"], m["target_folder"])
                if ok:
                    results["moved"] += 1
                    results["details"].append({"uid": m["uid"], "status": "moved"})
                else:
                    results["failed"] += 1
                    results["details"].append({"uid": m["uid"], "status": "failed"})
            except Exception as e:
                results["failed"] += 1
                results["details"].append({"uid": m["uid"], "status": "error", "error": str(e)})
        return results
    finally:
        imap.disconnect()


@mcp.tool(tags={"action"})
async def flag_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="IMAP folder name")],
    uid: Annotated[str, Field(description="Email UID")],
    flag: Annotated[str, Field(description="Flag: important, read, seen, answered, draft")] = "important",
    action: Annotated[str, Field(description="'add' or 'remove'")] = "add",
) -> dict:
    """Add or remove a flag on an email (important, read, seen, etc.)."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        if action == "add":
            if flag.lower() in ("read", "seen"):
                ok = imap.mark_read(uid, folder)
            else:
                ok = imap.flag_email(uid, folder, flag)
        else:
            if flag.lower() in ("read", "seen"):
                ok = imap.mark_unread(uid, folder)
            else:
                ok = imap.unflag_email(uid, folder, flag)
        return {"status": "ok" if ok else "failed", "uid": uid, "flag": flag, "action": action}
    finally:
        imap.disconnect()


@mcp.tool(tags={"action"})
async def delete_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="IMAP folder name")],
    uid: Annotated[str, Field(description="Email UID")],
) -> dict:
    """Delete an email (moves to Trash or flags as Deleted)."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        ok = imap.delete_email(uid, folder)
        return {"status": "deleted" if ok else "failed", "uid": uid, "folder": folder}
    finally:
        imap.disconnect()


@mcp.tool(tags={"action"})
async def create_folder(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder_name: Annotated[str, Field(description="Full folder path to create (e.g. INBOX.Archives.2024)")],
) -> dict:
    """Create a new IMAP folder."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        ok = imap.create_folder(folder_name)
        return {"status": "created" if ok else "failed", "folder": folder_name}
    finally:
        imap.disconnect()


# ---------------------------------------------------------------------------
# SEND EMAILS (SMTP)
# ---------------------------------------------------------------------------

@mcp.tool(tags={"send"})
async def send_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    to: Annotated[list[str], Field(description="Recipient email addresses")],
    subject: Annotated[str, Field(description="Email subject")] = "",
    body: Annotated[str, Field(description="Email body (plain text)")] = "",
    cc: Annotated[list[str], Field(description="CC recipients")] = [],
    bcc: Annotated[list[str], Field(description="BCC recipients")] = [],
) -> dict:
    """Send an email via SMTP."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account
    from src.security import decrypt_value

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    if not account.smtp_host:
        raise ToolError("SMTP not configured for this account")

    msg = MIMEMultipart()
    msg["From"] = account.smtp_user or account.imap_user
    msg["To"] = ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    smtp_password = decrypt_value(account.smtp_password_encrypted) if account.smtp_password_encrypted else decrypt_value(account.imap_password_encrypted)

    try:
        if account.smtp_ssl:
            server = smtplib.SMTP_SSL(account.smtp_host, account.smtp_port)
        else:
            server = smtplib.SMTP(account.smtp_host, account.smtp_port)
            server.starttls()
        server.login(account.smtp_user or account.imap_user, smtp_password)
        recipients = to + cc + bcc
        server.sendmail(msg["From"], recipients, msg.as_string())
        server.quit()

        # Save to Sent folder
        imap = get_imap(account)
        imap.connect()
        try:
            imap.save_to_sent(msg.as_bytes())
        finally:
            imap.disconnect()

        return {"status": "sent", "to": to, "subject": subject}
    except Exception as e:
        raise ToolError(f"SMTP error: {e}")


@mcp.tool(tags={"send"})
async def reply_to_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder of the original email")],
    uid: Annotated[str, Field(description="UID of the email to reply to")],
    body: Annotated[str, Field(description="Reply body text")],
    reply_all: Annotated[bool, Field(description="Reply to all recipients")] = False,
) -> dict:
    """Reply to an existing email. Reads the original, composes a reply, and sends via SMTP."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account
    from src.security import decrypt_value

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    if not account.smtp_host:
        raise ToolError("SMTP not configured for this account")

    imap = get_imap(account)
    imap.connect()
    try:
        original = imap.fetch_email(uid, folder)
        if not original:
            raise ToolError(f"Original email UID {uid} not found")

        to_addrs = [original.from_addr]
        subject = f"Re: {original.subject}" if not original.subject.lower().startswith("re:") else original.subject

        msg = MIMEMultipart()
        msg["From"] = account.smtp_user or account.imap_user
        msg["To"] = ", ".join(to_addrs)
        msg["Subject"] = subject
        msg["In-Reply-To"] = uid

        full_body = f"{body}\n\n--- Original ---\nFrom: {original.from_addr}\nDate: {original.date}\n\n{original.body_text[:5000]}"
        msg.attach(MIMEText(full_body, "plain", "utf-8"))

        smtp_password = decrypt_value(account.smtp_password_encrypted) if account.smtp_password_encrypted else decrypt_value(account.imap_password_encrypted)

        if account.smtp_ssl:
            server = smtplib.SMTP_SSL(account.smtp_host, account.smtp_port)
        else:
            server = smtplib.SMTP(account.smtp_host, account.smtp_port)
            server.starttls()
        server.login(account.smtp_user or account.imap_user, smtp_password)
        server.sendmail(msg["From"], to_addrs, msg.as_string())
        server.quit()

        imap.save_to_sent(msg.as_bytes())
        return {"status": "replied", "to": to_addrs, "subject": subject}
    finally:
        imap.disconnect()


@mcp.tool(tags={"send"})
async def forward_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder of the email to forward")],
    uid: Annotated[str, Field(description="UID of the email to forward")],
    to: Annotated[list[str], Field(description="Recipients to forward to")],
    comment: Annotated[str, Field(description="Optional comment to add above the forwarded email")] = "",
) -> dict:
    """Forward an email to new recipients with an optional comment."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account
    from src.security import decrypt_value

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    if not account.smtp_host:
        raise ToolError("SMTP not configured for this account")

    imap = get_imap(account)
    imap.connect()
    try:
        original = imap.fetch_email(uid, folder)
        if not original:
            raise ToolError(f"Email UID {uid} not found")

        subject = f"Fwd: {original.subject}" if not original.subject.lower().startswith("fwd:") else original.subject

        msg = MIMEMultipart()
        msg["From"] = account.smtp_user or account.imap_user
        msg["To"] = ", ".join(to)
        msg["Subject"] = subject

        fwd_body = ""
        if comment:
            fwd_body = f"{comment}\n\n"
        fwd_body += f"---------- Forwarded message ----------\nFrom: {original.from_addr}\nDate: {original.date}\nSubject: {original.subject}\n\n{original.body_text[:10000]}"
        msg.attach(MIMEText(fwd_body, "plain", "utf-8"))

        smtp_password = decrypt_value(account.smtp_password_encrypted) if account.smtp_password_encrypted else decrypt_value(account.imap_password_encrypted)

        if account.smtp_ssl:
            server = smtplib.SMTP_SSL(account.smtp_host, account.smtp_port)
        else:
            server = smtplib.SMTP(account.smtp_host, account.smtp_port)
            server.starttls()
        server.login(account.smtp_user or account.imap_user, smtp_password)
        server.sendmail(msg["From"], to, msg.as_string())
        server.quit()

        imap.save_to_sent(msg.as_bytes())
        return {"status": "forwarded", "to": to, "subject": subject}
    finally:
        imap.disconnect()


@mcp.tool(tags={"send"})
async def save_draft(
    account_id: Annotated[int, Field(description="Mail account ID")],
    to: Annotated[list[str], Field(description="Recipient email addresses")] = [],
    subject: Annotated[str, Field(description="Email subject")] = "",
    body: Annotated[str, Field(description="Email body")] = "",
) -> dict:
    """Save an email draft to the Drafts folder."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    msg = MIMEMultipart()
    msg["From"] = account.smtp_user or account.imap_user
    msg["To"] = ", ".join(to)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    imap = get_imap(account)
    imap.connect()
    try:
        ok = imap.save_draft(msg.as_bytes())
        return {"status": "saved" if ok else "failed"}
    finally:
        imap.disconnect()


# ---------------------------------------------------------------------------
# AI
# ---------------------------------------------------------------------------

@mcp.tool(tags={"ai"})
async def summarize_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="IMAP folder name")],
    uid: Annotated[str, Field(description="Email UID")],
) -> dict:
    """Summarize an email in a few sentences using AI."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account, get_user
    from src.ai.router import get_llm_for_user

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)
        user = await get_user(db, user_id)

        imap = get_imap(account)
        imap.connect()
        try:
            ctx = imap.fetch_email(uid, folder)
            if not ctx:
                raise ToolError(f"Email UID {uid} not found")
        finally:
            imap.disconnect()

        llm = await get_llm_for_user(db, user)
        summary = await llm.summarize(
            f"From: {ctx.from_addr}\nSubject: {ctx.subject}\nDate: {ctx.date}\n\n{ctx.body_text[:5000]}"
        )
        return {"uid": uid, "subject": ctx.subject, "summary": summary}


@mcp.tool(tags={"ai"})
async def summarize_thread(
    query: Annotated[str, Field(description="Search query to find the email thread (subject or sender)")],
    size: Annotated[int, Field(description="Max emails in thread", ge=1, le=20)] = 10,
) -> dict:
    """Summarize an email thread by searching for related emails and generating an AI summary."""
    from src.mcp.context import get_db, get_es
    from src.mcp.helpers import get_user
    from src.search.indexer import search_emails as _search
    from src.ai.router import get_llm_for_user
    from src.ai.base import AIMessage

    user_id = _user_id()
    es = await get_es()
    try:
        raw = await _search(es, user_id, query=query, size=size)
        hits = raw["hits"]["hits"]
        if not hits:
            return {"summary": "No emails found matching the query."}

        thread_text = []
        for hit in hits:
            src = hit["_source"]
            thread_text.append(
                f"From: {src.get('from_addr')}\n"
                f"Date: {src.get('date')}\n"
                f"Subject: {src.get('subject')}\n"
                f"{src.get('body', '')[:2000]}\n---"
            )
    finally:
        await es.close()

    async with get_db() as db:
        user = await get_user(db, user_id)
        llm = await get_llm_for_user(db, user)
        messages = [
            AIMessage("system", "Summarize this email thread concisely. Focus on key decisions, "
                      "action items, and important information. Reply in the same language as the emails."),
            AIMessage("user", "\n".join(thread_text)),
        ]
        response = await llm.chat(messages)
        return {"emails_found": len(hits), "summary": response.content}


@mcp.tool(tags={"ai"})
async def classify_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="IMAP folder name")],
    uid: Annotated[str, Field(description="Email UID")],
    categories: Annotated[list[str], Field(description="Categories to classify into")],
) -> dict:
    """Classify an email into one of the given categories using AI."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account, get_user
    from src.ai.router import get_llm_for_user

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)
        user = await get_user(db, user_id)

        imap = get_imap(account)
        imap.connect()
        try:
            ctx = imap.fetch_email(uid, folder)
            if not ctx:
                raise ToolError(f"Email UID {uid} not found")
        finally:
            imap.disconnect()

        llm = await get_llm_for_user(db, user)
        email_text = f"From: {ctx.from_addr}\nSubject: {ctx.subject}\n\n{ctx.body_text[:3000]}"
        category = await llm.classify(email_text, categories)
        return {"uid": uid, "category": category, "subject": ctx.subject}


@mcp.tool(tags={"ai"})
async def extract_info(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="IMAP folder name")],
    uid: Annotated[str, Field(description="Email UID")],
    fields: Annotated[list[str], Field(description="Fields to extract (e.g. date, amount, company, phone)")],
) -> dict:
    """Extract structured information from an email using AI (dates, amounts, names, etc.)."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account, get_user
    from src.ai.router import get_llm_for_user

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)
        user = await get_user(db, user_id)

        imap = get_imap(account)
        imap.connect()
        try:
            ctx = imap.fetch_email(uid, folder)
            if not ctx:
                raise ToolError(f"Email UID {uid} not found")
        finally:
            imap.disconnect()

        llm = await get_llm_for_user(db, user)
        email_text = f"From: {ctx.from_addr}\nSubject: {ctx.subject}\nDate: {ctx.date}\n\n{ctx.body_text[:5000]}"
        extracted = await llm.extract_info(email_text, fields)
        return {"uid": uid, "subject": ctx.subject, "extracted": extracted}


@mcp.tool(tags={"ai"})
async def ask_about_emails(
    question: Annotated[str, Field(description="Question in natural language about your emails")],
) -> dict:
    """Ask a question about your emails. Searches relevant emails first, then uses AI to answer."""
    from src.mcp.context import get_db, get_es
    from src.mcp.helpers import get_user
    from src.search.indexer import search_emails as _search
    from src.ai.router import get_llm_for_user
    from src.ai.base import AIMessage

    user_id = _user_id()
    es = await get_es()
    try:
        raw = await _search(es, user_id, query=question, size=10)
        hits = raw["hits"]["hits"]
        context_parts = []
        for hit in hits:
            src = hit["_source"]
            context_parts.append(
                f"From: {src.get('from_addr')}\n"
                f"Subject: {src.get('subject')}\n"
                f"Date: {src.get('date')}\n"
                f"{src.get('body', '')[:1500]}"
            )
        context_text = "\n---\n".join(context_parts) if context_parts else "No relevant emails found."
    finally:
        await es.close()

    async with get_db() as db:
        user = await get_user(db, user_id)
        llm = await get_llm_for_user(db, user)
        messages = [
            AIMessage("system",
                      "You are MailIA, an AI email assistant. Answer the user's question based on "
                      "the email context provided. Be concise and factual. Reply in the same language "
                      "as the user."),
            AIMessage("user", f"Relevant emails:\n{context_text}\n\nQuestion: {question}"),
        ]
        response = await llm.chat(messages)
        return {"answer": response.content, "emails_searched": len(hits)}


# ---------------------------------------------------------------------------
# RULES & AUTOMATION
# ---------------------------------------------------------------------------

@mcp.tool(tags={"rules"})
async def list_rules() -> dict:
    """List all AI rules configured for the user."""
    from sqlalchemy import select
    from src.mcp.context import get_db
    from src.db.models import AIRule
    from src.rules.parser import parse_rules_markdown

    user_id = _user_id()
    async with get_db() as db:
        result = await db.execute(
            select(AIRule).where(AIRule.user_id == user_id).order_by(AIRule.priority)
        )
        rules = result.scalars().all()
        return {
            "rules": [
                {
                    "id": r.id,
                    "name": r.name,
                    "priority": r.priority,
                    "is_active": r.is_active,
                    "parsed_count": len(parse_rules_markdown(r.rules_markdown)),
                    "markdown_preview": r.rules_markdown[:500],
                }
                for r in rules
            ]
        }


@mcp.tool(tags={"rules"})
async def create_rule(
    name: Annotated[str, Field(description="Rule name")],
    rules_markdown: Annotated[str, Field(description="Rules in Markdown format (## Rule Name, - **Si**: ..., - **Alors**: ...)")],
    priority: Annotated[int, Field(description="Priority (lower = higher priority)")] = 100,
) -> dict:
    """Create a new AI rule set from Markdown. Rules will be applied during email sync."""
    from src.mcp.context import get_db
    from src.db.models import AIRule
    from src.rules.parser import parse_rules_markdown

    user_id = _user_id()
    parsed = parse_rules_markdown(rules_markdown)
    if not parsed:
        raise ToolError("No valid rules found in the Markdown")

    async with get_db() as db:
        rule = AIRule(
            user_id=user_id,
            name=name,
            rules_markdown=rules_markdown,
            priority=priority,
        )
        db.add(rule)
        await db.commit()
        await db.refresh(rule)
        return {"id": rule.id, "name": rule.name, "parsed_rules": len(parsed)}


@mcp.tool(tags={"rules"})
async def preview_rule(
    rules_markdown: Annotated[str, Field(description="Rules in Markdown format to test")],
    limit: Annotated[int, Field(description="Max emails to test against", ge=1, le=50)] = 10,
) -> dict:
    """Preview what a rule would match against recent indexed emails WITHOUT applying actions."""
    from src.mcp.context import get_db, get_es
    from src.search.indexer import search_emails as _search
    from src.rules.parser import parse_rules_markdown
    from src.rules.engine import evaluate_rules, EmailContext

    user_id = _user_id()
    parsed = parse_rules_markdown(rules_markdown)
    if not parsed:
        raise ToolError("No valid rules found in the Markdown")

    es = await get_es()
    try:
        raw = await _search(es, user_id, query="", size=limit)
        hits = raw["hits"]["hits"]
    finally:
        await es.close()

    matches = []
    for hit in hits:
        src = hit["_source"]
        ctx = EmailContext(
            uid=src.get("uid", ""),
            folder=src.get("folder", ""),
            from_addr=src.get("from_addr", ""),
            to_addr=src.get("to_addr", ""),
            subject=src.get("subject", ""),
            body_text=src.get("body", "")[:3000],
            has_attachments=src.get("has_attachments", False),
            attachment_names=src.get("attachment_names", []),
            date=src.get("date", ""),
        )
        rule_matches = await evaluate_rules(ctx, parsed, llm=None)
        if rule_matches:
            matches.append({
                "uid": ctx.uid,
                "from": ctx.from_addr,
                "subject": ctx.subject,
                "matched_rules": [m.rule.name for m in rule_matches],
            })

    return {"tested": len(hits), "matched": len(matches), "matches": matches}


@mcp.tool(tags={"rules"})
async def trigger_sync(
    account_id: Annotated[int | None, Field(description="Account ID to sync (all if not specified)")] = None,
) -> dict:
    """Trigger an immediate email sync via Celery worker. Does NOT block — sync runs in background."""
    from src.worker.tasks import sync_account, sync_all_accounts

    if account_id:
        sync_account.delay(account_id)
        return {"status": "sync_queued", "account_id": account_id}
    else:
        sync_all_accounts.delay()
        return {"status": "sync_all_queued"}


# ---------------------------------------------------------------------------
# ACCOUNTS & ADMIN
# ---------------------------------------------------------------------------

@mcp.tool(tags={"admin"})
async def list_accounts() -> dict:
    """List all configured mail accounts with sync status."""
    from src.mcp.context import get_db
    from src.mcp.helpers import list_user_accounts

    user_id = _user_id()
    async with get_db() as db:
        accounts = await list_user_accounts(db, user_id)
        return {
            "accounts": [
                {
                    "id": a.id,
                    "name": a.name,
                    "imap_host": a.imap_host,
                    "imap_user": a.imap_user,
                    "sync_enabled": a.sync_enabled,
                    "last_sync_at": a.last_sync_at.isoformat() if a.last_sync_at else None,
                    "folders_synced": len(a.sync_state or {}),
                }
                for a in accounts
            ]
        }


@mcp.tool(tags={"admin"})
async def get_sync_status(
    account_id: Annotated[int | None, Field(description="Account ID (all accounts if not specified)")] = None,
) -> dict:
    """Get detailed sync status: last sync time, per-folder progress, ES index stats."""
    from src.mcp.context import get_db, get_es
    from src.mcp.helpers import list_user_accounts, get_account
    from src.search.indexer import _index_name

    user_id = _user_id()
    async with get_db() as db:
        if account_id:
            accounts = [await get_account(db, user_id, account_id)]
        else:
            accounts = await list_user_accounts(db, user_id)

    es = await get_es()
    try:
        index = _index_name(user_id)
        try:
            index_info = await es.cat.indices(index=index, format="json")
            es_info = index_info[0] if index_info else {}
        except Exception:
            es_info = {}

        results = []
        for a in accounts:
            sync_state = a.sync_state or {}
            # Get ES count for this account
            try:
                count_result = await es.count(
                    index=index,
                    body={"query": {"term": {"account_id": a.id}}},
                )
                es_count = count_result["count"]
            except Exception:
                es_count = 0

            results.append({
                "account_id": a.id,
                "name": a.name,
                "sync_enabled": a.sync_enabled,
                "last_sync_at": a.last_sync_at.isoformat() if a.last_sync_at else None,
                "folders_synced": len(sync_state),
                "sync_state": sync_state,
                "es_indexed_count": es_count,
            })

        return {
            "accounts": results,
            "es_index": {
                "name": es_info.get("index", index),
                "docs_count": int(es_info.get("docs.count", 0)),
                "store_size": es_info.get("store.size", "0b"),
            },
        }
    finally:
        await es.close()


@mcp.tool(tags={"admin"})
async def get_processing_logs(
    limit: Annotated[int, Field(description="Max logs to return", ge=1, le=100)] = 20,
    account_id: Annotated[int | None, Field(description="Filter by account ID")] = None,
) -> dict:
    """Get recent processing logs — actions executed by AI rules on emails."""
    from sqlalchemy import select
    from src.mcp.context import get_db
    from src.db.models import ProcessingLog

    user_id = _user_id()
    async with get_db() as db:
        query = select(ProcessingLog).where(ProcessingLog.user_id == user_id)
        if account_id:
            query = query.where(ProcessingLog.mail_account_id == account_id)
        query = query.order_by(ProcessingLog.created_at.desc()).limit(limit)

        result = await db.execute(query)
        logs = result.scalars().all()
        return {
            "logs": [
                {
                    "id": log.id,
                    "mail_uid": log.mail_uid,
                    "folder": log.folder,
                    "action": log.action_taken,
                    "detail": log.action_detail,
                    "ai_response": log.ai_response[:200] if log.ai_response else None,
                    "created_at": log.created_at.isoformat() if log.created_at else None,
                }
                for log in logs
            ]
        }


# ---------------------------------------------------------------------------
# LOCAL STORAGE (PostgreSQL — non-IMAP emails)
# ---------------------------------------------------------------------------

@mcp.tool(tags={"local"})
async def list_local_folders(
    account_id: Annotated[int, Field(description="Mail account ID")],
) -> dict:
    """List all local (non-IMAP) folders for a mail account, as a flat list with hierarchy info."""
    from sqlalchemy import select
    from src.mcp.context import get_db
    from src.mcp.helpers import get_account
    from src.db.models import LocalFolder

    user_id = _user_id()
    async with get_db() as db:
        await get_account(db, user_id, account_id)
        result = await db.execute(
            select(LocalFolder).where(LocalFolder.account_id == account_id).order_by(LocalFolder.path)
        )
        folders = result.scalars().all()
        return {
            "account_id": account_id,
            "folders": [
                {"id": f.id, "name": f.name, "path": f.path, "parent_path": f.parent_path}
                for f in folders
            ],
        }


@mcp.tool(tags={"local"})
async def list_local_emails(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder_path: Annotated[str, Field(description="Local folder path (e.g. 'Perso/maison')")],
    limit: Annotated[int, Field(description="Max emails to list", ge=1, le=200)] = 50,
    offset: Annotated[int, Field(description="Offset for pagination", ge=0)] = 0,
) -> dict:
    """List emails in a local folder with subject, sender, date. Paginated."""
    from sqlalchemy import select, func
    from src.mcp.context import get_db
    from src.mcp.helpers import get_account
    from src.db.models import LocalFolder, LocalEmail

    user_id = _user_id()
    async with get_db() as db:
        await get_account(db, user_id, account_id)
        folder = (await db.execute(
            select(LocalFolder).where(LocalFolder.account_id == account_id, LocalFolder.path == folder_path)
        )).scalar_one_or_none()
        if not folder:
            raise ToolError(f"Local folder '{folder_path}' not found")

        total = (await db.execute(
            select(func.count()).select_from(LocalEmail).where(LocalEmail.folder_id == folder.id)
        )).scalar()

        result = await db.execute(
            select(LocalEmail).where(LocalEmail.folder_id == folder.id)
            .order_by(LocalEmail.date.desc().nullslast())
            .offset(offset).limit(limit)
        )
        emails = result.scalars().all()
        return {
            "folder": folder_path, "total": total,
            "emails": [
                {
                    "id": e.id, "message_id": e.message_id_header,
                    "from": e.from_addr, "to": e.to_addr, "subject": e.subject,
                    "date": e.date.isoformat() if e.date else None,
                    "has_attachments": e.has_attachments,
                    "seen": e.seen, "flagged": e.flagged,
                }
                for e in emails
            ],
        }


@mcp.tool(tags={"local"})
async def read_local_email(
    email_id: Annotated[int, Field(description="Local email ID")],
) -> dict:
    """Read the full content of a local email: headers, body text/html."""
    from sqlalchemy import select
    from src.mcp.context import get_db
    from src.db.models import LocalEmail, LocalFolder, MailAccount

    user_id = _user_id()
    async with get_db() as db:
        email = (await db.execute(select(LocalEmail).where(LocalEmail.id == email_id))).scalar_one_or_none()
        if not email:
            raise ToolError(f"Local email {email_id} not found")
        folder = (await db.execute(select(LocalFolder).where(LocalFolder.id == email.folder_id))).scalar_one_or_none()
        if not folder:
            raise ToolError("Folder not found")
        account = (await db.execute(
            select(MailAccount).where(MailAccount.id == folder.account_id, MailAccount.user_id == user_id)
        )).scalar_one_or_none()
        if not account:
            raise ToolError("Access denied")

        return {
            "id": email.id, "folder": folder.path,
            "message_id": email.message_id_header,
            "from": email.from_addr, "to": email.to_addr, "cc": email.cc_addr,
            "subject": email.subject,
            "date": email.date.isoformat() if email.date else None,
            "body_text": (email.body_text or "")[:10000],
            "body_html": (email.body_html or "")[:10000],
            "has_attachments": email.has_attachments,
            "seen": email.seen, "flagged": email.flagged, "answered": email.answered,
        }


@mcp.tool(tags={"local"})
async def local_folder_stats(
    account_id: Annotated[int, Field(description="Mail account ID")],
) -> dict:
    """Get email count per local folder for an account."""
    from sqlalchemy import select, func
    from src.mcp.context import get_db
    from src.mcp.helpers import get_account
    from src.db.models import LocalFolder, LocalEmail

    user_id = _user_id()
    async with get_db() as db:
        await get_account(db, user_id, account_id)
        result = await db.execute(
            select(LocalFolder.path, func.count(LocalEmail.id))
            .outerjoin(LocalEmail, LocalEmail.folder_id == LocalFolder.id)
            .where(LocalFolder.account_id == account_id)
            .group_by(LocalFolder.path)
            .order_by(LocalFolder.path)
        )
        rows = result.all()
        total = sum(r[1] for r in rows)
        return {
            "account_id": account_id, "total_emails": total,
            "folders": [{"path": r[0], "count": r[1]} for r in rows],
        }


@mcp.tool(tags={"local"})
async def create_local_folder(
    account_id: Annotated[int, Field(description="Mail account ID")],
    path: Annotated[str, Field(description="Folder path (e.g. 'Archives/2024')")],
) -> dict:
    """Create a local folder (and any missing parent folders)."""
    from sqlalchemy import select
    from src.mcp.context import get_db
    from src.mcp.helpers import get_account
    from src.db.models import LocalFolder

    user_id = _user_id()
    async with get_db() as db:
        await get_account(db, user_id, account_id)

        existing = (await db.execute(
            select(LocalFolder).where(LocalFolder.account_id == account_id, LocalFolder.path == path)
        )).scalar_one_or_none()
        if existing:
            return {"status": "exists", "id": existing.id, "path": path}

        parts = path.split("/")
        for i in range(1, len(parts)):
            ancestor_path = "/".join(parts[:i])
            exists = (await db.execute(
                select(LocalFolder).where(LocalFolder.account_id == account_id, LocalFolder.path == ancestor_path)
            )).scalar_one_or_none()
            if not exists:
                ancestor = LocalFolder(
                    account_id=account_id, name=parts[i-1], path=ancestor_path,
                    parent_path="/".join(parts[:i-1]) if i > 1 else None,
                )
                db.add(ancestor)
                await db.flush()

        new_folder = LocalFolder(
            account_id=account_id, name=parts[-1], path=path,
            parent_path="/".join(parts[:-1]) if len(parts) > 1 else None,
        )
        db.add(new_folder)
        await db.commit()
        await db.refresh(new_folder)
        return {"status": "created", "id": new_folder.id, "path": path}


@mcp.tool(tags={"local"})
async def delete_local_folder(
    account_id: Annotated[int, Field(description="Mail account ID")],
    path: Annotated[str, Field(description="Folder path to delete")],
    recursive: Annotated[bool, Field(description="Also delete subfolders")] = False,
) -> dict:
    """Delete a local folder and all its emails. Use recursive=True to also delete subfolders."""
    from sqlalchemy import select, delete as sa_delete
    from src.mcp.context import get_db
    from src.mcp.helpers import get_account
    from src.db.models import LocalFolder, LocalEmail

    user_id = _user_id()
    async with get_db() as db:
        await get_account(db, user_id, account_id)

        if recursive:
            folders = (await db.execute(
                select(LocalFolder).where(
                    LocalFolder.account_id == account_id,
                    (LocalFolder.path == path) | LocalFolder.path.startswith(path + "/"),
                )
            )).scalars().all()
        else:
            f = (await db.execute(
                select(LocalFolder).where(LocalFolder.account_id == account_id, LocalFolder.path == path)
            )).scalar_one_or_none()
            folders = [f] if f else []

        if not folders:
            raise ToolError(f"Folder '{path}' not found")

        total_deleted = 0
        for folder in folders:
            count = (await db.execute(
                select(LocalEmail.id).where(LocalEmail.folder_id == folder.id)
            )).all()
            total_deleted += len(count)
            await db.execute(sa_delete(LocalEmail).where(LocalEmail.folder_id == folder.id))
            await db.delete(folder)
        await db.commit()
        return {"status": "deleted", "folders_deleted": len(folders), "emails_deleted": total_deleted}


@mcp.tool(tags={"local", "action"})
async def move_local_email(
    email_id: Annotated[int, Field(description="Local email ID")],
    target_folder_path: Annotated[str, Field(description="Target local folder path")],
    account_id: Annotated[int, Field(description="Mail account ID")],
) -> dict:
    """Move a local email to another local folder."""
    from sqlalchemy import select
    from src.mcp.context import get_db
    from src.mcp.helpers import get_account
    from src.db.models import LocalFolder, LocalEmail, MailAccount

    user_id = _user_id()
    async with get_db() as db:
        await get_account(db, user_id, account_id)
        email = (await db.execute(select(LocalEmail).where(LocalEmail.id == email_id))).scalar_one_or_none()
        if not email:
            raise ToolError(f"Email {email_id} not found")

        target = (await db.execute(
            select(LocalFolder).where(LocalFolder.account_id == account_id, LocalFolder.path == target_folder_path)
        )).scalar_one_or_none()
        if not target:
            raise ToolError(f"Target folder '{target_folder_path}' not found")

        old_folder_id = email.folder_id
        email.folder_id = target.id
        await db.commit()
        return {"status": "moved", "email_id": email_id, "target": target_folder_path}


# ---------------------------------------------------------------------------
# DEDUP / CROSS-STORAGE TOOLS
# ---------------------------------------------------------------------------

@mcp.tool(tags={"local", "action"})
async def find_duplicates_local_vs_imap(
    account_id: Annotated[int, Field(description="Mail account ID")],
    dry_run: Annotated[bool, Field(description="If True, only count duplicates without deleting")] = True,
) -> dict:
    """Find (and optionally delete) local emails that also exist on IMAP.
    Compares by Message-ID header across all IMAP folders.

    Use dry_run=True first to see how many duplicates exist.
    Set dry_run=False to actually delete the local duplicates.
    """
    from sqlalchemy import select, func, delete as sa_delete
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account
    from src.db.models import LocalFolder, LocalEmail

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

        # Step 1: Collect all Message-IDs from IMAP
        imap = get_imap(account)
        imap.connect()
        imap_msgids = set()
        folder_scan = {}
        try:
            folders = imap.list_folders()
            for f in folders:
                fname = f["name"]
                try:
                    imap._conn.select(f'"{fname}"')
                    status, data = imap._conn.search(None, "ALL")
                    if status != "OK" or not data[0]:
                        folder_scan[fname] = 0
                        continue
                    uids = data[0].split()
                    folder_count = 0
                    batch_size = 500
                    for i in range(0, len(uids), batch_size):
                        batch = uids[i:i + batch_size]
                        uid_range = b",".join(batch)
                        st, hdr_data = imap._conn.fetch(
                            uid_range.decode(), "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])"
                        )
                        if st == "OK":
                            for item in hdr_data:
                                if isinstance(item, tuple) and len(item) > 1:
                                    hdr = item[1].decode("utf-8", errors="replace")
                                    for line in hdr.splitlines():
                                        if line.lower().startswith("message-id:"):
                                            mid = line.split(":", 1)[1].strip()
                                            if mid:
                                                imap_msgids.add(mid)
                                                folder_count += 1
                    folder_scan[fname] = folder_count
                except Exception as e:
                    logger.warning(f"Dedup: error scanning IMAP folder {fname}: {e}")
                    folder_scan[fname] = -1
        finally:
            imap.disconnect()

        # Step 2: Find local emails whose Message-ID is in the IMAP set
        local_emails = (await db.execute(
            select(LocalEmail.id, LocalEmail.message_id_header, LocalEmail.folder_id)
            .where(
                LocalEmail.folder_id.in_(
                    select(LocalFolder.id).where(LocalFolder.account_id == account_id)
                ),
                LocalEmail.message_id_header.isnot(None),
                LocalEmail.message_id_header != "",
            )
        )).all()

        duplicates = []
        unique_local = 0
        for eid, mid, fid in local_emails:
            if mid in imap_msgids:
                duplicates.append(eid)
            else:
                unique_local += 1

        # Step 3: Optionally delete
        deleted = 0
        if not dry_run and duplicates:
            batch_size = 500
            for i in range(0, len(duplicates), batch_size):
                batch = duplicates[i:i + batch_size]
                await db.execute(sa_delete(LocalEmail).where(LocalEmail.id.in_(batch)))
            await db.commit()
            deleted = len(duplicates)

        return {
            "imap_folders_scanned": len(folder_scan),
            "imap_unique_message_ids": len(imap_msgids),
            "local_total": len(local_emails),
            "duplicates_found": len(duplicates),
            "unique_local_only": unique_local,
            "deleted": deleted,
            "dry_run": dry_run,
        }


@mcp.tool(tags={"local", "action"})
async def find_duplicates_imap_vs_local(
    account_id: Annotated[int, Field(description="Mail account ID")],
) -> dict:
    """Find IMAP emails that also exist in local storage (by Message-ID).
    Returns per-folder counts. Does NOT delete — use delete_email or move_email to act."""
    from sqlalchemy import select
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account
    from src.db.models import LocalFolder, LocalEmail

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

        # Collect all local Message-IDs
        local_msgids = set()
        result = await db.execute(
            select(LocalEmail.message_id_header).where(
                LocalEmail.folder_id.in_(
                    select(LocalFolder.id).where(LocalFolder.account_id == account_id)
                ),
                LocalEmail.message_id_header.isnot(None),
                LocalEmail.message_id_header != "",
            )
        )
        for row in result.all():
            local_msgids.add(row[0])

    # Scan IMAP
    imap = get_imap(account)
    imap.connect()
    try:
        folders = imap.list_folders()
        per_folder = {}
        total_imap = 0
        total_dups = 0
        for f in folders:
            fname = f["name"]
            try:
                imap._conn.select(f'"{fname}"')
                status, data = imap._conn.search(None, "ALL")
                if status != "OK" or not data[0]:
                    continue
                uids = data[0].split()
                folder_total = len(uids)
                folder_dups = 0
                batch_size = 500
                for i in range(0, len(uids), batch_size):
                    batch = uids[i:i + batch_size]
                    uid_range = b",".join(batch)
                    st, hdr_data = imap._conn.fetch(
                        uid_range.decode(), "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])"
                    )
                    if st == "OK":
                        for item in hdr_data:
                            if isinstance(item, tuple) and len(item) > 1:
                                hdr = item[1].decode("utf-8", errors="replace")
                                for line in hdr.splitlines():
                                    if line.lower().startswith("message-id:"):
                                        mid = line.split(":", 1)[1].strip()
                                        if mid and mid in local_msgids:
                                            folder_dups += 1
                total_imap += folder_total
                total_dups += folder_dups
                if folder_dups > 0:
                    per_folder[fname] = {"total": folder_total, "duplicates": folder_dups}
            except Exception as e:
                logger.warning(f"Dedup scan error for {fname}: {e}")
        return {
            "local_unique_message_ids": len(local_msgids),
            "imap_total_emails": total_imap,
            "imap_duplicates_with_local": total_dups,
            "per_folder": per_folder,
        }
    finally:
        imap.disconnect()


@mcp.tool(tags={"local", "action"})
async def copy_local_to_imap(
    account_id: Annotated[int, Field(description="Mail account ID")],
    local_folder_path: Annotated[str, Field(description="Source local folder path")],
    imap_folder: Annotated[str, Field(description="Target IMAP folder")],
    delete_after: Annotated[bool, Field(description="Delete local copy after successful IMAP upload")] = False,
) -> dict:
    """Copy emails from a local folder to an IMAP folder.
    Skips emails already on IMAP (by Message-ID). Optionally deletes local copies."""
    import imaplib
    import email.utils
    import time
    from sqlalchemy import select, delete as sa_delete
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account
    from src.db.models import LocalFolder, LocalEmail

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)
        folder = (await db.execute(
            select(LocalFolder).where(LocalFolder.account_id == account_id, LocalFolder.path == local_folder_path)
        )).scalar_one_or_none()
        if not folder:
            raise ToolError(f"Local folder '{local_folder_path}' not found")

        emails = (await db.execute(
            select(LocalEmail).where(LocalEmail.folder_id == folder.id)
        )).scalars().all()

    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _encode_imap_utf7
        encoded = _encode_imap_utf7(imap_folder)
        try:
            imap._conn.select(f'"{encoded}"')
        except Exception:
            imap._conn.create(f'"{encoded}"')
            imap._conn.subscribe(f'"{encoded}"')
            imap._conn.select(f'"{encoded}"')

        # Fetch existing IMAP message-ids for dedup
        existing = set()
        st, data = imap._conn.search(None, "ALL")
        if st == "OK" and data[0]:
            uids = data[0].split()
            for i in range(0, len(uids), 500):
                batch = uids[i:i+500]
                st2, hdata = imap._conn.fetch(b",".join(batch).decode(), "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")
                if st2 == "OK":
                    for item in hdata:
                        if isinstance(item, tuple) and len(item) > 1:
                            for line in item[1].decode("utf-8", errors="replace").splitlines():
                                if line.lower().startswith("message-id:"):
                                    existing.add(line.split(":", 1)[1].strip())

        uploaded = skipped = errors = 0
        uploaded_ids = []
        for em in emails:
            mid = em.message_id_header or ""
            if mid and mid in existing:
                skipped += 1
                continue
            if not em.raw_message:
                skipped += 1
                continue
            try:
                imap_date = imaplib.Time2Internaldate(time.time())
                if em.date:
                    imap_date = imaplib.Time2Internaldate(em.date.timestamp())
                flags = "\\Seen" if em.seen else ""
                st, _ = imap._conn.append(f'"{encoded}"', flags, imap_date, em.raw_message)
                if st == "OK":
                    uploaded += 1
                    uploaded_ids.append(em.id)
                else:
                    errors += 1
            except Exception:
                errors += 1
    finally:
        imap.disconnect()

    if delete_after and uploaded_ids:
        async with get_db() as db:
            await db.execute(sa_delete(LocalEmail).where(LocalEmail.id.in_(uploaded_ids)))
            await db.commit()

    return {
        "uploaded": uploaded, "skipped": skipped, "errors": errors,
        "deleted_local": len(uploaded_ids) if delete_after else 0,
    }


@mcp.tool(tags={"local", "action"})
async def purge_empty_local_folders(
    account_id: Annotated[int, Field(description="Mail account ID")],
) -> dict:
    """Delete all local folders that contain zero emails (cleanup after dedup)."""
    from sqlalchemy import select, func, delete as sa_delete
    from src.mcp.context import get_db
    from src.mcp.helpers import get_account
    from src.db.models import LocalFolder, LocalEmail

    user_id = _user_id()
    async with get_db() as db:
        await get_account(db, user_id, account_id)

        # Find folders with 0 emails
        result = await db.execute(
            select(LocalFolder.id, LocalFolder.path, func.count(LocalEmail.id))
            .outerjoin(LocalEmail, LocalEmail.folder_id == LocalFolder.id)
            .where(LocalFolder.account_id == account_id)
            .group_by(LocalFolder.id, LocalFolder.path)
        )
        all_folders = result.all()
        empty = [r for r in all_folders if r[2] == 0]

        # Only delete leaf-empty folders (no children with emails)
        empty_paths = {r[1] for r in empty}
        non_empty_paths = {r[1] for r in all_folders if r[2] > 0}

        to_delete = []
        for fid, fpath, _ in empty:
            has_non_empty_child = any(p.startswith(fpath + "/") for p in non_empty_paths)
            if not has_non_empty_child:
                to_delete.append((fid, fpath))

        if to_delete:
            await db.execute(
                sa_delete(LocalFolder).where(LocalFolder.id.in_([t[0] for t in to_delete]))
            )
            await db.commit()

        return {
            "deleted_folders": len(to_delete),
            "deleted_paths": [t[1] for t in to_delete],
        }


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    import os
    import sys

    global USER_ID
    USER_ID = int(os.environ.get("MCP_USER_ID", "0"))
    if USER_ID == 0:
        print("ERROR: Set MCP_USER_ID environment variable", file=sys.stderr)
        sys.exit(1)

    logger.info(f"MailIA MCP server starting for user_id={USER_ID}")
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

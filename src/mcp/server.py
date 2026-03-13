"""
MailIA MCP Server — exposes email management tools for AI assistants.

Run with:  python -m src.mcp.server
Or:        fastmcp run src/mcp/server.py
"""
import json
import logging
import os as _os
import smtplib
import ssl as ssl_mod
import time as _time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Annotated

import redis as _redis
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import Middleware
from pydantic import Field

from src.config import get_settings
from src.imap.manager import _decode_imap_utf7, _encode_imap_utf7

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-mailia")


def _normalize_folder(folder: str) -> str:
    """Ensure folder name is UTF-8. Decodes IMAP UTF-7 if detected."""
    import re
    if re.search(r'&[A-Za-z0-9+,]+-', folder):
        return _decode_imap_utf7(folder)
    return folder


async def _es_delete_docs(user_id: int, account_id: int, folder: str, uids: list[str]):
    """Delete docs from ES, trying both UTF-8 and UTF-7 folder name in doc ID.
    Chunked to handle 10K+ UIDs without ES timeout."""
    try:
        from src.search.indexer import get_es_client, _index_name
        es = await get_es_client()
        index = _index_name(user_id)
        utf7_folder = _encode_imap_utf7(folder)
        ops = []
        for uid in uids:
            ops.append({"delete": {"_index": index, "_id": f"{account_id}-{folder}-{uid}"}})
            if utf7_folder != folder:
                ops.append({"delete": {"_index": index, "_id": f"{account_id}-{utf7_folder}-{uid}"}})
        # Chunk bulk ops (1000 per call) to avoid ES timeouts on large batches
        chunk_size = 1000
        for i in range(0, len(ops), chunk_size):
            await es.bulk(operations=ops[i:i + chunk_size])
        await es.close()
    except Exception as e:
        logger.warning(f"ES cleanup failed: {e}")

# User ID is configured at startup via env var MCP_USER_ID
USER_ID: int = 0

_mcp_prefix = _os.environ.get("MCP_PATH_PREFIX", "")

mcp = FastMCP(
    name="MailIA",
    instructions=(
        "MailIA MCP server — manage emails via IMAP, search in Elasticsearch, "
        "apply AI rules, send emails via SMTP. All operations are scoped to the "
        "authenticated user."
    ),
    sse_path=f"{_mcp_prefix}/sse",
    message_path=f"{_mcp_prefix}/messages/",
)


# --- Tool activity tracking via Redis ---

def _get_redis():
    settings = get_settings()
    return _redis.from_url(settings.redis_url, decode_responses=True)


def _redis_key(user_id: int) -> str:
    return f"mcp:tool_activity:{user_id}"


class ToolActivityMiddleware(Middleware):
    """Publishes tool call start/end events to Redis for real-time UI tracking."""

    async def on_call_tool(self, context, call_next):
        tool_name = context.message.name
        args = context.message.arguments or {}
        user_id = USER_ID
        call_id = f"{tool_name}:{_time.time():.3f}"
        rkey = _redis_key(user_id)

        arg_summary = _summarize_args(tool_name, args)

        try:
            r = _get_redis()
            event = json.dumps({
                "id": call_id, "tool": tool_name, "args": arg_summary,
                "status": "running", "ts": _time.time(),
            })
            r.rpush(rkey, event)
            r.expire(rkey, 600)
        except Exception:
            pass

        try:
            result = await call_next(context)
            status = "done"
        except Exception as e:
            status = f"error: {str(e)[:80]}"
            raise
        finally:
            try:
                r = _get_redis()
                event = json.dumps({
                    "id": call_id, "tool": tool_name, "args": arg_summary,
                    "status": status, "ts": _time.time(),
                })
                r.rpush(rkey, event)
                r.expire(rkey, 600)
            except Exception:
                pass

        return result


def _summarize_args(tool_name: str, args: dict) -> str:
    """Short human-readable summary of tool arguments."""
    if not args:
        return ""
    # Folder path tools (create_folder, delete_folder)
    if "path" in args:
        return args["path"]
    if "folder_name" in args and "account_id" in args and len(args) <= 2:
        return args["folder_name"]
    # Rules-based organize
    if "rules" in args:
        rules = args["rules"]
        count = len(rules) if isinstance(rules, list) else "?"
        return f"{args.get('folder', '?')} → {count} règles"
    if "folder" in args and "account_id" in args:
        parts = [args.get("folder", "")]
        if "target_folder" in args:
            parts.append(f"→ {args['target_folder']}")
        if "query" in args:
            parts.append(f'"{args["query"][:40]}"')
        if "imap_criteria" in args:
            parts.append(args["imap_criteria"][:80])
        if "uids" in args:
            uids = args["uids"]
            count = len(uids) if isinstance(uids, list) else str(uids).count(",") + 1
            parts.append(f"{count} UIDs")
        return " · ".join(parts)
    if "query" in args:
        return f'"{args["query"][:50]}"'
    return ", ".join(f"{k}={str(v)[:50]}" for k, v in list(args.items())[:3])


mcp.add_middleware(ToolActivityMiddleware())


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
    folder: Annotated[str | None, Field(description="Filter by folder (use display name with accents)")] = None,
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
    folder: Annotated[str | None, Field(description="Filter by folder (use display name with accents)")] = None,
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
            from src.imap.manager import _encode_imap_utf7
            utf7 = _encode_imap_utf7(folder)
            if utf7 != folder:
                filters.append({"bool": {"should": [
                    {"term": {"folder": folder}},
                    {"term": {"folder": utf7}},
                ], "minimum_should_match": 1}})
            else:
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
        from src.imap.manager import _decode_imap_utf7
        buckets = result.get("aggregations", {}).get("folders", {}).get("buckets", [])
        folders = {_decode_imap_utf7(b["key"]): b["doc_count"] for b in buckets}
        return {"total": sum(folders.values()), "folders": folders}
    except Exception as e:
        return {"total": 0, "folders": {}, "error": str(e)}
    finally:
        await es.close()


@mcp.tool(tags={"search"})
async def get_senders_stats(
    account_id: Annotated[int | None, Field(description="Filter by account ID")] = None,
    folder: Annotated[str | None, Field(description="Filter by folder (use display name with accents)")] = None,
    min_count: Annotated[int, Field(description="Minimum email count to include a sender")] = 2,
    top_n: Annotated[int, Field(description="Number of top senders to return")] = 100,
) -> dict:
    """Get top email senders with counts — perfect for identifying newsletters, spam, and
    high-volume senders in a folder. Use this FIRST when analyzing folder content for cleanup.
    Returns senders sorted by email count (descending)."""
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
            from src.imap.manager import _encode_imap_utf7
            utf7 = _encode_imap_utf7(folder)
            if utf7 != folder:
                filters.append({"bool": {"should": [
                    {"term": {"folder": folder}},
                    {"term": {"folder": utf7}},
                ], "minimum_should_match": 1}})
            else:
                filters.append({"term": {"folder": folder}})

        query = {"bool": {"filter": filters}} if filters else {"match_all": {}}
        result = await es.search(
            index=index,
            body={
                "size": 0,
                "query": query,
                "aggs": {
                    "senders": {
                        "terms": {"field": "from_addr", "size": top_n, "min_doc_count": min_count,
                                  "order": {"_count": "desc"}},
                    }
                },
            },
        )
        buckets = result.get("aggregations", {}).get("senders", {}).get("buckets", [])
        senders = [{"email": b["key"], "count": b["doc_count"]} for b in buckets]
        total = result["hits"]["total"]["value"]
        return {"total_emails": total, "unique_senders": len(senders), "senders": senders}
    except Exception as e:
        return {"total_emails": 0, "senders": [], "error": str(e)}
    finally:
        await es.close()


# ---------------------------------------------------------------------------
# READ EMAILS (IMAP)
# ---------------------------------------------------------------------------

@mcp.tool(tags={"read"})
async def read_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name (use display name with accents, e.g. 'Éléments supprimés')")],
    uid: Annotated[str, Field(description="Email UID")],
) -> dict:
    """Read the full content of a specific email: headers, body text, and attachment list."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
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
    folder: Annotated[str, Field(description="Folder name (use display name with accents, e.g. 'Éléments supprimés')")] = "INBOX",
    limit: Annotated[int, Field(description="Max emails to list", ge=1, le=100)] = 20,
) -> dict:
    """List recent emails in an IMAP folder with subject, sender, and date."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
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


@mcp.tool(tags={"search"})
async def search_folder(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name (use display name with accents)")] = "INBOX",
    from_addr: Annotated[str | None, Field(description="Search sender name or email (partial match)")] = None,
    to_addr: Annotated[str | None, Field(description="Search recipient (partial match)")] = None,
    subject: Annotated[str | None, Field(description="Search in subject (partial match)")] = None,
    text: Annotated[str | None, Field(description="Full-text search in body")] = None,
    date_from: Annotated[str | None, Field(description="Emails since this date (YYYY-MM-DD)")] = None,
    date_to: Annotated[str | None, Field(description="Emails before this date (YYYY-MM-DD)")] = None,
    limit: Annotated[int, Field(description="Max results to fetch details for (use 0 for count only)", ge=0, le=500)] = 50,
) -> dict:
    """Search emails directly in an IMAP folder using server-side SEARCH.
    Use this to find emails by sender, recipient, subject, or text content.
    Returns total_matches (the real total) and up to `limit` most recent emails with details.
    More reliable than full-text search for finding specific emails."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        imap._conn.select(_imap_quote(folder), readonly=True)

        search_parts = []
        if from_addr:
            search_parts.append(f'FROM "{from_addr}"')
        if to_addr:
            search_parts.append(f'TO "{to_addr}"')
        if subject:
            search_parts.append(f'SUBJECT "{subject}"')
        if text:
            search_parts.append(f'TEXT "{text}"')
        if date_from:
            from datetime import datetime
            d = datetime.strptime(date_from, "%Y-%m-%d")
            search_parts.append(f'SINCE {d.strftime("%d-%b-%Y")}')
        if date_to:
            from datetime import datetime
            d = datetime.strptime(date_to, "%Y-%m-%d")
            search_parts.append(f'BEFORE {d.strftime("%d-%b-%Y")}')

        if not search_parts:
            return {"error": "At least one search filter is required"}

        criteria = ' '.join(search_parts)
        charset = None
        try:
            criteria.encode('ascii')
        except UnicodeEncodeError:
            charset = 'UTF-8'
        status, data = imap._conn.uid("SEARCH", charset, criteria)
        all_uids = data[0].decode().split() if status == "OK" and data[0] else []
        total = len(all_uids)

        if limit == 0:
            return {"folder": folder, "total_matches": total, "count": 0, "emails": []}

        # Take the most recent results
        fetch_uids = all_uids[-limit:] if len(all_uids) > limit else all_uids
        fetch_uids.reverse()

        emails = []
        for u in fetch_uids:
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
        return {"folder": folder, "total_matches": total, "count": len(emails), "emails": emails}
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
            "separator": folders[0]["separator"] if folders else "/",
            "note": "ALWAYS use 'name' (display name) when calling other tools, NEVER use 'imap_raw'.",
            "folders": [
                {"name": f.get("display_name", f["name"]), "imap_raw": f["name"], "separator": f.get("separator", "/")}
                for f in folders
            ],
        }
    finally:
        imap.disconnect()


@mcp.tool(tags={"read"})
async def get_attachment(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name (use display name with accents, e.g. 'Éléments supprimés')")],
    uid: Annotated[str, Field(description="Email UID")],
    attachment_index: Annotated[int, Field(description="Attachment index (0-based)")] = 0,
) -> dict:
    """Get metadata and base64-encoded content of an email attachment."""
    import base64
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
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
    folder: Annotated[str, Field(description="Source folder (use display name with accents)")],
    uid: Annotated[str, Field(description="Email UID")],
    target_folder: Annotated[str, Field(description="Target folder (use display name with accents)")],
) -> dict:
    """Move a single email to a different IMAP folder. Creates the target folder if needed.
    WARNING: For moving multiple emails, ALWAYS use move_emails_bulk instead — it is 50-100x faster."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    target_folder = _normalize_folder(target_folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        ok = imap.move_email(uid, folder, target_folder)
        if not ok:
            raise ToolError(f"Failed to move UID {uid} to {target_folder}")
    finally:
        imap.disconnect()

    # Remove old ES doc (new folder will be indexed on next sync)
    await _es_delete_docs(user_id, account_id, folder, [uid])

    return {"status": "moved", "uid": uid, "from": folder, "to": target_folder}


@mcp.tool(tags={"action"})
async def move_emails_bulk(
    account_id: Annotated[int, Field(description="Mail account ID")],
    moves: Annotated[list[dict], Field(description="List of {folder, uid, target_folder}. "
        "When all emails are from the same folder and going to the same target, "
        "uses optimized batch IMAP operations.")],
) -> dict:
    """Move multiple emails at once. Each move specifies folder, uid, and target_folder.
    Automatically batches same-folder moves for much better performance."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        # Group by (from_folder, to_folder) for batch optimization
        groups: dict[tuple[str, str], list[str]] = {}
        for m in moves:
            key = (_normalize_folder(m["folder"]), _normalize_folder(m["target_folder"]))
            groups.setdefault(key, []).append(m["uid"])

        total_moved = 0
        total_failed = 0
        for (from_folder, to_folder), uids in groups.items():
            result = imap.move_emails_bulk(uids, from_folder, to_folder)
            total_moved += result["moved"]
            total_failed += result["failed"]
    finally:
        imap.disconnect()

    # Remove old ES docs for moved emails (new folder indexed on next sync)
    for (from_folder, _), uids in groups.items():
        await _es_delete_docs(user_id, account_id, from_folder, uids)

    return {"moved": total_moved, "failed": total_failed}


@mcp.tool(tags={"action"})
async def flag_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name (use display name with accents, e.g. 'Éléments supprimés')")],
    uid: Annotated[str, Field(description="Email UID")],
    flag: Annotated[str, Field(description="Flag: important, read, seen, answered, draft")] = "important",
    action: Annotated[str, Field(description="'add' or 'remove'")] = "add",
) -> dict:
    """Add or remove a flag on an email (important, read, seen, etc.)."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
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
    folder: Annotated[str, Field(description="Folder name (use display name with accents, e.g. 'Éléments supprimés')")],
    uid: Annotated[str, Field(description="Email UID")],
) -> dict:
    """Delete a single email (moves to Trash or flags as Deleted).
    WARNING: For deleting multiple emails, ALWAYS use search_and_delete_emails (by criteria)
    or delete_emails_bulk (by UIDs) instead — they are 50-100x faster."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        ok = imap.delete_email(uid, folder)
    finally:
        imap.disconnect()

    if ok:
        await _es_delete_docs(user_id, account_id, folder, [uid])

    return {"status": "deleted" if ok else "failed", "uid": uid, "folder": folder}


@mcp.tool(tags={"action"})
async def delete_emails_bulk(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name (use display name with accents, e.g. 'Éléments supprimés')")],
    uids: Annotated[list[str], Field(description="List of email UIDs to delete")],
) -> dict:
    """Delete multiple emails by UIDs in one batch (50-100x faster than one-by-one).
    All UIDs must be from the same folder. Moves to Trash or flags as Deleted.
    Also removes from search index.
    PREFERRED over delete_email when you already have a list of UIDs.
    If you need to delete by criteria (sender, subject, etc.), use search_and_delete_emails instead."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        result = imap.delete_emails_bulk(uids, folder)
    finally:
        imap.disconnect()

    await _es_delete_docs(user_id, account_id, folder, uids)

    return result


@mcp.tool(tags={"action"})
async def search_and_delete_emails(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name (use display name with accents, e.g. 'Éléments supprimés')")],
    imap_criteria: Annotated[str, Field(description=(
        "IMAP SEARCH criteria string. Examples: "
        "'FROM \"newsletter@example.com\"' — single sender, "
        "'OR FROM \"news@a.com\" FROM \"news@b.com\"' — multiple senders, "
        "'SUBJECT \"newsletter\"' — by subject, "
        "'FROM \"@darty.com\"' — by domain, "
        "'OR (OR FROM \"a@x.com\" FROM \"b@x.com\") FROM \"c@y.com\"' — 3+ senders"
    ))],
    max_delete: Annotated[int, Field(description="Maximum emails to delete per call (safety limit, default 1000)")] = 1000,
) -> dict:
    """BEST tool for bulk cleanup. Searches directly in IMAP and deletes ALL matches in one call.
    Use this to delete newsletters, spam, promos, or any emails matching a pattern.
    Much faster and more reliable than search + delete separately.
    Examples: delete all emails from a sender, delete all with "newsletter" in subject, etc.
    Supports standard IMAP SEARCH syntax including OR for multiple criteria."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    folder = _normalize_folder(folder)
    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        imap._conn.select(_imap_quote(folder))
        status, data = imap._conn.uid("SEARCH", None, imap_criteria)
        if status != "OK" or not data[0]:
            return {"found": 0, "deleted": 0, "failed": 0}
        uids = data[0].decode().split()
        total_found = len(uids)
        # Apply safety limit
        uids = uids[:max_delete]
        result = imap.delete_emails_bulk(uids, folder)
        result["found"] = total_found
        result["limited_to"] = max_delete if total_found > max_delete else None
    finally:
        imap.disconnect()

    await _es_delete_docs(user_id, account_id, folder, uids)

    return result


@mcp.tool(tags={"action"})
async def search_and_move_emails(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Source folder (use display name with accents, e.g. 'Éléments supprimés')")],
    target_folder: Annotated[str, Field(description="Destination folder (use display name with accents)")],
    imap_criteria: Annotated[str, Field(description=(
        "IMAP SEARCH criteria string. Examples: "
        "'FROM \"newsletter@example.com\"' — single sender, "
        "'OR FROM \"news@a.com\" FROM \"news@b.com\"' — multiple senders, "
        "'SUBJECT \"newsletter\"' — by subject, "
        "'FROM \"@darty.com\"' — by domain, "
        "'OR (OR FROM \"a@x.com\" FROM \"b@x.com\") FROM \"c@y.com\"' — 3+ senders"
    ))],
    max_move: Annotated[int, Field(description="Maximum emails to move per call (safety limit, default 1000)")] = 1000,
) -> dict:
    """BEST tool for bulk organization. Searches directly in IMAP and moves ALL matches to target folder in one call.
    Use this to organize emails by sender, subject, or any pattern — much faster than list + move separately.
    Creates the target folder automatically if it doesn't exist.
    Supports standard IMAP SEARCH syntax including OR for multiple criteria."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    folder = _normalize_folder(folder)
    target_folder = _normalize_folder(target_folder)
    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        imap._conn.select(_imap_quote(folder))
        status, data = imap._conn.uid("SEARCH", None, imap_criteria)
        if status != "OK" or not data[0]:
            return {"found": 0, "moved": 0, "failed": 0}
        uids = data[0].decode().split()
        total_found = len(uids)
        uids = uids[:max_move]
        result = imap.move_emails_bulk(uids, folder, target_folder)
        result["found"] = total_found
        result["limited_to"] = max_move if total_found > max_move else None
    finally:
        imap.disconnect()

    await _es_delete_docs(user_id, account_id, folder, uids)

    return result


@mcp.tool(tags={"action"})
async def organize_emails(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Source folder to organize (use display name with accents)")],
    rules: Annotated[list[dict], Field(description=(
        "List of sorting rules. Each rule is {\"criteria\": \"IMAP SEARCH string\", \"target_folder\": \"destination\"}. "
        "Examples: ["
        "{\"criteria\": \"FROM \\\"@darty.com\\\"\", \"target_folder\": \"PRO/Commerce\"}, "
        "{\"criteria\": \"OR FROM \\\"@newsletter.fr\\\" SUBJECT \\\"newsletter\\\"\", \"target_folder\": \"Newsletters\"}, "
        "{\"criteria\": \"FROM \\\"@banque.fr\\\"\", \"target_folder\": \"Finances\"}"
        "]. Rules are applied sequentially — emails matched by an earlier rule are not re-matched by later ones."
    ))],
    max_per_rule: Annotated[int, Field(description="Maximum emails to move per rule (default 1000)")] = 1000,
) -> dict:
    """ULTIMATE tool for bulk email organization. Applies MULTIPLE sorting rules in ONE call with a single IMAP connection.
    Instead of calling search_and_move_emails 20 times (20 tool calls), pass all 20 rules here (1 tool call).
    Each rule searches in IMAP and moves all matches to the target folder.
    Creates target folders automatically. Rules run sequentially so already-moved emails are skipped."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    if not rules:
        raise ToolError("At least one rule is required")

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    folder = _normalize_folder(folder)
    imap = get_imap(account)
    imap.connect()

    results = []
    all_es_deletes: list[tuple[str, list[str]]] = []  # (folder, uids) pairs
    try:
        from src.imap.manager import _imap_quote
        for rule in rules:
            criteria = rule.get("criteria", "")
            target = _normalize_folder(rule.get("target_folder", ""))
            if not criteria or not target:
                results.append({"criteria": criteria, "target_folder": target, "error": "missing criteria or target_folder"})
                continue

            # Re-SELECT source folder each iteration (UIDs shift after EXPUNGE)
            imap._conn.select(_imap_quote(folder))
            status, data = imap._conn.uid("SEARCH", None, criteria)
            if status != "OK" or not data[0]:
                results.append({"criteria": criteria, "target_folder": target, "found": 0, "moved": 0, "failed": 0})
                continue

            uids = data[0].decode().split()
            total_found = len(uids)
            uids = uids[:max_per_rule]
            result = imap.move_emails_bulk(uids, folder, target)
            result["criteria"] = criteria
            result["target_folder"] = target
            result["found"] = total_found
            result["limited_to"] = max_per_rule if total_found > max_per_rule else None
            results.append(result)
            all_es_deletes.append((folder, uids))
    finally:
        imap.disconnect()

    # ES cleanup for all moved emails
    for src_folder, uids in all_es_deletes:
        await _es_delete_docs(user_id, account_id, src_folder, uids)

    total_moved = sum(r.get("moved", 0) for r in results)
    total_failed = sum(r.get("failed", 0) for r in results)
    return {"total_moved": total_moved, "total_failed": total_failed, "rules_applied": len(results), "details": results}


@mcp.tool(tags={"action"})
async def create_folder(
    account_id: Annotated[int, Field(description="Mail account ID")],
    path: Annotated[str, Field(description=(
        "Full folder path using '/' as separator. Examples: "
        "'Archives' (root level), "
        "'Éléments supprimés/PRO' (subfolder of Éléments supprimés), "
        "'Éléments supprimés/PRO/Clients' (nested). "
        "Use display names with accents. The server resolves the correct IMAP separator automatically."
    ))],
) -> dict:
    """Create a new IMAP folder. Use '/' as separator in the path — the tool
    automatically converts to the correct IMAP separator (which may differ per hierarchy)."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    path = _normalize_folder(path)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        folders = imap.list_folders()

        # Split user path by '/' → e.g. ["Éléments supprimés", "TEST"]
        parts = path.split("/")
        imap_sep = "/"  # default

        if len(parts) > 1:
            parent_display = parts[0]
            # Detect ACTUAL separator by inspecting existing child folders.
            # OVH reports '/' in LIST but uses '.' for some folder hierarchies.
            for f in folders:
                display = f.get("display_name", f["name"])
                raw = f.get("name", "")
                for sep in [".", "/"]:
                    if display.startswith(parent_display + sep) or raw.startswith(parent_display + sep):
                        imap_sep = sep
                        break
                if imap_sep != "/":
                    break

        imap_path = imap_sep.join(parts)
        ok = imap.create_folder(imap_path)
        return {"status": "created" if ok else "failed", "folder": path, "imap_path": imap_path, "separator_used": imap_sep}
    finally:
        imap.disconnect()


@mcp.tool(tags={"action"})
async def delete_folder(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder_name: Annotated[str, Field(description="Full folder path to delete (use display name with accents). Delete deepest subfolders first.")],
) -> dict:
    """Delete an IMAP folder. The folder should be empty. Delete children before parents."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder_name = _normalize_folder(folder_name)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        ok = imap.delete_folder(folder_name)
        return {"status": "deleted" if ok else "failed", "folder": folder_name}
    finally:
        imap.disconnect()


@mcp.tool(tags={"action"})
async def rename_folder(
    account_id: Annotated[int, Field(description="Mail account ID")],
    old_name: Annotated[str, Field(description="Current folder name (use display name with accents)")],
    new_name: Annotated[str, Field(description="New folder name")],
) -> dict:
    """Rename an IMAP folder."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    old_name = _normalize_folder(old_name)
    new_name = _normalize_folder(new_name)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        ok = imap.rename_folder(old_name, new_name)
        return {"status": "renamed" if ok else "failed", "old_name": old_name, "new_name": new_name}
    finally:
        imap.disconnect()


@mcp.tool(tags={"action"})
async def mark_read(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name (use display name with accents)")],
    uids: Annotated[list[str], Field(description="Email UIDs to mark as read")],
) -> dict:
    """Mark one or more emails as read."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        imap._conn.select(_imap_quote(folder))
        uid_set = ",".join(uids)
        status, _ = imap._conn.uid("STORE", uid_set, "+FLAGS", "\\Seen")
        ok = status == "OK"
        return {"status": "ok" if ok else "failed", "count": len(uids)}
    finally:
        imap.disconnect()


@mcp.tool(tags={"action"})
async def mark_unread(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name (use display name with accents)")],
    uids: Annotated[list[str], Field(description="Email UIDs to mark as unread")],
) -> dict:
    """Mark one or more emails as unread."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        imap._conn.select(_imap_quote(folder))
        uid_set = ",".join(uids)
        status, _ = imap._conn.uid("STORE", uid_set, "-FLAGS", "\\Seen")
        ok = status == "OK"
        return {"status": "ok" if ok else "failed", "count": len(uids)}
    finally:
        imap.disconnect()


@mcp.tool(tags={"action"})
async def unflag_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name (use display name with accents)")],
    uid: Annotated[str, Field(description="Email UID")],
) -> dict:
    """Remove the flagged/starred status from an email."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        ok = imap.unflag_email(uid, folder, "important")
        return {"status": "ok" if ok else "failed", "uid": uid}
    finally:
        imap.disconnect()


@mcp.tool(tags={"action"})
async def archive_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Source folder name")],
    uids: Annotated[list[str], Field(description="Email UIDs to archive")],
) -> dict:
    """Archive emails by moving them to the Archive folder."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        archive_names = ["Archive", "INBOX.Archive", "Archives", "INBOX.Archives"]
        folders = imap.list_folders()
        folder_names = [f["name"] for f in folders]
        archive_folder = None
        for a in archive_names:
            if a in folder_names:
                archive_folder = a
                break
        if not archive_folder:
            archive_folder = "Archive"
            imap.create_folder(archive_folder)

        result = imap.move_emails_bulk(uids, folder, archive_folder)
        return {"status": "archived", "archive_folder": archive_folder, **result}
    finally:
        imap.disconnect()


@mcp.tool(tags={"read"})
async def count_unread(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folders: Annotated[list[str] | None, Field(description="Folder names to check (null = all folders)")] = None,
) -> dict:
    """Count unread emails per folder using IMAP STATUS."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        if not folders:
            all_folders = imap.list_folders()
            folders = [f["name"] for f in all_folders]

        results = {}
        total = 0
        for f in folders:
            try:
                status, data = imap._conn.status(_imap_quote(f), "(UNSEEN MESSAGES)")
                if status == "OK" and data[0]:
                    import re
                    m = re.search(rb'UNSEEN (\d+)', data[0])
                    t = re.search(rb'MESSAGES (\d+)', data[0])
                    unseen = int(m.group(1)) if m else 0
                    msgs = int(t.group(1)) if t else 0
                    if unseen > 0:
                        results[f] = {"unread": unseen, "total": msgs}
                        total += unseen
            except Exception:
                pass
        return {"folders_with_unread": results, "total_unread": total}
    finally:
        imap.disconnect()


@mcp.tool(tags={"read"})
async def get_thread(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name (use display name with accents)")],
    uid: Annotated[str, Field(description="UID of any email in the thread")],
) -> dict:
    """Get a complete email thread/conversation by following References and In-Reply-To headers."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        imap._conn.select(_imap_quote(folder), readonly=True)

        # Fetch the target email's Message-ID, References, In-Reply-To, Subject
        status, data = imap._conn.uid("FETCH", uid, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID REFERENCES IN-REPLY-TO SUBJECT)])")
        if status != "OK" or not data or data[0] is None:
            raise ToolError(f"Email UID {uid} not found")

        import email as email_mod
        header_bytes = data[0][1] if isinstance(data[0], tuple) else b""
        msg = email_mod.message_from_bytes(header_bytes)
        message_id = msg.get("Message-ID", "").strip()
        references = msg.get("References", "").split()
        in_reply_to = msg.get("In-Reply-To", "").strip()
        subject = msg.get("Subject", "")

        # Collect all message IDs in the thread
        thread_ids = set()
        if message_id:
            thread_ids.add(message_id)
        thread_ids.update(references)
        if in_reply_to:
            thread_ids.add(in_reply_to)

        # Search by subject (stripped of Re:/Fwd:) as fallback
        import re
        base_subject = re.sub(r'^(Re|Fwd|Fw|Tr)\s*:\s*', '', subject, flags=re.IGNORECASE).strip()

        # Search for related emails by HEADER references or subject
        found_uids = set()
        found_uids.add(uid)

        for mid in thread_ids:
            if not mid:
                continue
            clean = mid.strip("<>")
            try:
                status, sdata = imap._conn.uid("SEARCH", None, f'HEADER Message-ID "<{clean}>"')
                if status == "OK" and sdata[0]:
                    found_uids.update(sdata[0].decode().split())
                status, sdata = imap._conn.uid("SEARCH", None, f'HEADER References "<{clean}>"')
                if status == "OK" and sdata[0]:
                    found_uids.update(sdata[0].decode().split())
            except Exception:
                pass

        # Also search by base subject for broader matching
        if base_subject and len(found_uids) < 3:
            try:
                safe_subj = base_subject[:60].replace('"', '')
                status, sdata = imap._conn.uid("SEARCH", None, f'SUBJECT "{safe_subj}"')
                if status == "OK" and sdata[0]:
                    found_uids.update(sdata[0].decode().split())
            except Exception:
                pass

        # Fetch all thread emails
        thread = []
        for u in sorted(found_uids, key=lambda x: int(x)):
            try:
                ctx = imap.fetch_email(u, folder)
                if ctx:
                    thread.append({
                        "uid": ctx.uid, "from": ctx.from_addr, "to": ctx.to_addr,
                        "subject": ctx.subject, "date": ctx.date,
                        "body_preview": ctx.body_text[:500] if ctx.body_text else "",
                    })
            except Exception:
                pass

        return {"thread_size": len(thread), "emails": thread}
    finally:
        imap.disconnect()


@mcp.tool(tags={"read"})
async def get_email_headers(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name")],
    uid: Annotated[str, Field(description="Email UID")],
) -> dict:
    """Get only email headers (fast, no body download). Useful for checking SPF/DKIM/DMARC, routing, etc."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        imap._conn.select(_imap_quote(folder), readonly=True)
        status, data = imap._conn.uid("FETCH", uid, "(BODY.PEEK[HEADER])")
        if status != "OK" or not data or data[0] is None:
            raise ToolError(f"Email UID {uid} not found")

        import email as email_mod
        header_bytes = data[0][1] if isinstance(data[0], tuple) else b""
        msg = email_mod.message_from_bytes(header_bytes)

        headers = {}
        for key in msg.keys():
            val = msg.get_all(key)
            headers[key] = val[0] if len(val) == 1 else val
        return {"uid": uid, "headers": headers}
    finally:
        imap.disconnect()


@mcp.tool(tags={"search"})
async def search_cross_folder(
    account_id: Annotated[int, Field(description="Mail account ID")],
    from_addr: Annotated[str | None, Field(description="Search sender (partial match)")] = None,
    to_addr: Annotated[str | None, Field(description="Search recipient (partial match)")] = None,
    subject: Annotated[str | None, Field(description="Search in subject (partial match)")] = None,
    text: Annotated[str | None, Field(description="Full-text search in body")] = None,
    date_from: Annotated[str | None, Field(description="Emails since (YYYY-MM-DD)")] = None,
    date_to: Annotated[str | None, Field(description="Emails before (YYYY-MM-DD)")] = None,
    limit_per_folder: Annotated[int, Field(description="Max results per folder", ge=1, le=50)] = 10,
) -> dict:
    """Search across ALL folders at once. Returns matches grouped by folder."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        folders = imap.list_folders()

        search_parts = []
        if from_addr:
            search_parts.append(f'FROM "{from_addr}"')
        if to_addr:
            search_parts.append(f'TO "{to_addr}"')
        if subject:
            search_parts.append(f'SUBJECT "{subject}"')
        if text:
            search_parts.append(f'TEXT "{text}"')
        if date_from:
            from datetime import datetime
            d = datetime.strptime(date_from, "%Y-%m-%d")
            search_parts.append(f'SINCE {d.strftime("%d-%b-%Y")}')
        if date_to:
            from datetime import datetime
            d = datetime.strptime(date_to, "%Y-%m-%d")
            search_parts.append(f'BEFORE {d.strftime("%d-%b-%Y")}')

        if not search_parts:
            return {"error": "At least one search filter is required"}

        criteria = ' '.join(search_parts)
        charset = None
        try:
            criteria.encode('ascii')
        except UnicodeEncodeError:
            charset = 'UTF-8'

        results = {}
        grand_total = 0

        for f_info in folders:
            fname = f_info["name"]
            try:
                imap._conn.select(_imap_quote(fname), readonly=True)
                status, data = imap._conn.uid("SEARCH", charset, criteria)
                found = data[0].decode().split() if status == "OK" and data[0] else []
                if not found:
                    continue
                grand_total += len(found)
                recent = found[-limit_per_folder:]
                recent.reverse()
                emails = []
                for u in recent:
                    try:
                        ctx = imap.fetch_email(u, fname)
                        if ctx:
                            emails.append({
                                "uid": ctx.uid, "from": ctx.from_addr,
                                "subject": ctx.subject, "date": ctx.date,
                            })
                    except Exception:
                        pass
                results[fname] = {"total_in_folder": len(found), "emails": emails}
            except Exception:
                pass

        return {"total_matches": grand_total, "folders": results}
    finally:
        imap.disconnect()


@mcp.tool(tags={"read"})
async def list_drafts(
    account_id: Annotated[int, Field(description="Mail account ID")],
    limit: Annotated[int, Field(description="Max drafts to list", ge=1, le=50)] = 20,
) -> dict:
    """List drafts in the Drafts folder."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        draft_names = ["Drafts", "INBOX.Drafts", "Draft", "INBOX.Draft", "Brouillons", "INBOX.Brouillons"]
        folders = imap.list_folders()
        folder_names = [f["name"] for f in folders]
        draft_folder = None
        for d in draft_names:
            if d in folder_names:
                draft_folder = d
                break
        if not draft_folder:
            return {"folder": "Drafts", "count": 0, "drafts": []}

        uids = imap.get_uids(draft_folder)
        recent = uids[-limit:] if len(uids) > limit else uids
        recent.reverse()

        drafts = []
        for u in recent:
            try:
                ctx = imap.fetch_email(u, draft_folder)
                if ctx:
                    drafts.append({
                        "uid": ctx.uid, "to": ctx.to_addr,
                        "subject": ctx.subject, "date": ctx.date,
                        "body_preview": ctx.body_text[:200] if ctx.body_text else "",
                    })
            except Exception:
                pass
        return {"folder": draft_folder, "count": len(drafts), "drafts": drafts}
    finally:
        imap.disconnect()


@mcp.tool(tags={"action"})
async def delete_draft(
    account_id: Annotated[int, Field(description="Mail account ID")],
    uid: Annotated[str, Field(description="Draft UID to delete")],
) -> dict:
    """Delete a draft from the Drafts folder (permanent deletion, not move to trash)."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        draft_names = ["Drafts", "INBOX.Drafts", "Draft", "INBOX.Draft", "Brouillons", "INBOX.Brouillons"]
        folders = imap.list_folders()
        folder_names = [f["name"] for f in folders]
        draft_folder = "Drafts"
        for d in draft_names:
            if d in folder_names:
                draft_folder = d
                break

        imap._conn.select(_imap_quote(draft_folder))
        status, _ = imap._conn.uid("STORE", uid, "+FLAGS", "\\Deleted")
        if status == "OK":
            imap._conn.expunge()
            return {"status": "deleted", "uid": uid}
        return {"status": "failed", "uid": uid}
    finally:
        imap.disconnect()


@mcp.tool(tags={"send"})
async def update_draft(
    account_id: Annotated[int, Field(description="Mail account ID")],
    old_uid: Annotated[str, Field(description="UID of existing draft to replace")],
    to: Annotated[list[str], Field(description="Recipient email addresses")] = [],
    subject: Annotated[str, Field(description="Email subject")] = "",
    body: Annotated[str, Field(description="Email body")] = "",
) -> dict:
    """Update an existing draft by deleting the old one and saving a new one."""
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
        from src.imap.manager import _imap_quote
        draft_names = ["Drafts", "INBOX.Drafts", "Draft", "INBOX.Draft", "Brouillons", "INBOX.Brouillons"]
        folders = imap.list_folders()
        folder_names = [f["name"] for f in folders]
        draft_folder = "Drafts"
        for d in draft_names:
            if d in folder_names:
                draft_folder = d
                break

        # Delete old draft
        imap._conn.select(_imap_quote(draft_folder))
        imap._conn.uid("STORE", old_uid, "+FLAGS", "\\Deleted")
        imap._conn.expunge()

        # Save new draft
        ok = imap.save_draft(msg.as_bytes())
        return {"status": "updated" if ok else "failed"}
    finally:
        imap.disconnect()


@mcp.tool(tags={"search"})
async def email_analytics(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name")] = "INBOX",
    date_from: Annotated[str | None, Field(description="Start date (YYYY-MM-DD)")] = None,
    date_to: Annotated[str | None, Field(description="End date (YYYY-MM-DD)")] = None,
    group_by: Annotated[str, Field(description="Group by: 'day', 'sender', or 'both'")] = "both",
) -> dict:
    """Get email statistics: count per day and/or per sender for a date range."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        imap._conn.select(_imap_quote(folder), readonly=True)

        search_parts = []
        if date_from:
            from datetime import datetime
            d = datetime.strptime(date_from, "%Y-%m-%d")
            search_parts.append(f'SINCE {d.strftime("%d-%b-%Y")}')
        if date_to:
            from datetime import datetime
            d = datetime.strptime(date_to, "%Y-%m-%d")
            search_parts.append(f'BEFORE {d.strftime("%d-%b-%Y")}')

        criteria = ' '.join(search_parts) if search_parts else 'ALL'
        status, data = imap._conn.uid("SEARCH", None, criteria)
        uids = data[0].decode().split() if status == "OK" and data[0] else []

        by_day = {}
        by_sender = {}
        total = len(uids)

        # Fetch headers only for performance (ENVELOPE)
        for u in uids[-500:]:  # Cap at 500 for performance
            try:
                ctx = imap.fetch_email(u, folder)
                if ctx:
                    day = ctx.date[:10] if ctx.date else "unknown"
                    sender = ctx.from_addr or "unknown"
                    if group_by in ("day", "both"):
                        by_day[day] = by_day.get(day, 0) + 1
                    if group_by in ("sender", "both"):
                        by_sender[sender] = by_sender.get(sender, 0) + 1
            except Exception:
                pass

        result = {"folder": folder, "total": total}
        if group_by in ("day", "both"):
            result["by_day"] = dict(sorted(by_day.items(), reverse=True)[:30])
        if group_by in ("sender", "both"):
            result["top_senders"] = dict(sorted(by_sender.items(), key=lambda x: -x[1])[:20])
        return result
    finally:
        imap.disconnect()


@mcp.tool(tags={"read"})
async def export_emails(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name")],
    uids: Annotated[list[str], Field(description="Email UIDs to export")],
    format: Annotated[str, Field(description="Export format: 'json' or 'eml'")] = "json",
) -> dict:
    """Export emails in JSON or EML format."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        exports = []
        for u in uids[:50]:
            try:
                if format == "eml":
                    raw = imap.fetch_raw(u, folder)
                    if raw:
                        import base64
                        exports.append({"uid": u, "eml_base64": base64.b64encode(raw).decode()})
                else:
                    ctx = imap.fetch_email(u, folder)
                    if ctx:
                        exports.append({
                            "uid": ctx.uid, "from": ctx.from_addr, "to": ctx.to_addr,
                            "subject": ctx.subject, "date": ctx.date,
                            "body": ctx.body_text, "has_attachments": ctx.has_attachments,
                            "attachment_names": ctx.attachment_names,
                        })
            except Exception:
                pass
        return {"format": format, "count": len(exports), "emails": exports}
    finally:
        imap.disconnect()


@mcp.tool(tags={"read"})
async def validate_email_address(
    email_address: Annotated[str, Field(description="Email address to validate")],
) -> dict:
    """Validate an email address format and check if the domain has MX records."""
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    format_ok = bool(re.match(pattern, email_address))

    domain = email_address.split("@")[-1] if "@" in email_address else ""
    mx_ok = False
    mx_records = []

    if domain:
        try:
            import subprocess
            result = subprocess.run(
                ["dig", "+short", "MX", domain],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                mx_records = [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]
                mx_ok = len(mx_records) > 0
        except Exception:
            pass

    return {
        "email": email_address,
        "format_valid": format_ok,
        "domain": domain,
        "mx_valid": mx_ok,
        "mx_records": mx_records[:5],
    }


@mcp.tool(tags={"admin"})
async def test_smtp(
    account_id: Annotated[int, Field(description="Mail account ID")],
) -> dict:
    """Test SMTP connection for a mail account."""
    from src.mcp.context import get_db
    from src.mcp.helpers import get_account
    from src.security import decrypt_value

    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    if not account.smtp_host:
        return {"status": "error", "message": "SMTP not configured"}

    try:
        server = _smtp_connect(account)
        smtp_password = decrypt_value(account.smtp_password_encrypted) if account.smtp_password_encrypted else decrypt_value(account.imap_password_encrypted)
        server.login(account.smtp_user or account.imap_user, smtp_password)
        server.quit()
        return {"status": "ok", "host": account.smtp_host, "port": account.smtp_port}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@mcp.tool(tags={"read"})
async def spam_analysis(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name")],
    uid: Annotated[str, Field(description="Email UID to analyze")],
) -> dict:
    """Analyze email headers for spam/phishing indicators (SPF, DKIM, DMARC, suspicious patterns)."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        imap._conn.select(_imap_quote(folder), readonly=True)
        status, data = imap._conn.uid("FETCH", uid, "(BODY.PEEK[HEADER])")
        if status != "OK" or not data or data[0] is None:
            raise ToolError(f"Email UID {uid} not found")

        import email as email_mod
        header_bytes = data[0][1] if isinstance(data[0], tuple) else b""
        msg = email_mod.message_from_bytes(header_bytes)

        checks = {}

        # SPF
        spf = msg.get("Received-SPF", "") or ""
        auth_results = msg.get("Authentication-Results", "") or ""
        checks["spf"] = "pass" if "pass" in spf.lower() else ("fail" if "fail" in spf.lower() else "unknown")

        # DKIM
        dkim_sig = msg.get("DKIM-Signature", "")
        checks["dkim_signed"] = bool(dkim_sig)
        checks["dkim"] = "pass" if "dkim=pass" in auth_results.lower() else ("fail" if "dkim=fail" in auth_results.lower() else "unknown")

        # DMARC
        checks["dmarc"] = "pass" if "dmarc=pass" in auth_results.lower() else ("fail" if "dmarc=fail" in auth_results.lower() else "unknown")

        # Suspicious indicators
        from_addr = msg.get("From", "")
        reply_to = msg.get("Reply-To", "")
        return_path = msg.get("Return-Path", "")

        warnings = []
        if reply_to and from_addr and reply_to.lower() != from_addr.lower():
            warnings.append("Reply-To differs from From address")
        if return_path and from_addr and return_path.lower() not in from_addr.lower():
            warnings.append("Return-Path differs from From address")
        if msg.get("X-Spam-Flag", "").upper() == "YES":
            warnings.append("Flagged as spam by server")
        spam_score = msg.get("X-Spam-Score", "")
        if spam_score:
            checks["spam_score"] = spam_score

        checks["warnings"] = warnings
        checks["auth_results"] = auth_results[:300]

        score = 0
        if checks["spf"] == "pass": score += 1
        if checks["dkim"] == "pass": score += 1
        if checks["dmarc"] == "pass": score += 1
        if not warnings: score += 1
        checks["trust_score"] = f"{score}/4"

        return {"uid": uid, "analysis": checks}
    finally:
        imap.disconnect()


@mcp.tool(tags={"search"})
async def scan_for_spam(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name")] = "INBOX",
    limit: Annotated[int, Field(description="Number of recent emails to scan", ge=1, le=200)] = 50,
) -> dict:
    """Scan recent emails in a folder for spam/phishing by analyzing headers in bulk.
    Returns a ranked list of suspicious emails with trust scores.
    Use this when asked to find spam, phishing, or suspicious emails."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        from src.imap.manager import _imap_quote
        import email as email_mod

        imap._conn.select(_imap_quote(folder), readonly=True)
        status, data = imap._conn.uid("SEARCH", None, "ALL")
        all_uids = data[0].decode().split() if status == "OK" and data[0] else []
        scan_uids = all_uids[-limit:] if len(all_uids) > limit else all_uids
        scan_uids.reverse()

        suspicious = []
        clean_count = 0

        for u in scan_uids:
            try:
                status, hdata = imap._conn.uid("FETCH", u, "(BODY.PEEK[HEADER.FIELDS (FROM REPLY-TO RETURN-PATH SUBJECT AUTHENTICATION-RESULTS RECEIVED-SPF X-SPAM-FLAG X-SPAM-SCORE DKIM-SIGNATURE)])")
                if status != "OK" or not hdata or hdata[0] is None:
                    continue

                header_bytes = hdata[0][1] if isinstance(hdata[0], tuple) else b""
                msg = email_mod.message_from_bytes(header_bytes)

                from_raw = msg.get("From", "")
                from_name, from_email = email_mod.utils.parseaddr(from_raw)
                subject = msg.get("Subject", "")
                reply_to = msg.get("Reply-To", "")
                return_path = msg.get("Return-Path", "")
                auth_results = (msg.get("Authentication-Results", "") or "").lower()
                spf = (msg.get("Received-SPF", "") or "").lower()
                spam_flag = (msg.get("X-Spam-Flag", "") or "").upper()
                spam_score_raw = msg.get("X-Spam-Score", "")
                has_dkim = bool(msg.get("DKIM-Signature", ""))

                # Score: 0 = very suspicious, 4 = clean
                trust = 0
                reasons = []

                # SPF check
                if "pass" in spf or "spf=pass" in auth_results:
                    trust += 1
                elif "fail" in spf or "spf=fail" in auth_results:
                    reasons.append("SPF fail")

                # DKIM check
                if has_dkim and "dkim=pass" in auth_results:
                    trust += 1
                elif "dkim=fail" in auth_results:
                    reasons.append("DKIM fail")
                elif not has_dkim:
                    reasons.append("No DKIM signature")

                # DMARC check
                if "dmarc=pass" in auth_results:
                    trust += 1
                elif "dmarc=fail" in auth_results:
                    reasons.append("DMARC fail")

                # Reply-To / Return-Path mismatch
                _, reply_email = email_mod.utils.parseaddr(reply_to)
                _, return_email = email_mod.utils.parseaddr(return_path)
                if reply_email and from_email and reply_email.lower() != from_email.lower():
                    reasons.append(f"Reply-To mismatch: {reply_email}")
                if return_email and from_email and return_email.lower() != from_email.lower():
                    reasons.append(f"Return-Path mismatch: {return_email}")

                # Server spam flag
                if spam_flag == "YES":
                    reasons.append("Server flagged as spam")
                if spam_score_raw:
                    try:
                        ss = float(spam_score_raw)
                        if ss > 5:
                            reasons.append(f"High spam score: {ss}")
                    except ValueError:
                        pass

                if not reasons:
                    trust += 1

                if trust < 3 or reasons:
                    suspicious.append({
                        "uid": u,
                        "from": from_raw[:80],
                        "subject": subject[:100],
                        "trust_score": f"{trust}/4",
                        "reasons": reasons,
                    })
                else:
                    clean_count += 1

            except Exception:
                pass

        # Sort by trust score ascending (most suspicious first)
        suspicious.sort(key=lambda x: x["trust_score"])

        return {
            "folder": folder,
            "scanned": len(scan_uids),
            "suspicious_count": len(suspicious),
            "clean_count": clean_count,
            "suspicious_emails": suspicious[:50],
        }
    finally:
        imap.disconnect()


@mcp.tool(tags={"read"})
async def extract_calendar_events(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name")],
    uid: Annotated[str, Field(description="Email UID")],
) -> dict:
    """Extract calendar events (ICS/iCalendar) from email attachments or body."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        raw = imap.fetch_raw(uid, folder)
        if not raw:
            raise ToolError(f"Email UID {uid} not found")

        import email as email_mod
        msg = email_mod.message_from_bytes(raw)
        events = []

        for part in msg.walk():
            ct = part.get_content_type()
            if ct in ("text/calendar", "application/ics"):
                payload = part.get_payload(decode=True)
                if payload:
                    ics_text = payload.decode("utf-8", errors="replace")
                    # Parse basic VEVENT fields
                    event = {}
                    in_event = False
                    for line in ics_text.split("\n"):
                        line = line.strip()
                        if line == "BEGIN:VEVENT":
                            in_event = True
                            event = {}
                        elif line == "END:VEVENT":
                            if event:
                                events.append(event)
                            in_event = False
                        elif in_event and ":" in line:
                            key, _, val = line.partition(":")
                            key = key.split(";")[0]
                            if key in ("SUMMARY", "DTSTART", "DTEND", "LOCATION", "DESCRIPTION", "ORGANIZER", "STATUS"):
                                event[key.lower()] = val

        return {"uid": uid, "events_found": len(events), "events": events}
    finally:
        imap.disconnect()


@mcp.tool(tags={"read"})
async def contact_from_email(
    account_id: Annotated[int, Field(description="Mail account ID")],
    folder: Annotated[str, Field(description="Folder name")],
    uid: Annotated[str, Field(description="Email UID")],
) -> dict:
    """Extract contact information (name, email, organization) from an email's headers and signature."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account

    folder = _normalize_folder(folder)
    user_id = _user_id()
    async with get_db() as db:
        account = await get_account(db, user_id, account_id)

    imap = get_imap(account)
    imap.connect()
    try:
        raw = imap.fetch_raw(uid, folder)
        if not raw:
            raise ToolError(f"Email UID {uid} not found")

        import email as email_mod
        msg = email_mod.message_from_bytes(raw)

        from_name, from_email = email_mod.utils.parseaddr(msg.get("From", ""))
        reply_to_name, reply_to_email = email_mod.utils.parseaddr(msg.get("Reply-To", ""))
        org = msg.get("Organization", "") or msg.get("X-Mailer", "")

        # Extract phone/url from signature (last lines of body)
        import re
        body = ""
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    body = payload.decode("utf-8", errors="replace")
                    break

        # Look in last 15 lines for signature info
        lines = body.strip().split("\n")[-15:]
        sig_text = "\n".join(lines)

        phones = re.findall(r'[\+]?[\d\s\-\.]{8,15}', sig_text)
        urls = re.findall(r'https?://[^\s<>"]+', sig_text)

        contact = {
            "name": from_name or "",
            "email": from_email,
            "reply_to": reply_to_email if reply_to_email != from_email else "",
            "organization": org,
            "phones": [p.strip() for p in phones[:3]],
            "urls": urls[:3],
        }
        return {"uid": uid, "contact": contact}
    finally:
        imap.disconnect()


# ---------------------------------------------------------------------------
# SEND EMAILS (SMTP)
# ---------------------------------------------------------------------------

def _smtp_connect(account):
    """Connect to SMTP server with proper SSL/STARTTLS handling."""
    port = account.smtp_port or 465
    use_implicit_ssl = port == 465 or (account.smtp_ssl and port != 587)
    if use_implicit_ssl:
        return smtplib.SMTP_SSL(account.smtp_host, port, timeout=30)
    else:
        server = smtplib.SMTP(account.smtp_host, port, timeout=30)
        server.starttls(context=ssl_mod.create_default_context())
        return server

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
        server = _smtp_connect(account)
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
    folder: Annotated[str, Field(description="Folder of the original email (use display name with accents)")],
    uid: Annotated[str, Field(description="UID of the email to reply to")],
    body: Annotated[str, Field(description="Reply body text")],
    reply_all: Annotated[bool, Field(description="Reply to all recipients")] = False,
) -> dict:
    """Reply to an existing email. Reads the original, composes a reply, and sends via SMTP."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account
    from src.security import decrypt_value

    folder = _normalize_folder(folder)
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

        server = _smtp_connect(account)
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
    folder: Annotated[str, Field(description="Folder of the email to forward (use display name with accents)")],
    uid: Annotated[str, Field(description="UID of the email to forward")],
    to: Annotated[list[str], Field(description="Recipients to forward to")],
    comment: Annotated[str, Field(description="Optional comment to add above the forwarded email")] = "",
) -> dict:
    """Forward an email to new recipients with an optional comment."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account
    from src.security import decrypt_value

    folder = _normalize_folder(folder)
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

        server = _smtp_connect(account)
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
    folder: Annotated[str, Field(description="Folder name (use display name with accents, e.g. 'Éléments supprimés')")],
    uid: Annotated[str, Field(description="Email UID")],
) -> dict:
    """Summarize an email in a few sentences using AI."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account, get_user
    from src.ai.router import get_llm_for_user

    folder = _normalize_folder(folder)
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
    folder: Annotated[str, Field(description="Folder name (use display name with accents, e.g. 'Éléments supprimés')")],
    uid: Annotated[str, Field(description="Email UID")],
    categories: Annotated[list[str], Field(description="Categories to classify into")],
) -> dict:
    """Classify an email into one of the given categories using AI."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account, get_user
    from src.ai.router import get_llm_for_user

    folder = _normalize_folder(folder)
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
    folder: Annotated[str, Field(description="Folder name (use display name with accents, e.g. 'Éléments supprimés')")],
    uid: Annotated[str, Field(description="Email UID")],
    fields: Annotated[list[str], Field(description="Fields to extract (e.g. date, amount, company, phone)")],
) -> dict:
    """Extract structured information from an email using AI (dates, amounts, names, etc.)."""
    from src.mcp.context import get_db, get_imap
    from src.mcp.helpers import get_account, get_user
    from src.ai.router import get_llm_for_user

    folder = _normalize_folder(folder)
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
    imap_folder: Annotated[str, Field(description="Target folder (use display name with accents)")],
    delete_after: Annotated[bool, Field(description="Delete local copy after successful IMAP upload")] = False,
) -> dict:
    """Copy emails from a local folder to an IMAP folder.
    Skips emails already on IMAP (by Message-ID). Optionally deletes local copies."""
    import imaplib
    import email.utils
    import time
    from sqlalchemy import select, delete as sa_delete
    from src.mcp.context import get_db, get_imap

    imap_folder = _normalize_folder(imap_folder)
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

    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    port = int(os.environ.get("MCP_PORT", "8200"))
    host = os.environ.get("MCP_HOST", "0.0.0.0")

    logger.info(f"MailIA MCP server starting for user_id={USER_ID} transport={transport}")

    if transport == "sse":
        import asyncio
        asyncio.run(mcp.run_async(transport="sse", host=host, port=port))
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

"""
Thunderbird mbox importer — for initial bulk import of historical emails.
Parses .mbox files and indexes them into Elasticsearch.
"""
import mailbox
import email
import email.utils
import logging
import os
from pathlib import Path
from datetime import datetime, timezone

from src.rules.engine import EmailContext
from src.search.indexer import get_es_client, ensure_index, index_email

logger = logging.getLogger(__name__)


async def import_mbox_directory(
    base_path: str,
    user_id: int,
    account_id: int,
    account_name: str = "",
) -> dict:
    """Recursively import all mbox files from a Thunderbird profile directory.

    Returns stats: {total, indexed, errors}
    """
    es = await get_es_client()
    await ensure_index(es, user_id)

    stats = {"total": 0, "indexed": 0, "errors": 0}
    base = Path(base_path)

    # Find all mbox files (files without .msf extension and not .dat)
    for path in sorted(base.rglob("*")):
        if path.is_dir():
            continue
        if path.suffix in (".msf", ".dat", ".html", ".json", ".sqlite"):
            continue
        if path.stat().st_size == 0:
            continue

        # Derive folder name from path
        rel = path.relative_to(base)
        folder_parts = []
        for part in rel.parts:
            clean = part.replace(".sbd", "")
            folder_parts.append(clean)
        folder_name = "/".join(folder_parts)

        logger.info(f"Importing {path} as folder '{folder_name}'")

        try:
            mbox = mailbox.mbox(str(path))
            for i, msg in enumerate(mbox):
                stats["total"] += 1
                try:
                    ctx = _parse_mbox_message(msg, str(i), folder_name)
                    if ctx:
                        await index_email(es, user_id, account_id, ctx)
                        stats["indexed"] += 1
                except Exception as e:
                    stats["errors"] += 1
                    if stats["errors"] <= 10:
                        logger.error(f"Error parsing message {i} in {path}: {e}")

                if stats["total"] % 1000 == 0:
                    logger.info(f"Progress: {stats['total']} processed, {stats['indexed']} indexed")

        except Exception as e:
            logger.error(f"Error opening mbox {path}: {e}")

    await es.close()
    logger.info(f"Import complete: {stats}")
    return stats


def _parse_mbox_message(msg, uid: str, folder: str) -> EmailContext | None:
    """Parse a single mbox message into an EmailContext."""
    body_text = ""
    has_attachments = False
    attachment_names = []

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))

            if "attachment" in disposition:
                has_attachments = True
                filename = part.get_filename() or "unnamed"
                attachment_names.append(filename)
            elif content_type == "text/plain" and not body_text:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    try:
                        body_text = payload.decode(charset, errors="replace")
                    except (UnicodeDecodeError, LookupError):
                        body_text = payload.decode("utf-8", errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            try:
                body_text = payload.decode(charset, errors="replace")
            except (UnicodeDecodeError, LookupError):
                body_text = payload.decode("utf-8", errors="replace")

    date_str = msg.get("Date", "")
    try:
        date_parsed = email.utils.parsedate_to_datetime(date_str)
        if date_parsed.tzinfo is None:
            date_parsed = date_parsed.replace(tzinfo=timezone.utc)
        date_formatted = date_parsed.strftime("%Y-%m-%d %H:%M")
    except Exception:
        date_formatted = ""

    from_addr = email.utils.parseaddr(msg.get("From", ""))[1]
    to_addr = email.utils.parseaddr(msg.get("To", ""))[1]
    subject = msg.get("Subject", "") or ""

    if not from_addr and not subject:
        return None

    return EmailContext(
        uid=uid,
        folder=folder,
        from_addr=from_addr,
        to_addr=to_addr,
        subject=subject,
        body_text=body_text,
        has_attachments=has_attachments,
        attachment_names=attachment_names,
        date=date_formatted,
    )

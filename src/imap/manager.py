"""
IMAP Manager — handles all IMAP operations.
All actions are performed on the real mail server (source of truth).
"""
import imaplib
import email
import email.utils
import logging
from dataclasses import dataclass
from datetime import datetime

from src.rules.engine import EmailContext

logger = logging.getLogger(__name__)

PROCESSED_FLAG = "X-MailIA-Processed"


def _imap_quote(folder: str) -> str:
    """Quote folder name for IMAP commands (RFC 3501).

    Auto-encodes UTF-8 folder names to IMAP modified UTF-7 if they contain
    non-ASCII characters. Already-encoded names (pure ASCII) pass through.
    """
    if folder.startswith('"') and folder.endswith('"'):
        return folder
    # If folder contains non-ASCII chars, it's UTF-8 and needs IMAP UTF-7 encoding
    if any(ord(c) > 127 for c in folder):
        folder = _encode_imap_utf7(folder)
    escaped = folder.replace('\\', '\\\\').replace('"', '\\"')
    return f'"{escaped}"'


@dataclass
class IMAPConfig:
    host: str
    port: int
    ssl: bool
    user: str
    password: str


class IMAPManager:
    """Manages IMAP connection and operations for a single mail account."""

    def __init__(self, config: IMAPConfig):
        self.config = config
        self._conn: imaplib.IMAP4_SSL | imaplib.IMAP4 | None = None

    def connect(self):
        if self.config.ssl:
            self._conn = imaplib.IMAP4_SSL(self.config.host, self.config.port, timeout=30)
        else:
            self._conn = imaplib.IMAP4(self.config.host, self.config.port, timeout=30)
        try:
            self._conn.login(self.config.user, self.config.password)
        except imaplib.IMAP4.error:
            # Fallback to AUTHENTICATE PLAIN for non-ASCII passwords
            import base64
            auth_string = f"\x00{self.config.user}\x00{self.config.password}"
            self._conn.authenticate("PLAIN", lambda _: auth_string.encode("utf-8"))
        logger.info(f"Connected to {self.config.host} as {self.config.user}")

    def disconnect(self):
        if self._conn:
            try:
                self._conn.logout()
            except Exception:
                pass
            self._conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def list_folders(self) -> list[dict]:
        """List all IMAP folders with their separator."""
        import re
        status, data = self._conn.list()
        folders = []
        for item in data:
            if isinstance(item, bytes):
                # IMAP LIST format: (\\flags) "sep" "folder_name"
                match = re.match(rb'\(([^)]*)\)\s+"(.+?)"\s+(.*)', item)
                if match:
                    flags = match.group(1).decode()
                    sep = match.group(2).decode()
                    raw_name = match.group(3).decode().strip('"')
                    folders.append({
                        "name": raw_name,
                        "display_name": _decode_imap_utf7(raw_name),
                        "separator": sep,
                        "flags": flags,
                    })
        return folders

    def get_uids(self, folder: str = "INBOX", since_uid: str | None = None) -> list[str]:
        """Get message UIDs in a folder, optionally since a given UID."""
        self._conn.select(_imap_quote(folder), readonly=True)
        if since_uid:
            criteria = f"UID {int(since_uid) + 1}:*"
            status, data = self._conn.uid("SEARCH", None, criteria)
        else:
            status, data = self._conn.uid("SEARCH", None, "ALL")

        if status != "OK":
            return []
        uids = data[0].decode().split() if data[0] else []
        # Filter out UIDs <= since_uid (IMAP search can return the boundary)
        if since_uid:
            uids = [u for u in uids if int(u) > int(since_uid)]
        return uids

    def fetch_email(self, uid: str, folder: str = "INBOX") -> EmailContext | None:
        """Fetch and parse a single email by UID."""
        self._conn.select(_imap_quote(folder), readonly=True)
        status, data = self._conn.uid("FETCH", uid, "(RFC822)")
        if status != "OK" or not data or data[0] is None:
            return None

        raw = data[0][1]
        msg = email.message_from_bytes(raw)

        body_text = ""
        has_attachments = False
        attachment_names = []

        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))

            if "attachment" in disposition:
                has_attachments = True
                filename = part.get_filename() or "unnamed"
                attachment_names.append(filename)
            elif content_type == "text/plain" and not body_text:
                try:
                    payload = part.get_payload(decode=True)
                except Exception:
                    payload = part.get_payload(decode=False)
                    if isinstance(payload, str):
                        payload = payload.encode("utf-8", errors="replace")
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    try:
                        body_text = payload.decode(charset, errors="replace")
                    except (UnicodeDecodeError, LookupError):
                        body_text = payload.decode("utf-8", errors="replace")

        date_str = msg.get("Date", "")
        try:
            date_tuple = email.utils.parsedate_to_datetime(date_str)
            date_formatted = date_tuple.strftime("%Y-%m-%d %H:%M")
        except Exception:
            date_formatted = date_str

        from_addr = email.utils.parseaddr(msg.get("From", ""))[1]
        to_addr = email.utils.parseaddr(msg.get("To", ""))[1]

        return EmailContext(
            uid=uid,
            folder=folder,
            from_addr=from_addr,
            to_addr=to_addr,
            subject=msg.get("Subject", ""),
            body_text=body_text,
            has_attachments=has_attachments,
            attachment_names=attachment_names,
            date=date_formatted,
        )

    def fetch_raw(self, uid: str, folder: str = "INBOX") -> bytes | None:
        """Fetch raw email bytes (for attachment extraction)."""
        self._conn.select(_imap_quote(folder), readonly=True)
        status, data = self._conn.uid("FETCH", uid, "(RFC822)")
        if status != "OK" or not data or data[0] is None:
            return None
        return data[0][1]

    # --- Write operations (modify mail server state) ---

    def create_folder(self, folder: str) -> bool:
        """Create a new IMAP folder and subscribe to it."""
        status, _ = self._conn.create(_imap_quote(folder))
        if status == "OK":
            self._conn.subscribe(_imap_quote(folder))
            logger.info(f"Created and subscribed folder: {folder}")
            return True
        logger.error(f"Failed to create folder: {folder}")
        return False

    def delete_folder(self, folder: str) -> bool:
        """Delete an IMAP folder (must be empty or server empties it)."""
        # Unsubscribe first
        self._conn.unsubscribe(_imap_quote(folder))
        status, _ = self._conn.delete(_imap_quote(folder))
        if status == "OK":
            logger.info(f"Deleted folder: {folder}")
            return True
        logger.error(f"Failed to delete folder: {folder}")
        return False

    def move_email(self, uid: str, from_folder: str, to_folder: str) -> bool:
        """Move an email to another folder via IMAP. Creates folder if needed."""
        self._conn.select(_imap_quote(from_folder))
        # Ensure target folder exists
        self._conn.create(_imap_quote(to_folder))
        # Copy then delete (MOVE not supported everywhere)
        status, _ = self._conn.uid("COPY", uid, _imap_quote(to_folder))
        if status == "OK":
            self._conn.uid("STORE", uid, "+FLAGS", "\\Deleted")
            self._conn.expunge()
            logger.info(f"Moved UID {uid}: {from_folder} -> {to_folder}")
            return True
        logger.error(f"Failed to move UID {uid} to {to_folder}")
        return False

    def move_emails_bulk(self, uids: list[str], from_folder: str, to_folder: str) -> dict:
        """Move multiple emails in one batch. Batch COPY by UID sets (50 per call), single EXPUNGE."""
        if not uids:
            return {"moved": 0, "failed": 0}
        self._conn.select(_imap_quote(from_folder))
        self._conn.create(_imap_quote(to_folder))
        moved = []
        failed = 0
        # Batch COPY: send comma-separated UID sets (chunks of 50) instead of one-by-one
        chunk_size = 50
        for i in range(0, len(uids), chunk_size):
            chunk = uids[i:i + chunk_size]
            uid_set = ",".join(chunk)
            status, _ = self._conn.uid("COPY", uid_set, _imap_quote(to_folder))
            if status == "OK":
                moved.extend(chunk)
            else:
                # Fallback: try individually for this chunk
                for uid in chunk:
                    status, _ = self._conn.uid("COPY", uid, _imap_quote(to_folder))
                    if status == "OK":
                        moved.append(uid)
                    else:
                        failed += 1
        if moved:
            # Batch STORE+EXPUNGE in one go
            uid_set = ",".join(moved)
            self._conn.uid("STORE", uid_set, "+FLAGS", "\\Deleted")
            self._conn.expunge()
        logger.info(f"Bulk move: {len(moved)} moved to {to_folder}, {failed} failed")
        return {"moved": len(moved), "failed": failed}

    def flag_email(self, uid: str, folder: str, flag: str) -> bool:
        """Add a flag to an email."""
        self._conn.select(_imap_quote(folder))
        imap_flag = _resolve_flag(flag)
        status, _ = self._conn.uid("STORE", uid, "+FLAGS", imap_flag)
        if status == "OK":
            logger.info(f"Flagged UID {uid} with {imap_flag}")
            return True
        return False

    def unflag_email(self, uid: str, folder: str, flag: str) -> bool:
        """Remove a flag from an email."""
        self._conn.select(_imap_quote(folder))
        imap_flag = _resolve_flag(flag)
        status, _ = self._conn.uid("STORE", uid, "-FLAGS", imap_flag)
        if status == "OK":
            logger.info(f"Unflagged UID {uid}: removed {imap_flag}")
            return True
        return False

    def mark_read(self, uid: str, folder: str) -> bool:
        """Mark an email as read."""
        self._conn.select(_imap_quote(folder))
        status, _ = self._conn.uid("STORE", uid, "+FLAGS", "\\Seen")
        return status == "OK"

    def mark_unread(self, uid: str, folder: str) -> bool:
        """Mark an email as unread."""
        self._conn.select(_imap_quote(folder))
        status, _ = self._conn.uid("STORE", uid, "-FLAGS", "\\Seen")
        return status == "OK"

    def mark_processed(self, uid: str, folder: str) -> bool:
        """Add the MailIA processed flag."""
        self._conn.select(_imap_quote(folder))
        status, _ = self._conn.uid("STORE", uid, "+FLAGS", PROCESSED_FLAG)
        return status == "OK"

    def get_unprocessed_uids(self, folder: str = "INBOX") -> list[str]:
        """Get UIDs of emails not yet processed by MailIA."""
        self._conn.select(_imap_quote(folder), readonly=True)
        # Search for emails WITHOUT our custom flag
        status, data = self._conn.uid("SEARCH", None, f"UNKEYWORD {PROCESSED_FLAG}")
        if status != "OK":
            return []
        return data[0].decode().split() if data[0] else []

    def _find_trash_folder(self) -> str | None:
        """Find the Trash folder name, cached per connection."""
        if hasattr(self, '_trash_cache'):
            return self._trash_cache
        trash_names = [
            "Trash", "INBOX.Trash", "Deleted", "INBOX.Deleted",
            "Deleted Items", "INBOX.Deleted Items",
            "Corbeille", "INBOX.Corbeille",
        ]
        try:
            folders = self.list_folders()
            folder_names = [f["name"] for f in folders]
            for t in trash_names:
                if t in folder_names:
                    self._trash_cache = t
                    return t
        except Exception:
            pass
        self._trash_cache = None
        return None

    def delete_email(self, uid: str, folder: str) -> bool:
        """Delete an email (move to Trash, or flag as Deleted)."""
        trash_folder = self._find_trash_folder()
        if trash_folder and folder != trash_folder:
            return self.move_email(uid, folder, trash_folder)
        # Fallback: flag as Deleted
        self._conn.select(_imap_quote(folder))
        status, _ = self._conn.uid("STORE", uid, "+FLAGS", "\\Deleted")
        if status == "OK":
            self._conn.expunge()
            return True
        return False

    def delete_emails_bulk(self, uids: list[str], folder: str) -> dict:
        """Delete multiple emails in one batch. Uses UID sets for efficiency."""
        if not uids:
            return {"deleted": 0, "failed": 0}

        trash_folder = self._find_trash_folder()

        if trash_folder and folder != trash_folder:
            # Batch move to trash: SELECT once, COPY each, flag all, EXPUNGE once
            self._conn.select(_imap_quote(folder))
            self._conn.create(_imap_quote(trash_folder))
            moved = []
            failed = 0
            for uid in uids:
                status, _ = self._conn.uid("COPY", uid, _imap_quote(trash_folder))
                if status == "OK":
                    moved.append(uid)
                else:
                    failed += 1
            if moved:
                uid_set = ",".join(moved)
                self._conn.uid("STORE", uid_set, "+FLAGS", "\\Deleted")
                self._conn.expunge()
            logger.info(f"Bulk delete: {len(moved)} moved to trash, {failed} failed")
            return {"deleted": len(moved), "failed": failed}
        else:
            # Already in trash or no trash: batch flag + single EXPUNGE
            self._conn.select(_imap_quote(folder))
            uid_set = ",".join(uids)
            status, _ = self._conn.uid("STORE", uid_set, "+FLAGS", "\\Deleted")
            if status == "OK":
                self._conn.expunge()
                logger.info(f"Bulk delete: {len(uids)} permanently deleted from {folder}")
                return {"deleted": len(uids), "failed": 0}
            logger.error(f"Bulk delete STORE failed for {folder}")
            return {"deleted": 0, "failed": len(uids)}

    def save_draft(self, raw_message: bytes) -> bool:
        """Save a message to the Drafts folder."""
        draft_names = [
            "Drafts", "INBOX.Drafts", "Draft", "INBOX.Draft",
            "Brouillons", "INBOX.Brouillons",
        ]
        try:
            folders = self.list_folders()
            folder_names = [f["name"] for f in folders]
            draft_folder = None
            for d in draft_names:
                if d in folder_names:
                    draft_folder = d
                    break
            if not draft_folder:
                draft_folder = "Drafts"
                self._conn.create(_imap_quote(draft_folder))
        except Exception:
            draft_folder = "Drafts"

        import time
        status, _ = self._conn.append(
            _imap_quote(draft_folder),
            "\\Draft",
            imaplib.Time2Internaldate(time.time()),
            raw_message,
        )
        return status == "OK"

    def save_to_sent(self, raw_message: bytes) -> bool:
        """Save a sent message to the Sent folder."""
        try:
            folders = self.list_folders()
            # First: find folder with \Sent flag (most reliable)
            sent_folder = None
            for f in folders:
                if "\\Sent" in f.get("flags", ""):
                    sent_folder = f["name"]
                    break
            # Fallback: try common names
            if not sent_folder:
                sent_names = [
                    "Sent", "INBOX.Sent", "Sent Items", "INBOX.Sent Items",
                    "Sent Messages", "INBOX.Sent Messages",
                    "&AMk-l&AOk-ments envoy&AOk-s",
                ]
                folder_names = [f["name"] for f in folders]
                for s in sent_names:
                    if s in folder_names:
                        sent_folder = s
                        break
            if not sent_folder:
                sent_folder = "Sent"
                self._conn.create(_imap_quote(sent_folder))
        except Exception:
            sent_folder = "Sent"

        import time
        status, _ = self._conn.append(
            _imap_quote(sent_folder),
            "\\Seen",
            imaplib.Time2Internaldate(time.time()),
            raw_message,
        )
        return status == "OK"

    def get_attachment_data(self, uid: str, folder: str, attachment_index: int) -> dict | None:
        """Get attachment data by index from an email."""
        raw = self.fetch_raw(uid, folder)
        if not raw:
            return None
        msg = email.message_from_bytes(raw)
        idx = 0
        for part in msg.walk():
            disposition = str(part.get("Content-Disposition", ""))
            if "attachment" in disposition:
                if idx == attachment_index:
                    try:
                        payload = part.get_payload(decode=True)
                    except Exception:
                        payload = part.get_payload(decode=False)
                        if isinstance(payload, str):
                            payload = payload.encode("utf-8", errors="replace")
                    filename = part.get_filename() or "unnamed"
                    content_type = part.get_content_type()
                    return {
                        "filename": filename,
                        "content_type": content_type,
                        "data": payload,
                    }
                idx += 1
        return None


def _resolve_flag(flag: str) -> str:
    """Convert human-readable flag names to IMAP flags."""
    mapping = {
        "important": "\\Flagged",
        "flagged": "\\Flagged",
        "read": "\\Seen",
        "seen": "\\Seen",
        "answered": "\\Answered",
        "draft": "\\Draft",
    }
    return mapping.get(flag.lower(), flag)


def _encode_imap_utf7(s: str) -> str:
    """Encode a Unicode string to IMAP modified UTF-7 (RFC 3501 section 5.1.3).

    ASCII printable chars (0x20-0x7e) pass through, except '&' becomes '&-'.
    Non-ASCII chars are encoded as modified base64 between '&' and '-'.
    """
    import base64
    result = []
    non_ascii = []

    def _flush_non_ascii():
        if non_ascii:
            utf16 = ''.join(non_ascii).encode('utf-16-be')
            b64 = base64.b64encode(utf16).decode('ascii').rstrip('=')
            b64 = b64.replace('/', ',')
            result.append('&' + b64 + '-')
            non_ascii.clear()

    for ch in s:
        if ch == '&':
            _flush_non_ascii()
            result.append('&-')
        elif 0x20 <= ord(ch) <= 0x7e:
            _flush_non_ascii()
            result.append(ch)
        else:
            non_ascii.append(ch)
    _flush_non_ascii()
    return ''.join(result)


def _decode_imap_utf7(s: str) -> str:
    """Decode IMAP modified UTF-7 folder names (RFC 3501 section 5.1.3).

    IMAP uses '&' instead of '+' as shift character, and ',' instead of '/' in base64.
    '&-' encodes a literal '&'.
    """
    result = []
    i = 0
    while i < len(s):
        if s[i] == '&':
            j = s.index('-', i + 1)
            if j == i + 1:
                # &- is a literal &
                result.append('&')
            else:
                # Decode modified base64 section
                import base64
                encoded = s[i + 1:j]
                # Replace , with / for standard base64
                encoded = encoded.replace(',', '/')
                # Pad to multiple of 4
                encoded += '=' * (4 - len(encoded) % 4) if len(encoded) % 4 else ''
                try:
                    decoded_bytes = base64.b64decode(encoded)
                    result.append(decoded_bytes.decode('utf-16-be'))
                except Exception:
                    result.append(s[i:j + 1])
            i = j + 1
        else:
            result.append(s[i])
            i += 1
    return ''.join(result)

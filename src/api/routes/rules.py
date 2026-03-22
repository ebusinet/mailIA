import email
import email.header
import email.utils
import json
import logging
import re

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from starlette.concurrency import iterate_in_threadpool
from starlette.responses import StreamingResponse

from src.db.session import get_db
from src.db.models import User, AIRule, ClassicRule, MailAccount, SpamWhitelist, SpamBlacklist
from src.api.deps import get_current_user
from src.rules.parser import parse_rules_markdown
from src.api.routes.accounts import _spam_analysis

logger = logging.getLogger(__name__)

router = APIRouter()


class RuleCreate(BaseModel):
    name: str
    rules_markdown: str
    priority: int = 100
    ai_provider_id: int | None = None


class RuleUpdate(BaseModel):
    name: str | None = None
    rules_markdown: str | None = None
    priority: int | None = None
    is_active: bool | None = None
    ai_provider_id: int | None = None


class RuleResponse(BaseModel):
    id: int
    name: str
    priority: int
    is_active: bool
    rules_markdown: str
    ai_provider_id: int | None
    parsed_count: int

    model_config = {"from_attributes": True}


@router.get("/", response_model=list[RuleResponse])
async def list_rules(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(AIRule).where(AIRule.user_id == user.id).order_by(AIRule.priority)
    )
    rules = result.scalars().all()
    return [
        RuleResponse(
            id=r.id, name=r.name, priority=r.priority, is_active=r.is_active,
            rules_markdown=r.rules_markdown, ai_provider_id=r.ai_provider_id,
            parsed_count=len(parse_rules_markdown(r.rules_markdown)),
        )
        for r in rules
    ]


@router.post("/", response_model=RuleResponse)
async def create_rule(
    req: RuleCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # Validate markdown parses correctly
    parsed = parse_rules_markdown(req.rules_markdown)

    rule = AIRule(
        user_id=user.id,
        name=req.name,
        rules_markdown=req.rules_markdown,
        priority=req.priority,
        ai_provider_id=req.ai_provider_id,
    )
    db.add(rule)
    await db.commit()
    await db.refresh(rule)

    return RuleResponse(
        id=rule.id, name=rule.name, priority=rule.priority, is_active=rule.is_active,
        rules_markdown=rule.rules_markdown, ai_provider_id=rule.ai_provider_id,
        parsed_count=len(parsed),
    )


@router.put("/{rule_id}", response_model=RuleResponse)
async def update_rule(
    rule_id: int,
    req: RuleUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(AIRule).where(AIRule.id == rule_id, AIRule.user_id == user.id)
    )
    rule = result.scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    if req.name is not None:
        rule.name = req.name
    if req.rules_markdown is not None:
        parse_rules_markdown(req.rules_markdown)  # validate
        rule.rules_markdown = req.rules_markdown
    if req.priority is not None:
        rule.priority = req.priority
    if req.is_active is not None:
        rule.is_active = req.is_active
    if req.ai_provider_id is not None:
        rule.ai_provider_id = req.ai_provider_id

    await db.commit()
    await db.refresh(rule)

    return RuleResponse(
        id=rule.id, name=rule.name, priority=rule.priority, is_active=rule.is_active,
        rules_markdown=rule.rules_markdown, ai_provider_id=rule.ai_provider_id,
        parsed_count=len(parse_rules_markdown(rule.rules_markdown)),
    )


@router.delete("/{rule_id}")
async def delete_rule(
    rule_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(AIRule).where(AIRule.id == rule_id, AIRule.user_id == user.id)
    )
    rule = result.scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    await db.delete(rule)
    await db.commit()
    return {"status": "deleted"}


@router.post("/{rule_id}/preview")
async def preview_rule(
    rule_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Preview parsed rules from markdown — shows what the engine will execute."""
    result = await db.execute(
        select(AIRule).where(AIRule.id == rule_id, AIRule.user_id == user.id)
    )
    rule = result.scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    parsed = parse_rules_markdown(rule.rules_markdown)
    return {
        "rules": [
            {
                "name": r.name,
                "condition": {
                    "raw": r.condition.raw,
                    "keywords": r.condition.keywords,
                    "from_patterns": r.condition.from_patterns,
                    "subject_patterns": r.condition.subject_patterns,
                    "has_attachment": r.condition.has_attachment,
                    "needs_ai": r.condition.needs_ai,
                },
                "actions": [{"type": a.action_type, "target": a.target} for a in r.actions],
                "notify": r.notify,
                "notify_summary": r.notify_summary,
            }
            for r in parsed
        ]
    }


# ---------------------------------------------------------------------------
# Classic (non-AI) rules
# ---------------------------------------------------------------------------

class ClassicRuleCreate(BaseModel):
    name: str
    match_mode: str = "all"  # "all" (AND) or "any" (OR)
    stop_processing: bool = False
    priority: int = 100
    conditions: list[dict]  # [{"field":"from","operator":"contains","value":"..."}]
    actions: list[dict]      # [{"type":"move","target":"Junk"}, {"type":"mark_read"}]


class ClassicRuleUpdate(BaseModel):
    name: str | None = None
    match_mode: str | None = None
    stop_processing: bool | None = None
    priority: int | None = None
    is_active: bool | None = None
    conditions: list[dict] | None = None
    actions: list[dict] | None = None


_VALID_FIELDS = {"from", "to", "cc", "subject", "has_attachments", "is_spam", "is_reply", "size", "age"}
_VALID_OPERATORS = {"contains", "not_contains", "equals", "domain_is", "starts_with", "ends_with", "is_true", "is_false", "greater_than", "less_than", "older_than", "newer_than"}
_VALID_ACTIONS = {"move", "mark_read", "mark_flagged", "mark_spam", "delete", "forward"}


def _validate_classic_rule(conditions: list[dict], actions: list[dict]):
    if not conditions:
        raise HTTPException(status_code=422, detail="At least one condition is required")
    if not actions:
        raise HTTPException(status_code=422, detail="At least one action is required")
    for c in conditions:
        if c.get("field") not in _VALID_FIELDS:
            raise HTTPException(status_code=422, detail=f"Invalid field: {c.get('field')}")
        if c.get("operator") not in _VALID_OPERATORS:
            raise HTTPException(status_code=422, detail=f"Invalid operator: {c.get('operator')}")
    for a in actions:
        if a.get("type") not in _VALID_ACTIONS:
            raise HTTPException(status_code=422, detail=f"Invalid action: {a.get('type')}")


@router.get("/classic/")
async def list_classic_rules(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ClassicRule).where(ClassicRule.user_id == user.id).order_by(ClassicRule.priority)
    )
    rules = result.scalars().all()
    return [
        {
            "id": r.id, "name": r.name, "priority": r.priority,
            "is_active": r.is_active, "match_mode": r.match_mode,
            "stop_processing": r.stop_processing,
            "conditions": r.conditions, "actions": r.actions,
        }
        for r in rules
    ]


@router.post("/classic/")
async def create_classic_rule(
    req: ClassicRuleCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _validate_classic_rule(req.conditions, req.actions)
    rule = ClassicRule(
        user_id=user.id, name=req.name, priority=req.priority,
        match_mode=req.match_mode, stop_processing=req.stop_processing,
        conditions=req.conditions, actions=req.actions,
    )
    db.add(rule)
    await db.commit()
    await db.refresh(rule)
    return {
        "id": rule.id, "name": rule.name, "priority": rule.priority,
        "is_active": rule.is_active, "match_mode": rule.match_mode,
        "stop_processing": rule.stop_processing,
        "conditions": rule.conditions, "actions": rule.actions,
    }


@router.put("/classic/{rule_id}")
async def update_classic_rule(
    rule_id: int,
    req: ClassicRuleUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ClassicRule).where(ClassicRule.id == rule_id, ClassicRule.user_id == user.id)
    )
    rule = result.scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    if req.name is not None:
        rule.name = req.name
    if req.match_mode is not None:
        rule.match_mode = req.match_mode
    if req.stop_processing is not None:
        rule.stop_processing = req.stop_processing
    if req.priority is not None:
        rule.priority = req.priority
    if req.is_active is not None:
        rule.is_active = req.is_active
    if req.conditions is not None:
        _validate_classic_rule(req.conditions, rule.actions)
        rule.conditions = req.conditions
    if req.actions is not None:
        _validate_classic_rule(rule.conditions, req.actions)
        rule.actions = req.actions

    await db.commit()
    await db.refresh(rule)
    return {
        "id": rule.id, "name": rule.name, "priority": rule.priority,
        "is_active": rule.is_active, "match_mode": rule.match_mode,
        "stop_processing": rule.stop_processing,
        "conditions": rule.conditions, "actions": rule.actions,
    }


@router.delete("/classic/{rule_id}")
async def delete_classic_rule(
    rule_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ClassicRule).where(ClassicRule.id == rule_id, ClassicRule.user_id == user.id)
    )
    rule = result.scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    await db.delete(rule)
    await db.commit()
    return {"status": "deleted"}


class ApplyRuleRequest(BaseModel):
    account_id: int
    folder: str


def _parse_age_value(val: str):
    """Parse age value like '5d', '2w', '1m' into a timedelta. Returns None on error."""
    from datetime import timedelta
    val = val.strip().lower()
    if not val:
        return None
    unit = val[-1]
    try:
        num = int(val[:-1])
    except (ValueError, IndexError):
        return None
    if unit == "d":
        return timedelta(days=num)
    elif unit == "w":
        return timedelta(weeks=num)
    elif unit == "m":
        return timedelta(days=num * 30)
    return None


def _match_condition(cond: dict, from_addr: str, to_addr: str, cc_addr: str, subject: str,
                     has_attach: bool, spam_score: float, is_reply: bool, size: int,
                     email_date: "datetime | None" = None) -> bool:
    """Test a single condition against email headers."""
    field = cond.get("field", "")
    op = cond.get("operator", "")
    val = (cond.get("value") or "").lower()

    if field == "from":
        target = from_addr.lower()
    elif field == "to":
        target = to_addr.lower()
    elif field == "cc":
        target = cc_addr.lower()
    elif field == "subject":
        target = subject.lower()
    elif field == "has_attachments":
        return (op == "is_true" and has_attach) or (op == "is_false" and not has_attach)
    elif field == "is_spam":
        try:
            threshold = float(val) if val else _SPAM_SCORE_DEFAULT
        except ValueError:
            threshold = _SPAM_SCORE_DEFAULT
        if op in ("greater_than", "is_true"):
            return spam_score >= threshold
        elif op in ("less_than", "is_false"):
            return spam_score < threshold
        return False
    elif field == "is_reply":
        return (op == "is_true" and is_reply) or (op == "is_false" and not is_reply)
    elif field == "size":
        try:
            threshold = int(val)
        except ValueError:
            return False
        return (op == "greater_than" and size > threshold) or (op == "less_than" and size < threshold)
    elif field == "age":
        from datetime import datetime, timezone
        if not email_date:
            return False
        delta = _parse_age_value(val)
        if not delta:
            return False
        now = datetime.now(timezone.utc)
        if email_date.tzinfo is None:
            email_date = email_date.replace(tzinfo=timezone.utc)
        age = now - email_date
        return (op == "older_than" and age > delta) or (op == "newer_than" and age < delta)
    else:
        return False

    if op == "contains":
        return val in target
    elif op == "not_contains":
        return val not in target
    elif op == "equals":
        return target == val
    elif op == "domain_is":
        # Match exact domain or subdomains (e.g. "linkedin.com" matches "@em.linkedin.com")
        at_pos = target.rfind("@")
        if at_pos < 0:
            return False
        domain = target[at_pos + 1:]
        return domain == val or domain.endswith("." + val)
    elif op == "starts_with":
        return target.startswith(val)
    elif op == "ends_with":
        return target.endswith(val)
    return False


def _match_rule(rule_data: dict, from_addr: str, to_addr: str, cc_addr: str, subject: str,
                has_attach: bool, spam_score: int, is_reply: bool, size: int,
                email_date: "datetime | None" = None) -> bool:
    """Test all conditions against email headers."""
    conditions = rule_data.get("conditions", [])
    if not conditions:
        return False
    results = [_match_condition(c, from_addr, to_addr, cc_addr, subject, has_attach, spam_score, is_reply, size, email_date)
               for c in conditions]
    if rule_data.get("match_mode") == "any":
        return any(results)
    return all(results)


_SPAM_SCORE_DEFAULT = 5  # Default _spam_analysis score threshold (matches accounts.py is_spam >= 5.0)

_FETCH_HEADERS = (
    "(UID FLAGS BODYSTRUCTURE "
    "BODY.PEEK[HEADER.FIELDS (FROM TO CC SUBJECT DATE IN-REPLY-TO REFERENCES "
    "X-SPAM-STATUS X-SPAM-FLAG X-SPAM-SCORE X-VR-SPAMSCORE "
    "AUTHENTICATION-RESULTS X-MAILER USER-AGENT PRECEDENCE MESSAGE-ID)] RFC822.SIZE)"
)


def _parse_email_fields(item, whitelist=None, blacklist=None):
    """Parse a single IMAP FETCH item into email fields dict. Returns None if invalid."""
    if not isinstance(item, tuple) or len(item) < 2:
        return None
    meta_line = item[0].decode("utf-8", errors="replace")
    header_bytes = item[1]
    uid_match = re.search(r"UID (\d+)", meta_line)
    if not uid_match:
        return None
    uid = uid_match.group(1)
    size_match = re.search(r"RFC822\.SIZE (\d+)", meta_line)
    size = int(size_match.group(1)) if size_match else 0
    has_attach = "attachment" in meta_line.lower()

    msg = email.message_from_bytes(header_bytes)
    from_addr = str(email.utils.parseaddr(str(msg.get("From", "") or ""))[1])
    to_addr = str(email.utils.parseaddr(str(msg.get("To", "") or ""))[1])
    cc_addr = str(msg.get("Cc", "") or "")
    raw_subject = str(msg.get("Subject", "") or "")
    try:
        decoded_parts = email.header.decode_header(raw_subject)
        subject = "".join(
            part.decode(enc or "utf-8", errors="replace") if isinstance(part, bytes) else str(part)
            for part, enc in decoded_parts
        )
    except Exception:
        subject = str(raw_subject)
    is_reply = bool(msg.get("In-Reply-To") or msg.get("References"))
    # Parse email date
    date_str = str(msg.get("Date", "") or "").strip()
    email_date = None
    if date_str:
        try:
            email_date = email.utils.parsedate_to_datetime(date_str)
        except Exception:
            email_date = None
    # Use the same _spam_analysis as the email list display (accounts.py)
    # Wrap msg to ensure all header values are plain strings (avoid Header objects)
    class _StrMsg:
        def __init__(self, m):
            self._m = m
        def get(self, key, default=""):
            val = self._m.get(key, default)
            return str(val) if val is not None else default
    flags_str = ""
    fm = re.search(r"FLAGS \(([^)]*)\)", meta_line)
    if fm:
        flags_str = fm.group(1)
    analysis = _spam_analysis(_StrMsg(msg), flags_str=flags_str, subject=subject, from_addr=from_addr, whitelist=whitelist, blacklist=blacklist)
    spam_score = analysis["score"]
    return {
        "uid": uid, "from": from_addr, "to": to_addr, "cc": cc_addr,
        "subject": subject, "has_attach": has_attach, "spam_score": spam_score,
        "is_reply": is_reply, "size": size, "email_date": email_date,
    }


def _batched_store(conn, uids, flags, folder_quoted):
    """STORE flags in batches to avoid IMAP command-length limits."""
    batch = 50
    for i in range(0, len(uids), batch):
        chunk = uids[i:i + batch]
        conn.uid("STORE", ",".join(chunk), "+FLAGS", flags)


def _execute_actions(imap, conn, matched_uids, actions, folder):
    """Execute rule actions on matched UIDs. Returns list of action results."""
    from src.imap.manager import _imap_quote
    actions_done = []
    folder_q = _imap_quote(folder)
    for action in actions:
        atype = action.get("type")
        target = action.get("target", "")
        if atype == "move" and target and matched_uids:
            res = imap.move_emails_bulk(matched_uids, folder, target)
            actions_done.append({"type": "move", "target": target, **res})
        elif atype == "mark_read" and matched_uids:
            conn.select(folder_q)
            _batched_store(conn, matched_uids, "\\Seen", folder_q)
            actions_done.append({"type": "mark_read", "count": len(matched_uids)})
        elif atype == "mark_flagged" and matched_uids:
            conn.select(folder_q)
            _batched_store(conn, matched_uids, "\\Flagged", folder_q)
            actions_done.append({"type": "mark_flagged", "count": len(matched_uids)})
        elif atype == "mark_spam" and matched_uids:
            res = imap.move_emails_bulk(matched_uids, folder, "Junk")
            actions_done.append({"type": "mark_spam", **res})
        elif atype == "delete" and matched_uids:
            conn.select(folder_q)
            _batched_store(conn, matched_uids, "\\Deleted", folder_q)
            conn.expunge()
            actions_done.append({"type": "delete", "count": len(matched_uids)})
    return actions_done


def _apply_rule_generator(rule_data, rule_actions, config, folder, whitelist=None, blacklist=None):
    """Sync generator that scans folder and yields NDJSON progress events."""
    from src.imap.manager import IMAPManager, _imap_quote

    try:
        with IMAPManager(config) as imap:
            conn = imap._conn
            conn.select(_imap_quote(folder))
            status, data = conn.uid("SEARCH", None, "ALL")
            if status != "OK" or not data or not data[0]:
                yield json.dumps({"type": "result", "matched": 0, "actions": []}) + "\n"
                return
            all_uids = data[0].split()
            total = len(all_uids)
            yield json.dumps({"type": "start", "total": total}) + "\n"

            matched_uids = []
            batch_size = 100
            for i in range(0, total, batch_size):
                chunk = all_uids[i:i + batch_size]
                uid_set = b",".join(chunk)
                status, fetch_data = conn.uid("FETCH", uid_set, _FETCH_HEADERS)
                if status != "OK":
                    continue
                for item in fetch_data:
                    fields = _parse_email_fields(item, whitelist=whitelist, blacklist=blacklist)
                    if fields and _match_rule(
                        rule_data, fields["from"], fields["to"], fields["cc"],
                        fields["subject"], fields["has_attach"], fields["spam_score"],
                        fields["is_reply"], fields["size"], fields.get("email_date"),
                    ):
                        matched_uids.append(fields["uid"])

                scanned = min(i + batch_size, total)
                yield json.dumps({"type": "progress", "scanned": scanned, "total": total, "matched": len(matched_uids)}) + "\n"

            actions_done = _execute_actions(imap, conn, matched_uids, rule_actions, folder)
            yield json.dumps({"type": "result", "matched": len(matched_uids), "actions": actions_done}) + "\n"

    except Exception as e:
        logger.error(f"Apply rule stream error: {e}", exc_info=True)
        yield json.dumps({"type": "error", "detail": str(e)}) + "\n"


@router.post("/classic/{rule_id}/apply")
async def apply_classic_rule(
    rule_id: int,
    req: ApplyRuleRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    stream: bool = Query(False),
):
    """Apply a classic rule to all emails in a folder. Returns matched/acted count."""
    result = await db.execute(
        select(ClassicRule).where(ClassicRule.id == rule_id, ClassicRule.user_id == user.id)
    )
    rule = result.scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    acct_result = await db.execute(
        select(MailAccount).where(MailAccount.id == req.account_id, MailAccount.user_id == user.id)
    )
    account = acct_result.scalar_one_or_none()
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    from src.imap.manager import IMAPManager, IMAPConfig, _imap_quote
    from src.security import decrypt_value as _dec

    config = IMAPConfig(
        host=account.imap_host, port=account.imap_port, ssl=account.imap_ssl,
        user=account.imap_user, password=_dec(account.imap_password_encrypted),
    )

    rule_data = {
        "match_mode": rule.match_mode,
        "conditions": rule.conditions,
        "actions": rule.actions,
    }

    wl_result = await db.execute(
        select(SpamWhitelist).where(SpamWhitelist.account_id == req.account_id)
    )
    wl_set = {e.value for e in wl_result.scalars().all()}
    bl_result = await db.execute(
        select(SpamBlacklist).where(SpamBlacklist.account_id == req.account_id)
    )
    bl_set = {e.value for e in bl_result.scalars().all()}

    if stream:
        return StreamingResponse(
            iterate_in_threadpool(_apply_rule_generator(rule_data, rule.actions, config, req.folder, whitelist=wl_set, blacklist=bl_set)),
            media_type="application/x-ndjson",
        )

    # Non-streaming mode (used by toolbar panel per-folder)
    matched_uids = []
    try:
        with IMAPManager(config) as imap:
            conn = imap._conn
            conn.select(_imap_quote(req.folder))
            status, data = conn.uid("SEARCH", None, "ALL")
            if status != "OK" or not data or not data[0]:
                return {"matched": 0, "actions": []}
            all_uids = data[0].split()

            batch_size = 100
            for i in range(0, len(all_uids), batch_size):
                chunk = all_uids[i:i + batch_size]
                uid_set = b",".join(chunk)
                status, fetch_data = conn.uid("FETCH", uid_set, _FETCH_HEADERS)
                if status != "OK":
                    continue
                for item in fetch_data:
                    fields = _parse_email_fields(item, whitelist=wl_set, blacklist=bl_set)
                    if fields and _match_rule(
                        rule_data, fields["from"], fields["to"], fields["cc"],
                        fields["subject"], fields["has_attach"], fields["spam_score"],
                        fields["is_reply"], fields["size"], fields.get("email_date"),
                    ):
                        matched_uids.append(fields["uid"])

            actions_done = _execute_actions(imap, conn, matched_uids, rule.actions, req.folder)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Apply rule {rule_id} error: {e}")
        raise HTTPException(status_code=502, detail=f"IMAP error: {e}")

    return {"matched": len(matched_uids), "actions": actions_done}


def apply_classic_rules_on_sync(imap, folder: str, uids: list[str],
                                 classic_rules: list, whitelist=None, blacklist=None) -> dict:
    """Apply all active classic rules to a batch of UIDs during sync.

    Args:
        imap: IMAPManager instance (connected)
        folder: IMAP folder name
        uids: list of UID strings to evaluate
        classic_rules: list of ClassicRule objects sorted by priority
        whitelist: set of whitelisted emails/domains
        blacklist: set of blacklisted emails/domains

    Returns:
        dict with counts: {"matched": int, "actions_taken": int}
    """
    from src.imap.manager import _imap_quote

    if not classic_rules or not uids:
        return {"matched": 0, "actions_taken": 0}

    conn = imap._conn
    conn.select(_imap_quote(folder))

    # Bulk fetch headers
    uid_set = ",".join(uids)
    status, fetch_data = conn.uid("FETCH", uid_set, _FETCH_HEADERS)
    if status != "OK":
        return {"matched": 0, "actions_taken": 0}

    # Parse all email headers
    email_fields_map = {}
    for item in fetch_data:
        fields = _parse_email_fields(item, whitelist=whitelist, blacklist=blacklist)
        if fields:
            email_fields_map[fields["uid"]] = fields

    total_matched = 0
    total_actions = 0
    moved_or_deleted = set()

    for uid, fields in email_fields_map.items():
        for rule in classic_rules:
            rule_data = {
                "match_mode": rule.match_mode,
                "conditions": rule.conditions,
            }
            if _match_rule(
                rule_data, fields["from"], fields["to"], fields["cc"],
                fields["subject"], fields["has_attach"], fields["spam_score"],
                fields["is_reply"], fields["size"], fields.get("email_date"),
            ):
                total_matched += 1
                actions_done = _execute_actions(imap, conn, [uid], rule.actions, folder)
                total_actions += len(actions_done)

                for action in rule.actions:
                    if action.get("type") in ("move", "delete", "mark_spam"):
                        moved_or_deleted.add(uid)

                if rule.stop_processing or uid in moved_or_deleted:
                    break

    return {"matched": total_matched, "actions_taken": total_actions}

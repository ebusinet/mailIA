import re
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, union_all, literal, func, or_
from sqlalchemy.orm import selectinload

from src.db.session import get_db
from src.db.models import User, Contact, ContactGroup, contact_group_members, MailAccount, LocalFolder, LocalEmail
from src.api.deps import get_current_user

router = APIRouter()


# --- Schemas ---

class ContactCreate(BaseModel):
    name: str
    first_name: str | None = None
    last_name: str | None = None
    emails: list[str]
    ai_directives: str | None = None
    notes: str | None = None
    group_ids: list[int] = []
    signature_id: int | None = None


class ContactUpdate(BaseModel):
    name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    emails: list[str] | None = None
    ai_directives: str | None = None
    notes: str | None = None
    group_ids: list[int] | None = None
    signature_id: int | None = None


class GroupCreate(BaseModel):
    name: str
    ai_directives: str | None = None
    signature_id: int | None = None


class GroupUpdate(BaseModel):
    name: str | None = None
    ai_directives: str | None = None
    signature_id: int | None = None


class GroupMembersAdd(BaseModel):
    contact_ids: list[int]


# --- Contacts CRUD ---

@router.get("/")
async def list_contacts(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Contact)
        .where(Contact.user_id == user.id)
        .options(selectinload(Contact.groups))
        .order_by(Contact.name)
    )
    contacts = result.scalars().all()
    return [
        {
            "id": c.id,
            "name": c.name,
            "first_name": c.first_name,
            "last_name": c.last_name,
            "emails": c.emails or [],
            "ai_directives": c.ai_directives,
            "notes": c.notes,
            "signature_id": c.signature_id,
            "groups": [{"id": g.id, "name": g.name} for g in c.groups],
        }
        for c in contacts
    ]


@router.post("/")
async def create_contact(
    req: ContactCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    contact = Contact(
        user_id=user.id,
        name=req.name,
        first_name=req.first_name,
        last_name=req.last_name,
        emails=req.emails,
        ai_directives=req.ai_directives,
        notes=req.notes,
        signature_id=req.signature_id,
    )
    if req.group_ids:
        groups = (await db.execute(
            select(ContactGroup).where(
                ContactGroup.id.in_(req.group_ids),
                ContactGroup.user_id == user.id,
            )
        )).scalars().all()
        contact.groups = list(groups)
    db.add(contact)
    await db.commit()
    await db.refresh(contact, attribute_names=["groups"])
    return {
        "id": contact.id,
        "name": contact.name,
        "first_name": contact.first_name,
        "last_name": contact.last_name,
        "emails": contact.emails or [],
        "ai_directives": contact.ai_directives,
        "notes": contact.notes,
        "signature_id": contact.signature_id,
        "groups": [{"id": g.id, "name": g.name} for g in contact.groups],
    }


@router.put("/{contact_id}")
async def update_contact(
    contact_id: int,
    req: ContactUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Contact)
        .where(Contact.id == contact_id, Contact.user_id == user.id)
        .options(selectinload(Contact.groups))
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    if req.name is not None:
        contact.name = req.name
    if req.first_name is not None:
        contact.first_name = req.first_name if req.first_name else None
    if req.last_name is not None:
        contact.last_name = req.last_name if req.last_name else None
    if req.emails is not None:
        contact.emails = req.emails
    if req.ai_directives is not None:
        contact.ai_directives = req.ai_directives if req.ai_directives else None
    if req.notes is not None:
        contact.notes = req.notes if req.notes else None
    if req.signature_id is not None:
        contact.signature_id = req.signature_id if req.signature_id != 0 else None
    if req.group_ids is not None:
        groups = (await db.execute(
            select(ContactGroup).where(
                ContactGroup.id.in_(req.group_ids),
                ContactGroup.user_id == user.id,
            )
        )).scalars().all()
        contact.groups = list(groups)

    await db.commit()
    await db.refresh(contact, attribute_names=["groups"])
    return {
        "id": contact.id,
        "name": contact.name,
        "first_name": contact.first_name,
        "last_name": contact.last_name,
        "emails": contact.emails or [],
        "ai_directives": contact.ai_directives,
        "notes": contact.notes,
        "signature_id": contact.signature_id,
        "groups": [{"id": g.id, "name": g.name} for g in contact.groups],
    }


@router.delete("/{contact_id}")
async def delete_contact(
    contact_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Contact).where(Contact.id == contact_id, Contact.user_id == user.id)
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    await db.delete(contact)
    await db.commit()
    return {"status": "deleted"}


# --- Groups CRUD ---

@router.get("/groups")
async def list_groups(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # Auto-create "Tous" group if it doesn't exist
    tous_check = await db.execute(
        select(ContactGroup).where(
            ContactGroup.user_id == user.id,
            ContactGroup.name == "Tous",
        )
    )
    if not tous_check.scalar_one_or_none():
        db.add(ContactGroup(user_id=user.id, name="Tous"))
        await db.commit()

    result = await db.execute(
        select(ContactGroup)
        .where(ContactGroup.user_id == user.id)
        .options(selectinload(ContactGroup.contacts))
        .order_by(ContactGroup.name)
    )
    groups_list = result.scalars().all()
    # Sort "Tous" first, then alphabetical
    groups = sorted(groups_list, key=lambda g: (0 if g.name == "Tous" else 1, g.name))
    return [
        {
            "id": g.id,
            "name": g.name,
            "ai_directives": g.ai_directives,
            "signature_id": g.signature_id,
            "member_count": len(g.contacts),
            "members": [{"id": c.id, "name": c.name} for c in g.contacts],
        }
        for g in groups
    ]


@router.post("/groups")
async def create_group(
    req: GroupCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    group = ContactGroup(
        user_id=user.id,
        name=req.name,
        ai_directives=req.ai_directives,
        signature_id=req.signature_id,
    )
    db.add(group)
    await db.commit()
    await db.refresh(group)
    return {
        "id": group.id,
        "name": group.name,
        "ai_directives": group.ai_directives,
        "signature_id": group.signature_id,
        "member_count": 0,
        "members": [],
    }


@router.put("/groups/{group_id}")
async def update_group(
    group_id: int,
    req: GroupUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ContactGroup).where(ContactGroup.id == group_id, ContactGroup.user_id == user.id)
    )
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    if req.name is not None:
        group.name = req.name
    if req.ai_directives is not None:
        group.ai_directives = req.ai_directives if req.ai_directives else None
    if req.signature_id is not None:
        group.signature_id = req.signature_id if req.signature_id != 0 else None

    await db.commit()
    await db.refresh(group)
    return {
        "id": group.id,
        "name": group.name,
        "ai_directives": group.ai_directives,
        "signature_id": group.signature_id,
    }


@router.delete("/groups/{group_id}")
async def delete_group(
    group_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ContactGroup).where(ContactGroup.id == group_id, ContactGroup.user_id == user.id)
    )
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    if group.name == "Tous":
        raise HTTPException(status_code=400, detail="Cannot delete the 'Tous' group")
    await db.delete(group)
    await db.commit()
    return {"status": "deleted"}


@router.post("/groups/{group_id}/members")
async def add_group_members(
    group_id: int,
    req: GroupMembersAdd,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ContactGroup)
        .where(ContactGroup.id == group_id, ContactGroup.user_id == user.id)
        .options(selectinload(ContactGroup.contacts))
    )
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    contacts = (await db.execute(
        select(Contact).where(
            Contact.id.in_(req.contact_ids),
            Contact.user_id == user.id,
        )
    )).scalars().all()

    existing_ids = {c.id for c in group.contacts}
    for c in contacts:
        if c.id not in existing_ids:
            group.contacts.append(c)

    await db.commit()
    return {"status": "ok", "member_count": len(group.contacts)}


@router.delete("/groups/{group_id}/members/{contact_id}")
async def remove_group_member(
    group_id: int,
    contact_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ContactGroup)
        .where(ContactGroup.id == group_id, ContactGroup.user_id == user.id)
        .options(selectinload(ContactGroup.contacts))
    )
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    group.contacts = [c for c in group.contacts if c.id != contact_id]
    await db.commit()
    return {"status": "ok"}


# --- Autocomplete ---

_EMAIL_RE = re.compile(r'[\w.+-]+@[\w.-]+\.\w+')
_DISPLAY_RE = re.compile(r'(?:"?([^"<]+?)"?\s+)?<?([^<>\s,]+@[^<>\s,]+)>?')


def _parse_addrs(raw: str | None) -> list[tuple[str, str]]:
    """Extract (display_name, email) pairs from a raw header string."""
    if not raw:
        return []
    results = []
    for match in _DISPLAY_RE.finditer(raw):
        name = (match.group(1) or "").strip()
        email = match.group(2).strip()
        if "@" in email:
            results.append((name, email.lower()))
    return results


@router.get("/autocomplete")
async def autocomplete_contacts(
    q: str = "",
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if len(q) < 1:
        return []

    q_lower = q.lower()
    matches = []
    seen_emails = set()

    # 1. Contacts (priority — shown first)
    result = await db.execute(
        select(Contact)
        .where(Contact.user_id == user.id)
        .options(selectinload(Contact.groups))
        .order_by(Contact.name)
    )
    contacts = result.scalars().all()

    for c in contacts:
        name_match = q_lower in c.name.lower()
        for em in (c.emails or []):
            if name_match or q_lower in em.lower():
                key = em.lower()
                if key not in seen_emails:
                    seen_emails.add(key)
                    matches.append({
                        "name": c.name,
                        "email": em,
                        "source": "contact",
                    })
        if len(matches) >= 30:
            break

    # 2. Elasticsearch (IMAP indexed emails — from_addr and to_addr)
    if len(matches) < 30:
        try:
            from src.search.indexer import get_es_client, _index_name
            es = await get_es_client()
            index = _index_name(user.id)
            es_body = {
                "size": 0,
                "query": {
                    "bool": {
                        "should": [
                            {"wildcard": {"from_addr": f"*{q_lower}*"}},
                            {"wildcard": {"to_addr": f"*{q_lower}*"}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "aggs": {
                    "from_addrs": {
                        "terms": {"field": "from_addr", "size": 30,
                                  "include": f".*{re.escape(q_lower)}.*"}
                    },
                    "to_addrs": {
                        "terms": {"field": "to_addr", "size": 30,
                                  "include": f".*{re.escape(q_lower)}.*"}
                    },
                },
            }
            es_result = await es.search(index=index, body=es_body)
            await es.close()

            for bucket in es_result.get("aggregations", {}).get("from_addrs", {}).get("buckets", []):
                addr = bucket["key"]
                if addr not in seen_emails:
                    seen_emails.add(addr)
                    matches.append({"name": addr.split("@")[0], "email": addr, "source": "history"})
                if len(matches) >= 30:
                    break
            for bucket in es_result.get("aggregations", {}).get("to_addrs", {}).get("buckets", []):
                addr = bucket["key"]
                if addr not in seen_emails:
                    seen_emails.add(addr)
                    matches.append({"name": addr.split("@")[0], "email": addr, "source": "history"})
                if len(matches) >= 30:
                    break
        except Exception:
            pass

    # 3. Local email history (fallback for non-indexed emails)
    if len(matches) < 30:
        account_ids_q = await db.execute(
            select(MailAccount.id).where(MailAccount.user_id == user.id)
        )
        account_ids = [r[0] for r in account_ids_q.fetchall()]

        if account_ids:
            folder_ids_q = await db.execute(
                select(LocalFolder.id).where(LocalFolder.account_id.in_(account_ids))
            )
            folder_ids = [r[0] for r in folder_ids_q.fetchall()]

            if folder_ids:
                emails_q = await db.execute(
                    select(LocalEmail.from_addr, LocalEmail.to_addr, LocalEmail.cc_addr)
                    .where(
                        LocalEmail.folder_id.in_(folder_ids),
                        or_(
                            func.lower(LocalEmail.from_addr).contains(q_lower),
                            func.lower(LocalEmail.to_addr).contains(q_lower),
                            func.lower(LocalEmail.cc_addr).contains(q_lower),
                        )
                    )
                    .order_by(LocalEmail.date.desc())
                    .limit(200)
                )

                for row in emails_q.fetchall():
                    for field in row:
                        for name, email in _parse_addrs(field):
                            if q_lower in name.lower() or q_lower in email:
                                if email not in seen_emails:
                                    seen_emails.add(email)
                                    matches.append({
                                        "name": name or email.split("@")[0],
                                        "email": email,
                                        "source": "history",
                                    })
                                    if len(matches) >= 30:
                                        break
                        if len(matches) >= 30:
                            break
                    if len(matches) >= 30:
                        break

    return matches

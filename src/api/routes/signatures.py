from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from src.db.session import get_db
from src.db.models import User, EmailSignature, Contact, ContactGroup, contact_group_members
from src.api.deps import get_current_user

router = APIRouter()


class SignatureCreate(BaseModel):
    name: str
    body_html: str = ""
    is_default: bool = False


class SignatureUpdate(BaseModel):
    name: str | None = None
    body_html: str | None = None
    is_default: bool | None = None


def _sig_dict(s: EmailSignature) -> dict:
    return {
        "id": s.id,
        "name": s.name,
        "body_html": s.body_html,
        "is_default": s.is_default,
    }


@router.get("/")
async def list_signatures(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(EmailSignature)
        .where(EmailSignature.user_id == user.id)
        .order_by(EmailSignature.name)
    )
    return [_sig_dict(s) for s in result.scalars().all()]


@router.post("/")
async def create_signature(
    req: SignatureCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if req.is_default:
        await db.execute(
            select(EmailSignature)
            .where(EmailSignature.user_id == user.id, EmailSignature.is_default == True)
        )
        # Clear existing defaults
        existing = (await db.execute(
            select(EmailSignature).where(
                EmailSignature.user_id == user.id, EmailSignature.is_default == True
            )
        )).scalars().all()
        for s in existing:
            s.is_default = False

    sig = EmailSignature(
        user_id=user.id,
        name=req.name,
        body_html=req.body_html,
        is_default=req.is_default,
    )
    db.add(sig)
    await db.commit()
    await db.refresh(sig)
    return _sig_dict(sig)


@router.put("/{sig_id}")
async def update_signature(
    sig_id: int,
    req: SignatureUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(EmailSignature).where(
            EmailSignature.id == sig_id, EmailSignature.user_id == user.id
        )
    )
    sig = result.scalar_one_or_none()
    if not sig:
        raise HTTPException(status_code=404, detail="Signature not found")

    if req.name is not None:
        sig.name = req.name
    if req.body_html is not None:
        sig.body_html = req.body_html
    if req.is_default is not None:
        if req.is_default:
            existing = (await db.execute(
                select(EmailSignature).where(
                    EmailSignature.user_id == user.id,
                    EmailSignature.is_default == True,
                    EmailSignature.id != sig_id,
                )
            )).scalars().all()
            for s in existing:
                s.is_default = False
        sig.is_default = req.is_default

    await db.commit()
    await db.refresh(sig)
    return _sig_dict(sig)


@router.delete("/{sig_id}")
async def delete_signature(
    sig_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(EmailSignature).where(
            EmailSignature.id == sig_id, EmailSignature.user_id == user.id
        )
    )
    sig = result.scalar_one_or_none()
    if not sig:
        raise HTTPException(status_code=404, detail="Signature not found")
    await db.delete(sig)
    await db.commit()
    return {"status": "deleted"}


@router.get("/resolve")
async def resolve_signature(
    emails: str = Query(..., description="Comma-separated recipient emails"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Resolve which signature to use for given recipients.
    Priority: contact > group > default."""
    email_list = [e.strip().lower() for e in emails.split(",") if e.strip()]
    if not email_list:
        return {"signature": None}

    # 1. Check contacts (highest priority)
    contacts_result = await db.execute(
        select(Contact)
        .where(Contact.user_id == user.id, Contact.signature_id.is_not(None))
        .options(selectinload(Contact.signature))
    )
    for contact in contacts_result.scalars().all():
        contact_emails = [e.lower() for e in (contact.emails or [])]
        for recipient in email_list:
            if recipient in contact_emails:
                return {"signature": _sig_dict(contact.signature)}

    # 2. Check groups (medium priority)
    contacts_with_groups = await db.execute(
        select(Contact)
        .where(Contact.user_id == user.id)
        .options(selectinload(Contact.groups).selectinload(ContactGroup.signature))
    )
    for contact in contacts_with_groups.scalars().all():
        contact_emails = [e.lower() for e in (contact.emails or [])]
        for recipient in email_list:
            if recipient in contact_emails:
                for group in contact.groups:
                    if group.signature_id:
                        return {"signature": _sig_dict(group.signature)}

    # 3. Default signature (lowest priority)
    default_result = await db.execute(
        select(EmailSignature).where(
            EmailSignature.user_id == user.id, EmailSignature.is_default == True
        )
    )
    default_sig = default_result.scalar_one_or_none()
    if default_sig:
        return {"signature": _sig_dict(default_sig)}

    return {"signature": None}

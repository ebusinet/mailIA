import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.db.session import get_db
from src.db.models import User, MailAccount
from src.security import (
    hash_password, verify_password, create_access_token,
    create_reset_token, decode_reset_token, decrypt_value,
)
from src.api.deps import get_current_user
from src.config import get_settings

logger = logging.getLogger(__name__)
router = APIRouter()


class RegisterRequest(BaseModel):
    email: EmailStr
    username: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class ForgotPasswordRequest(BaseModel):
    email: str


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    id: int
    email: str
    username: str
    is_admin: bool = False
    telegram_chat_id: str | None

    model_config = {"from_attributes": True}


@router.post("/register", response_model=TokenResponse)
async def register(req: RegisterRequest, db: AsyncSession = Depends(get_db)):
    existing = await db.execute(select(User).where(User.email == req.email))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Email already registered")

    user = User(
        email=req.email,
        username=req.username,
        password_hash=hash_password(req.password),
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    token = create_access_token({"sub": str(user.id)})
    return TokenResponse(access_token=token)


@router.post("/login", response_model=TokenResponse)
async def login(req: LoginRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.email == req.email))
    user = result.scalar_one_or_none()

    if not user or not verify_password(req.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    token = create_access_token({"sub": str(user.id)})
    return TokenResponse(access_token=token)


@router.post("/forgot-password")
async def forgot_password(req: ForgotPasswordRequest, db: AsyncSession = Depends(get_db)):
    """Send a password reset link via email. Always returns success to prevent email enumeration."""
    settings = get_settings()

    result = await db.execute(select(User).where(User.email == req.email))
    user = result.scalar_one_or_none()
    if not user:
        return {"status": "ok"}

    reset_token = create_reset_token(user.id)
    reset_url = f"{settings.app_url}/static/index.html?reset_token={reset_token}"

    # Find an SMTP-capable account to send the email (admin first, then any)
    smtp_account = None
    result = await db.execute(
        select(MailAccount)
        .join(User, MailAccount.user_id == User.id)
        .where(User.is_admin.is_(True), MailAccount.smtp_host.isnot(None))
        .limit(1)
    )
    smtp_account = result.scalar_one_or_none()
    if not smtp_account:
        result = await db.execute(
            select(MailAccount).where(MailAccount.smtp_host.isnot(None)).limit(1)
        )
        smtp_account = result.scalar_one_or_none()

    if not smtp_account:
        logger.error("No SMTP account available to send password reset email")
        return {"status": "ok"}

    sender = smtp_account.smtp_user or smtp_account.imap_user
    msg = MIMEMultipart("alternative")
    msg["From"] = f"MailIA <{sender}>"
    msg["To"] = user.email
    msg["Subject"] = "MailIA - Reinitialisation de votre mot de passe"

    text_body = (
        f"Bonjour {user.username},\n\n"
        f"Vous avez demande la reinitialisation de votre mot de passe MailIA.\n\n"
        f"Cliquez sur ce lien pour definir un nouveau mot de passe :\n"
        f"{reset_url}\n\n"
        f"Ce lien est valable 30 minutes.\n\n"
        f"Si vous n'avez pas fait cette demande, ignorez cet email.\n\n"
        f"-- MailIA"
    )
    html_body = (
        f"<div style='font-family:sans-serif;max-width:500px;margin:0 auto;padding:2rem'>"
        f"<h2 style='color:#7c3aed'>MailIA</h2>"
        f"<p>Bonjour <strong>{user.username}</strong>,</p>"
        f"<p>Vous avez demande la reinitialisation de votre mot de passe.</p>"
        f"<p style='margin:1.5rem 0'><a href='{reset_url}' "
        f"style='background:#7c3aed;color:white;padding:0.75rem 1.5rem;border-radius:0.5rem;"
        f"text-decoration:none;font-weight:bold'>Reinitialiser mon mot de passe</a></p>"
        f"<p style='color:#888;font-size:0.85rem'>Ce lien est valable 30 minutes.<br>"
        f"Si vous n'avez pas fait cette demande, ignorez cet email.</p>"
        f"</div>"
    )
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        smtp_password = decrypt_value(smtp_account.smtp_password_encrypted) if smtp_account.smtp_password_encrypted else decrypt_value(smtp_account.imap_password_encrypted)
        smtp_port = smtp_account.smtp_port or 587
        if smtp_port == 465:
            server = smtplib.SMTP_SSL(smtp_account.smtp_host, smtp_port)
        else:
            server = smtplib.SMTP(smtp_account.smtp_host, smtp_port)
            server.starttls()
        server.login(smtp_account.smtp_user or smtp_account.imap_user, smtp_password)
        server.sendmail(sender, [user.email], msg.as_string())
        server.quit()
        logger.info(f"Password reset email sent to {user.email}")
    except Exception as e:
        logger.error(f"Failed to send password reset email: {e}")

    return {"status": "ok"}


@router.post("/reset-password")
async def reset_password(req: ResetPasswordRequest, db: AsyncSession = Depends(get_db)):
    """Reset the password using a valid reset token."""
    user_id = decode_reset_token(req.token)
    if user_id is None:
        raise HTTPException(status_code=400, detail="Lien invalide ou expire")

    if len(req.new_password) < 6:
        raise HTTPException(status_code=400, detail="Le mot de passe doit contenir au moins 6 caracteres")

    result = await db.execute(select(User).where(User.id == user_id, User.is_active.is_(True)))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=400, detail="Utilisateur introuvable")

    user.password_hash = hash_password(req.new_password)
    await db.commit()

    token = create_access_token({"sub": str(user.id)})
    return {"status": "ok", "access_token": token, "token_type": "bearer"}


@router.get("/me", response_model=UserResponse)
async def me(user: User = Depends(get_current_user)):
    return user

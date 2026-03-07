from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, ForeignKey, JSON, SmallInteger
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    username = Column(String(100), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    is_admin = Column(Boolean, default=False)
    telegram_chat_id = Column(String(50), nullable=True)
    settings = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)

    mail_accounts = relationship("MailAccount", back_populates="user", cascade="all, delete-orphan")
    ai_providers = relationship("AIProvider", back_populates="user", cascade="all, delete-orphan")
    ai_rules = relationship("AIRule", back_populates="user", cascade="all, delete-orphan")


class MailAccount(Base):
    __tablename__ = "mail_accounts"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(100), nullable=False)

    imap_host = Column(String(255), nullable=False)
    imap_port = Column(SmallInteger, default=993)
    imap_ssl = Column(Boolean, default=True)
    imap_user = Column(String(255), nullable=False)
    imap_password_encrypted = Column(Text, nullable=False)

    smtp_host = Column(String(255), nullable=True)
    smtp_port = Column(SmallInteger, default=465)
    smtp_ssl = Column(Boolean, default=True)
    smtp_user = Column(String(255), nullable=True)
    smtp_password_encrypted = Column(Text, nullable=True)

    sync_enabled = Column(Boolean, default=True)
    last_sync_uid = Column(String(50), nullable=True)
    last_sync_at = Column(DateTime, nullable=True)
    sync_state = Column(JSON, default=dict)  # {folder: last_uid} per-folder tracking
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="mail_accounts")


class AIProvider(Base):
    __tablename__ = "ai_providers"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(100), nullable=False)
    provider_type = Column(String(50), nullable=False)  # claude, ollama, openai
    endpoint = Column(String(500), nullable=True)
    api_key_encrypted = Column(Text, nullable=True)
    model = Column(String(100), nullable=False)
    is_default = Column(Boolean, default=False)
    is_local = Column(Boolean, default=False)  # requires WebSocket bridge
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="ai_providers")


class AIRule(Base):
    __tablename__ = "ai_rules"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    priority = Column(Integer, default=100)
    is_active = Column(Boolean, default=True)
    rules_markdown = Column(Text, nullable=False)
    ai_provider_id = Column(Integer, ForeignKey("ai_providers.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="ai_rules")
    ai_provider = relationship("AIProvider")


class SystemSetting(Base):
    __tablename__ = "system_settings"

    id = Column(Integer, primary_key=True)
    key = Column(String(100), unique=True, nullable=False, index=True)
    value = Column(Text, nullable=True)
    is_encrypted = Column(Boolean, default=False)
    description = Column(String(500), nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ProcessingLog(Base):
    __tablename__ = "processing_log"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    mail_account_id = Column(Integer, ForeignKey("mail_accounts.id", ondelete="CASCADE"), nullable=False)
    mail_uid = Column(String(100), nullable=False)
    folder = Column(String(255), nullable=True)
    rule_id = Column(Integer, ForeignKey("ai_rules.id", ondelete="SET NULL"), nullable=True)
    action_taken = Column(String(50), nullable=False)  # moved, flagged, labeled, notified
    action_detail = Column(JSON, nullable=True)
    ai_response = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

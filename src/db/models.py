# PERFORMANCE NOTE: Run on DB: CREATE INDEX IF NOT EXISTS idx_local_emails_folder_date ON local_emails(folder_id, date DESC);
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, LargeBinary,
    ForeignKey, JSON, SmallInteger, UniqueConstraint, Table, func
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
    classic_rules = relationship("ClassicRule", back_populates="user", cascade="all, delete-orphan")
    contacts = relationship("Contact", back_populates="user", cascade="all, delete-orphan")
    contact_groups = relationship("ContactGroup", back_populates="user", cascade="all, delete-orphan")
    signatures = relationship("EmailSignature", back_populates="user", cascade="all, delete-orphan")


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


class ClassicRule(Base):
    __tablename__ = "classic_rules"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    priority = Column(Integer, default=100)
    is_active = Column(Boolean, default=True)
    match_mode = Column(String(10), default="all")  # "all" (AND) or "any" (OR)
    stop_processing = Column(Boolean, default=False)
    conditions = Column(JSON, nullable=False, default=list)
    actions = Column(JSON, nullable=False, default=list)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="classic_rules")


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


class LocalFolder(Base):
    __tablename__ = "local_folders"

    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(Integer, ForeignKey("mail_accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(500), nullable=False)
    path = Column(String(500), nullable=False)
    parent_path = Column(String(500), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (UniqueConstraint("account_id", "path"),)


class EmailSignature(Base):
    __tablename__ = "email_signatures"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    body_html = Column(Text, nullable=False, default="")
    is_default = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="signatures")


contact_group_members = Table(
    "contact_group_members",
    Base.metadata,
    Column("contact_id", Integer, ForeignKey("contacts.id", ondelete="CASCADE"), primary_key=True),
    Column("group_id", Integer, ForeignKey("contact_groups.id", ondelete="CASCADE"), primary_key=True),
)


class Contact(Base):
    __tablename__ = "contacts"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    first_name = Column(String(255), nullable=True)
    last_name = Column(String(255), nullable=True)
    emails = Column(JSON, nullable=False, default=list)
    ai_directives = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)
    signature_id = Column(Integer, ForeignKey("email_signatures.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="contacts")
    groups = relationship("ContactGroup", secondary=contact_group_members, back_populates="contacts")
    signature = relationship("EmailSignature", foreign_keys=[signature_id])


class ContactGroup(Base):
    __tablename__ = "contact_groups"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    ai_directives = Column(Text, nullable=True)
    signature_id = Column(Integer, ForeignKey("email_signatures.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="contact_groups")
    contacts = relationship("Contact", secondary=contact_group_members, back_populates="groups")
    signature = relationship("EmailSignature", foreign_keys=[signature_id])


class SpamWhitelist(Base):
    __tablename__ = "spam_whitelist"
    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(Integer, ForeignKey("mail_accounts.id", ondelete="CASCADE"), nullable=False)
    entry_type = Column(String, nullable=False)  # "email" or "domain"
    value = Column(String, nullable=False)  # email address or domain
    created_at = Column(DateTime, server_default=func.now())


class SpamBlacklist(Base):
    __tablename__ = "spam_blacklist"
    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(Integer, ForeignKey("mail_accounts.id", ondelete="CASCADE"), nullable=False)
    entry_type = Column(String, nullable=False)  # "email" or "domain"
    value = Column(String, nullable=False)  # email address or domain
    created_at = Column(DateTime, server_default=func.now())


class LocalEmail(Base):
    __tablename__ = "local_emails"

    id = Column(Integer, primary_key=True, index=True)
    folder_id = Column(Integer, ForeignKey("local_folders.id", ondelete="CASCADE"), nullable=False, index=True)
    message_id_header = Column(String(500), index=True)
    from_addr = Column(String(500))
    to_addr = Column(String(500))
    cc_addr = Column(String(500))
    subject = Column(String(1000))
    date = Column(DateTime, index=True)
    seen = Column(Boolean, default=True)
    flagged = Column(Boolean, default=False)
    answered = Column(Boolean, default=False)
    has_attachments = Column(Boolean, default=False)
    body_text = Column(Text)
    body_html = Column(Text)
    raw_message = Column(LargeBinary)
    created_at = Column(DateTime, server_default=func.now())

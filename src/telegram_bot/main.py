"""
MailIA Telegram Bot — search, ask AI, get notifications.
"""
import logging
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters, ContextTypes
)
from sqlalchemy import select

from src.config import get_settings
from src.db.session import async_session
from src.db.models import User
from src.search.indexer import get_es_client, search_emails
from src.ai.router import get_llm_for_user
from src.ai.base import AIMessage

logger = logging.getLogger(__name__)
settings = get_settings()

_bot_app: Application | None = None


async def send_notification(chat_id: str, message: str):
    """Send a notification message to a user."""
    global _bot_app
    if _bot_app and _bot_app.bot:
        await _bot_app.bot.send_message(chat_id=chat_id, text=message)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "MailIA Bot\n\n"
        "Commands:\n"
        "/link <email> - Link your MailIA account\n"
        "/search <query> - Search your emails\n"
        "/ask <question> - Ask AI about your emails\n"
        "/status - Account status\n\n"
        "Or just type a message to search/ask."
    )


async def link_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Link Telegram to a MailIA account."""
    if not context.args:
        await update.message.reply_text("Usage: /link your@email.com")
        return

    email = context.args[0]
    chat_id = str(update.effective_chat.id)

    async with async_session() as db:
        result = await db.execute(select(User).where(User.email == email))
        user = result.scalar_one_or_none()
        if not user:
            await update.message.reply_text("Account not found. Register at the web app first.")
            return

        user.telegram_chat_id = chat_id
        await db.commit()
        await update.message.reply_text(f"Linked to {email}. You'll receive notifications here.")


async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search emails."""
    query = " ".join(context.args) if context.args else ""
    if not query:
        await update.message.reply_text("Usage: /search <query>")
        return

    user = await _get_user_from_chat(str(update.effective_chat.id))
    if not user:
        await update.message.reply_text("Link your account first: /link your@email.com")
        return

    es = await get_es_client()
    try:
        raw = await search_emails(es, user.id, query=query, size=5)
        hits = raw["hits"]["hits"]
        if not hits:
            await update.message.reply_text("No results found.")
            return

        lines = []
        for hit in hits:
            src = hit["_source"]
            lines.append(f"From: {src.get('from_addr', '?')}\n"
                        f"Subject: {src.get('subject', '?')}\n"
                        f"Date: {src.get('date', '?')}\n---")

        await update.message.reply_text("\n".join(lines))
    finally:
        await es.close()


async def ask_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ask AI a question about emails."""
    question = " ".join(context.args) if context.args else ""
    if not question:
        await update.message.reply_text("Usage: /ask <question>")
        return

    user = await _get_user_from_chat(str(update.effective_chat.id))
    if not user:
        await update.message.reply_text("Link your account first: /link your@email.com")
        return

    # Search for relevant emails first
    es = await get_es_client()
    try:
        raw = await search_emails(es, user.id, query=question, size=5)
        hits = raw["hits"]["hits"]

        context_text = ""
        if hits:
            email_summaries = []
            for hit in hits:
                src = hit["_source"]
                email_summaries.append(
                    f"From: {src.get('from_addr')}, Subject: {src.get('subject')}, "
                    f"Date: {src.get('date')}\n{src.get('body', '')[:500]}"
                )
            context_text = "\n---\n".join(email_summaries)
    finally:
        await es.close()

    async with async_session() as db:
        llm = await get_llm_for_user(db, user)
        messages = [
            AIMessage("system",
                      "You are MailIA, an AI email assistant. Answer based on the email context provided. "
                      "Reply in the same language as the user. Be concise."),
            AIMessage("user", f"Context (relevant emails):\n{context_text}\n\nQuestion: {question}"),
        ]
        response = await llm.chat(messages)
        await update.message.reply_text(response.content)


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle free-form messages — treat as search or AI question."""
    text = update.message.text
    # If it looks like a question, use AI; otherwise search
    if text.endswith("?") or any(text.lower().startswith(w) for w in
                                  ["qui", "quoi", "comment", "pourquoi", "quand", "combien",
                                   "resume", "résume", "liste", "trouve"]):
        context.args = text.split()
        await ask_command(update, context)
    else:
        context.args = text.split()
        await search_command(update, context)


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = await _get_user_from_chat(str(update.effective_chat.id))
    if not user:
        await update.message.reply_text("Not linked. Use /link your@email.com")
        return
    await update.message.reply_text(f"Linked as: {user.email}\nUsername: {user.username}")


async def _get_user_from_chat(chat_id: str) -> User | None:
    async with async_session() as db:
        result = await db.execute(
            select(User).where(User.telegram_chat_id == chat_id, User.is_active.is_(True))
        )
        return result.scalar_one_or_none()


def main():
    """Start the Telegram bot."""
    if not settings.telegram_bot_token:
        logger.error("TELEGRAM_BOT_TOKEN not set")
        return

    global _bot_app
    _bot_app = Application.builder().token(settings.telegram_bot_token).build()

    _bot_app.add_handler(CommandHandler("start", start_command))
    _bot_app.add_handler(CommandHandler("link", link_command))
    _bot_app.add_handler(CommandHandler("search", search_command))
    _bot_app.add_handler(CommandHandler("ask", ask_command))
    _bot_app.add_handler(CommandHandler("status", status_command))
    _bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    logger.info("MailIA Telegram bot starting...")
    _bot_app.run_polling()


if __name__ == "__main__":
    main()

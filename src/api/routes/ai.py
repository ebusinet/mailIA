import json
import logging
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.db.session import get_db
from src.db.models import User, AIProvider
from src.api.deps import get_current_user
from src.security import encrypt_value
from src.ai.router import get_llm_for_user
from src.ai.base import AIMessage

router = APIRouter()

CHAT_SYSTEM_PROMPT = (
    "You are MailIA, an AI email assistant. Help the user with their email-related queries. "
    "Reply in the same language as the user. Use Markdown formatting for readability "
    "(headers, bold, tables, lists). "
    "IMPORTANT: Keep the conversation context — the user may ask follow-up questions "
    "referring to previous messages. Stay on topic.\n"
    "When you reference specific emails from tool results, append a clickable marker after "
    "each email subject using this exact format: [[email:ACCOUNT_ID:FOLDER:UID]] where "
    "ACCOUNT_ID, FOLDER and UID come from the tool parameters/results. "
    "Example: 'Votre dossier auto [[email:1:INBOX:4523]]'.\n"
    "When you mention email attachments, include a download marker using this format: "
    "[[attachment:ACCOUNT_ID:FOLDER:UID:INDEX:FILENAME]] where INDEX is the 0-based "
    "attachment index. Example: 'Certificat.pdf [[attachment:1:INBOX:4523:0:Certificat.pdf]]'.\n"
    "These markers render as clickable links in the user interface.\n"
    "IMPORTANT: Do NOT narrate your search process step by step. Do NOT write things like "
    "'Laissez-moi chercher...', 'Les résultats ne sont pas pertinents...', 'Je vais faire "
    "une autre recherche...'. Instead, search silently and only present the final, "
    "well-structured answer to the user."
)


class AIProviderCreate(BaseModel):
    name: str
    provider_type: str  # claude, ollama, openai, local
    endpoint: str | None = None
    api_key: str | None = None
    model: str
    is_default: bool = False
    is_local: bool = False


class AIProviderResponse(BaseModel):
    id: int
    name: str
    provider_type: str
    model: str
    is_default: bool
    is_local: bool

    model_config = {"from_attributes": True}


class ChatMessage(BaseModel):
    role: str  # user, assistant
    content: str


class AIChatRequest(BaseModel):
    message: str
    provider_id: int | None = None
    history: list[ChatMessage] = []


class AIChatResponse(BaseModel):
    response: str
    model: str
    provider: str


@router.get("/providers", response_model=list[AIProviderResponse])
async def list_providers(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(AIProvider).where(AIProvider.user_id == user.id)
    )
    return result.scalars().all()


@router.post("/providers", response_model=AIProviderResponse)
async def create_provider(
    req: AIProviderCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # If setting as default, unset others
    if req.is_default:
        result = await db.execute(
            select(AIProvider).where(AIProvider.user_id == user.id, AIProvider.is_default.is_(True))
        )
        for existing in result.scalars():
            existing.is_default = False

    provider = AIProvider(
        user_id=user.id,
        name=req.name,
        provider_type=req.provider_type,
        endpoint=req.endpoint,
        api_key_encrypted=encrypt_value(req.api_key) if req.api_key else None,
        model=req.model,
        is_default=req.is_default,
        is_local=req.is_local,
    )
    db.add(provider)
    await db.commit()
    await db.refresh(provider)
    return provider


@router.delete("/providers/{provider_id}")
async def delete_provider(
    provider_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(AIProvider).where(AIProvider.id == provider_id, AIProvider.user_id == user.id)
    )
    provider = result.scalar_one_or_none()
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")

    await db.delete(provider)
    await db.commit()
    return {"status": "deleted"}


def _build_messages(req: AIChatRequest) -> list[AIMessage]:
    messages = [AIMessage("system", CHAT_SYSTEM_PROMPT)]
    for h in req.history[-20:]:  # Keep last 20 messages for context
        messages.append(AIMessage(h.role, h.content))
    messages.append(AIMessage("user", req.message))
    return messages


@router.post("/chat", response_model=AIChatResponse)
async def chat(
    req: AIChatRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Chat with the AI — for Q&A about emails, summarization requests, etc."""
    llm = await get_llm_for_user(db, user, req.provider_id)
    messages = _build_messages(req)
    response = await llm.chat(messages)
    return AIChatResponse(response=response.content, model=response.model, provider=response.provider)


@router.post("/chat/stream")
async def chat_stream(
    req: AIChatRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Streaming chat with the AI — returns SSE events."""
    llm = await get_llm_for_user(db, user, req.provider_id)
    logger.info(f"Stream chat: provider={llm.provider_name}, model={getattr(llm, 'default_model', '?')}")
    messages = _build_messages(req)

    async def generate():
        import asyncio
        chunk_count = 0
        try:
            chunk_queue = asyncio.Queue()
            stream_done = asyncio.Event()

            async def _consume_stream():
                nonlocal chunk_count
                try:
                    async for chunk in llm.stream_chat(messages):
                        chunk_count += 1
                        await chunk_queue.put(('text', chunk))
                    logger.info(f"Stream finished: {chunk_count} chunks yielded")
                except Exception as e:
                    logger.warning(f"Stream consume error: {e}")
                    await chunk_queue.put(('error', str(e)))
                finally:
                    stream_done.set()

            task = asyncio.create_task(_consume_stream())
            heartbeat_interval = 10  # seconds

            while not stream_done.is_set() or not chunk_queue.empty():
                try:
                    kind, data = await asyncio.wait_for(chunk_queue.get(), timeout=heartbeat_interval)
                    if kind == 'error':
                        yield f"data: {json.dumps({'error': data})}\n\n"
                    else:
                        yield f"data: {json.dumps({'text': data})}\n\n"
                except asyncio.TimeoutError:
                    # Send heartbeat to keep connection alive
                    logger.debug(f"Stream heartbeat (chunks: {chunk_count})")
                    yield f"data: {json.dumps({'heartbeat': True})}\n\n"

            await task
        except Exception as e:
            logger.error(f"Stream error: {e}", exc_info=True)
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

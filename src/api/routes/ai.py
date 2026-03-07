from fastapi import APIRouter, Depends, HTTPException
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


class AIChatRequest(BaseModel):
    message: str
    provider_id: int | None = None


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


@router.post("/chat", response_model=AIChatResponse)
async def chat(
    req: AIChatRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Chat with the AI — for Q&A about emails, summarization requests, etc."""
    llm = await get_llm_for_user(db, user, req.provider_id)
    messages = [
        AIMessage("system", "You are MailIA, an AI email assistant. Help the user with their email-related queries. "
                  "Reply in the same language as the user."),
        AIMessage("user", req.message),
    ]
    response = await llm.chat(messages)
    return AIChatResponse(response=response.content, model=response.model, provider=response.provider)

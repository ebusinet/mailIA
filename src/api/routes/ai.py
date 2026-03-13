import json
import logging
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_tool_redis: aioredis.Redis | None = None

async def _get_tool_redis() -> aioredis.Redis:
    global _tool_redis
    if _tool_redis is None:
        from src.config import get_settings
        _tool_redis = aioredis.from_url(get_settings().redis_url, decode_responses=True)
    return _tool_redis

async def _get_tool_activities(user_id: int) -> list[dict]:
    """Read and flush tool activity events from Redis (async, non-blocking)."""
    try:
        r = await _get_tool_redis()
        key = f"mcp:tool_activity:{user_id}"
        items = await r.lrange(key, 0, -1)
        if items:
            await r.delete(key)
        return [json.loads(item) for item in items]
    except Exception:
        return []
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
    "CRITICAL — CONVERSATION CONTEXT: You MUST read and use your own previous messages. "
    "When the user confirms an action ('go', 'ok', 'lance', etc.), you MUST follow the plan "
    "you already proposed in the conversation — do NOT re-analyze from scratch. "
    "Use the exact senders, folders, and data from YOUR previous response. "
    "The user may ask follow-up questions referring to previous messages. Stay on topic.\n"
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
    "well-structured answer to the user.\n"
    "TASK PLANS — CRITICAL: When a user confirms execution of a multi-step task (says 'go', "
    "'ok', 'procède', 'lance', 'commence', etc.), you MUST:\n"
    "1. IMMEDIATELY output the task plan FIRST, before doing ANY work. Use this exact format:\n"
    "[[task-plan]]\n"
    "1. [ ] First step\n"
    "2. [ ] Second step\n"
    "[[/task-plan]]\n"
    "2. Then execute ONLY ONE step at a time. After completing a step, output the updated "
    "plan with [x] for completed steps. Do NOT try to do everything in one response.\n"
    "3. At the end of each response, tell the user what was done and ask to continue.\n"
    "This applies to: organizing emails, batch operations, creating folders, deleting emails, "
    "moving emails, complex analysis with multiple phases.\n"
    "IMPORTANT: Work in batches of up to 500 emails per action.\n"
    "TOOL EFFICIENCY — CRITICAL RULES:\n"
    "- To DELETE emails by sender/subject/pattern: use search_and_delete_emails with IMAP criteria. "
    "This is the FASTEST tool — one call can delete hundreds of emails. "
    "Example: search_and_delete_emails(account_id=1, folder='INBOX', imap_criteria='FROM \"newsletter@darty.com\"', max_delete=500)\n"
    "- To DELETE emails when you already have UIDs: use delete_emails_bulk (pass ALL UIDs at once, not one by one).\n"
    "- To MOVE multiple emails: use move_emails_bulk (pass ALL moves at once).\n"
    "- NEVER call delete_email or move_email in a loop — ALWAYS use the bulk/batch variants.\n"
    "- You can chain multiple search_and_delete_emails calls for different senders in one step.\n"
    "If a user says 'Reprendre', 'Continue', or asks to resume, read the plan to identify "
    "completed steps ([x]) and continue from the first incomplete step ([ ]). "
    "Do NOT redo completed steps."
)


class AIProviderCreate(BaseModel):
    name: str
    provider_type: str  # claude, ollama, openai, local, claude-native
    endpoint: str | None = None
    api_key: str | None = None
    model: str
    is_default: bool = False
    is_local: bool = False


class AIProviderUpdate(BaseModel):
    name: str | None = None
    provider_type: str | None = None
    endpoint: str | None = None
    api_key: str | None = None
    model: str | None = None
    is_default: bool | None = None
    is_local: bool | None = None


class AIProviderResponse(BaseModel):
    id: int
    name: str
    provider_type: str
    endpoint: str | None = None
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
    context: str | None = None  # e.g. "account_id=1, folder=INBOX"


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


@router.put("/providers/{provider_id}", response_model=AIProviderResponse)
async def update_provider(
    provider_id: int,
    req: AIProviderUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(AIProvider).where(AIProvider.id == provider_id, AIProvider.user_id == user.id)
    )
    provider = result.scalar_one_or_none()
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")

    if req.is_default:
        others = await db.execute(
            select(AIProvider).where(AIProvider.user_id == user.id, AIProvider.is_default.is_(True))
        )
        for existing in others.scalars():
            if existing.id != provider_id:
                existing.is_default = False

    if req.name is not None:
        provider.name = req.name
    if req.provider_type is not None:
        provider.provider_type = req.provider_type
    if req.endpoint is not None:
        provider.endpoint = req.endpoint
    if req.api_key is not None:
        provider.api_key_encrypted = encrypt_value(req.api_key) if req.api_key else None
    if req.model is not None:
        provider.model = req.model
    if req.is_default is not None:
        provider.is_default = req.is_default
    if req.is_local is not None:
        provider.is_local = req.is_local

    await db.commit()
    await db.refresh(provider)
    # Clear provider cache
    from src.ai.router import _provider_cache
    _provider_cache.pop((user.id, provider_id), None)
    _provider_cache.pop((user.id, None), None)
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


class AITestRequest(BaseModel):
    provider_type: str
    endpoint: str | None = None
    api_key: str | None = None
    model: str


@router.post("/providers/{provider_id}/test")
async def test_provider(
    provider_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Test an existing AI provider by sending a simple prompt."""
    import time
    llm = await get_llm_for_user(db, user, provider_id)
    return await _run_ai_test(llm)


@router.post("/providers/test-connection")
async def test_provider_connection(
    req: AITestRequest,
    user: User = Depends(get_current_user),
):
    """Test AI provider credentials without saving."""
    from src.ai.router import _build_provider_from_params
    llm = _build_provider_from_params(req.provider_type, req.endpoint, req.api_key, req.model)
    return await _run_ai_test(llm)


async def _run_ai_test(llm) -> dict:
    import time
    test_messages = [
        AIMessage("system", "You are a test assistant. Reply with exactly: OK"),
        AIMessage("user", "ping"),
    ]
    start = time.time()
    try:
        resp = await llm.chat(test_messages)
        elapsed = round(time.time() - start, 2)
        return {
            "status": "ok",
            "message": f"Connexion reussie ({elapsed}s)",
            "details": {
                "provider": resp.provider,
                "model": resp.model,
                "response": resp.content[:200],
                "tokens": resp.tokens_used,
                "latency_s": elapsed,
            },
        }
    except Exception as e:
        elapsed = round(time.time() - start, 2)
        error_msg = str(e)
        error_type = type(e).__name__
        return {
            "status": "error",
            "message": f"Echec: {error_msg[:200]}",
            "details": {
                "error_type": error_type,
                "error_detail": error_msg[:500],
                "latency_s": elapsed,
            },
        }


@router.post("/providers/{provider_id}/diagnose")
async def diagnose_provider(
    provider_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Run full diagnostics on an AI provider: connectivity, streaming, MCP."""
    import time
    import httpx

    result = await db.execute(
        select(AIProvider).where(AIProvider.id == provider_id, AIProvider.user_id == user.id)
    )
    provider_config = result.scalar_one_or_none()
    if not provider_config:
        raise HTTPException(status_code=404, detail="Provider not found")

    checks = []

    # 1. Endpoint reachability
    endpoint = provider_config.endpoint
    if endpoint:
        start = time.time()
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
                r = await client.get(endpoint.rstrip("/"))
                elapsed = round(time.time() - start, 2)
                checks.append({
                    "name": "Endpoint accessible",
                    "status": "ok" if r.status_code < 500 else "warn",
                    "detail": f"HTTP {r.status_code} ({elapsed}s)",
                })
        except Exception as e:
            elapsed = round(time.time() - start, 2)
            checks.append({
                "name": "Endpoint accessible",
                "status": "error",
                "detail": f"{type(e).__name__}: {str(e)[:150]} ({elapsed}s)",
            })
    else:
        checks.append({"name": "Endpoint accessible", "status": "skip", "detail": "Pas d'endpoint configure"})

    # 2. Non-streaming chat test
    llm = await get_llm_for_user(db, user, provider_id)
    test_msgs = [
        AIMessage("system", "Reply with exactly: DIAG_OK"),
        AIMessage("user", "diagnostic ping"),
    ]
    start = time.time()
    try:
        resp = await llm.chat(test_msgs)
        elapsed = round(time.time() - start, 2)
        checks.append({
            "name": "Chat (non-streaming)",
            "status": "ok",
            "detail": f"Reponse: {resp.content[:100]} ({elapsed}s, {resp.tokens_used} tokens)",
        })
    except Exception as e:
        elapsed = round(time.time() - start, 2)
        checks.append({
            "name": "Chat (non-streaming)",
            "status": "error",
            "detail": f"{type(e).__name__}: {str(e)[:200]} ({elapsed}s)",
        })

    # 3. Streaming test
    start = time.time()
    try:
        chunks = []
        async for chunk in llm.stream_chat(test_msgs):
            chunks.append(chunk)
            if len(chunks) > 50:
                break
        elapsed = round(time.time() - start, 2)
        full_text = "".join(chunks)[:100]
        checks.append({
            "name": "Chat (streaming)",
            "status": "ok",
            "detail": f"{len(chunks)} chunks, texte: {full_text} ({elapsed}s)",
        })
    except Exception as e:
        elapsed = round(time.time() - start, 2)
        checks.append({
            "name": "Chat (streaming)",
            "status": "error",
            "detail": f"{type(e).__name__}: {str(e)[:200]} ({elapsed}s)",
        })

    # 4. MCP connectivity check
    from src.config import get_settings
    settings = get_settings()
    mcp_url = settings.mcp_sse_url
    if mcp_url:
        start = time.time()
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
                r = await client.get(mcp_url.rstrip("/").replace("/sse", "/health") if "/sse" in mcp_url else mcp_url)
                elapsed = round(time.time() - start, 2)
                checks.append({
                    "name": "MCP Server",
                    "status": "ok" if r.status_code < 500 else "warn",
                    "detail": f"HTTP {r.status_code} a {mcp_url} ({elapsed}s)",
                })
        except Exception as e:
            elapsed = round(time.time() - start, 2)
            checks.append({
                "name": "MCP Server",
                "status": "error",
                "detail": f"{type(e).__name__}: {str(e)[:150]} ({elapsed}s)",
            })
    else:
        checks.append({"name": "MCP Server", "status": "skip", "detail": "MCP_SSE_URL non configure"})

    # 5. Provider config summary
    checks.append({
        "name": "Configuration",
        "status": "ok",
        "detail": f"Type: {provider_config.provider_type}, Modele: {provider_config.model}, Endpoint: {provider_config.endpoint or 'defaut'}",
    })

    all_ok = all(c["status"] in ("ok", "skip") for c in checks)
    return {
        "status": "ok" if all_ok else "error",
        "provider_name": provider_config.name,
        "checks": checks,
    }


def _build_messages(req: AIChatRequest) -> list[AIMessage]:
    # The proxy (Claude Code Agent SDK) ignores multi-turn message arrays —
    # each request starts a fresh session. Inject conversation history into
    # the system prompt so the agent sees full context in a single turn.
    history_lines = []
    for h in req.history[-20:]:
        prefix = "USER" if h.role == "user" else "ASSISTANT"
        history_lines.append(f"[{prefix}]: {h.content}")
    history_block = ""
    if history_lines:
        history_block = (
            "\n\n=== CONVERSATION HISTORY (you MUST reference this context) ===\n"
            + "\n".join(history_lines)
            + "\n=== END HISTORY ===\n"
        )
    context_block = ""
    if req.context:
        context_block = (
            "\n\n=== ACTIVE CONTEXT ===\n"
            f"The user is currently browsing: {req.context}\n"
            "Focus your answers on this folder/account. Use the relevant account_id and folder "
            "when calling tools. Do NOT ask the user which account or folder — you already know.\n"
            "=== END CONTEXT ===\n"
        )
    system_with_history = CHAT_SYSTEM_PROMPT + context_block + history_block
    messages = [
        AIMessage("system", system_with_history),
        AIMessage("user", req.message),
    ]
    hist_summary = [(m.role, len(m.content)) for m in messages]
    total = sum(l for _, l in hist_summary)
    logger.info(f"[BUILD-MSGS] {len(messages)} messages ({total} chars), history={len(req.history)} entries: {hist_summary}")
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

    user_id_for_tools = user.id  # capture before generator runs

    async def generate():
        import asyncio
        import re as _re
        import time as _time
        chunk_count = 0
        stream_start = _time.time()
        last_tool_poll = 0  # poll immediately on first iteration
        tool_re = _re.compile(r'\[\[tool:([^\]]+)\]\]')
        try:
            chunk_queue = asyncio.Queue(maxsize=100)
            stream_done = asyncio.Event()

            async def _consume_stream():
                nonlocal chunk_count
                try:
                    async for chunk in llm.stream_chat(messages):
                        m = tool_re.search(chunk)
                        if m:
                            await chunk_queue.put(('progress', m.group(1)))
                            text = tool_re.sub('', chunk).strip()
                            if text:
                                chunk_count += 1
                                await chunk_queue.put(('text', text))
                            continue
                        chunk_count += 1
                        await chunk_queue.put(('text', chunk))
                    logger.info(f"Stream finished: {chunk_count} chunks yielded")
                except Exception as e:
                    logger.warning(f"Stream consume error: {e}")
                    await chunk_queue.put(('error', str(e)))
                finally:
                    stream_done.set()

            stream_task = asyncio.create_task(_consume_stream())
            last_heartbeat = stream_start

            while not stream_done.is_set() or not chunk_queue.empty():
                activities = await _get_tool_activities(user_id_for_tools)
                if activities:
                    yield f"data: {json.dumps({'tools': activities})}\n\n"
                try:
                    kind, data = await asyncio.wait_for(chunk_queue.get(), timeout=0.5)
                    if kind == 'error':
                        yield f"data: {json.dumps({'error': data})}\n\n"
                    elif kind == 'progress':
                        yield f"data: {json.dumps({'progress': data})}\n\n"
                    else:
                        yield f"data: {json.dumps({'text': data})}\n\n"
                except asyncio.TimeoutError:
                    now = _time.time()
                    if now - last_heartbeat >= 5:
                        last_heartbeat = now
                        elapsed = int(now - stream_start)
                        yield f"data: {json.dumps({'heartbeat': True, 'chunks': chunk_count, 'elapsed': elapsed})}\n\n"
            # Final flush
            final_activities = await _get_tool_activities(user_id_for_tools)
            if final_activities:
                yield f"data: {json.dumps({'tools': final_activities})}\n\n"

            await stream_task
        except Exception as e:
            logger.error(f"Stream error: {e}", exc_info=True)
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

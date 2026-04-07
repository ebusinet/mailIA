import json
import logging
from datetime import datetime, timedelta

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.db.session import get_db
from src.db.models import User, MailAccount, Contact, ContactGroup
from src.api.deps import get_current_user
from src.ai.router import get_llm_for_user
from src.ai.base import AIMessage
from src.config import get_settings

logger = logging.getLogger(__name__)
router = APIRouter()

DIGEST_CACHE_TTL = 3600  # 1 hour
_digest_redis: aioredis.Redis | None = None


async def _get_redis() -> aioredis.Redis:
    global _digest_redis
    if _digest_redis is None:
        _digest_redis = aioredis.from_url(get_settings().redis_url, decode_responses=True)
    return _digest_redis


async def _fetch_emails_from_es(user_id: int, days: int = 7, max_emails: int = 500) -> list[dict]:
    """Fetch recent emails from Elasticsearch."""
    from src.search.indexer import get_es_client, _index_name

    es = await get_es_client()
    index = _index_name(user_id)

    try:
        if not await es.indices.exists(index=index):
            return []

        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        body = {
            "size": max_emails,
            "query": {
                "range": {"date": {"gte": since}}
            },
            "sort": [{"date": "desc"}],
            "_source": ["account_id", "uid", "folder", "from_addr", "to_addr",
                        "subject", "body", "date", "has_attachments"],
        }
        result = await es.search(index=index, body=body)
        hits = result.get("hits", {}).get("hits", [])
        return [h["_source"] for h in hits]
    except Exception as e:
        logger.warning(f"ES fetch failed: {e}")
        return []
    finally:
        await es.close()


def _truncate_body(body: str, max_chars: int = 300) -> str:
    if not body:
        return ""
    clean = body.strip()
    if len(clean) <= max_chars:
        return clean
    return clean[:max_chars] + "..."


def _build_email_summary(emails: list[dict], user_accounts: list[str]) -> str:
    """Build a compact text representation of emails for the AI prompt."""
    user_emails_lower = [e.lower() for e in user_accounts]
    lines = []
    for i, e in enumerate(emails):
        from_addr = e.get("from_addr", "?")
        to_addr = e.get("to_addr", "?")
        subject = e.get("subject", "(sans objet)")
        date = e.get("date", "?")
        body = _truncate_body(e.get("body", ""))
        folder = e.get("folder", "?")
        is_sent = from_addr.lower() in user_emails_lower or folder.upper() in ("SENT", "ELEMENTS ENVOYES", "INBOX.SENT", "ÉLÉMENTS ENVOYÉS")

        direction = "ENVOYE" if is_sent else "RECU"
        attachments = " [PJ]" if e.get("has_attachments") else ""

        lines.append(
            f"[{i+1}] {direction} | {date} | De: {from_addr} | A: {to_addr} | "
            f"Objet: {subject}{attachments}\n"
            f"    Extrait: {body}"
        )
    return "\n\n".join(lines)


DIGEST_SYSTEM_PROMPT = """Tu es un assistant d'analyse d'emails professionnels. Tu analyses les emails de la semaine passee et tu produis un rapport structure au format JSON.

IMPORTANT:
- Reponds UNIQUEMENT avec du JSON valide, sans texte avant ni apres.
- Analyse chaque email attentivement pour en extraire les informations pertinentes.
- Quand tu references un email, utilise son numero [N] du listing.
- Les dates et heures doivent etre au format "YYYY-MM-DD HH:MM".
- Sois precis et concis dans tes analyses.

Le JSON doit avoir cette structure exacte:
{
  "pending_actions": [
    {
      "email_index": 1,
      "from": "expediteur",
      "subject": "objet",
      "date": "date",
      "action_needed": "description de l'action attendue",
      "urgency": "high|medium|low"
    }
  ],
  "follow_ups": [
    {
      "email_index": 1,
      "to": "destinataire",
      "subject": "objet",
      "sent_date": "date d'envoi",
      "description": "ce qui a ete envoye et ce qu'on attend"
    }
  ],
  "commitments": [
    {
      "email_index": 1,
      "description": "engagement pris",
      "deadline": "date limite si mentionnee ou null",
      "status": "pending|overdue"
    }
  ],
  "detected_tasks": [
    {
      "email_index": 1,
      "task": "description de la tache",
      "assigned_by": "qui demande",
      "deadline": "date limite ou null",
      "priority": "high|medium|low"
    }
  ],
  "summary": "Synthese de la semaine en 5-10 lignes: sujets principaux, decisions prises, dossiers avances",
  "top_conversations": [
    {
      "subject": "sujet du fil",
      "participants": ["email1", "email2"],
      "email_count": 3,
      "summary": "resume du fil de discussion",
      "email_indices": [1, 5, 12]
    }
  ],
  "new_contacts": [
    {
      "email": "adresse",
      "name": "nom si disponible",
      "context": "contexte du premier echange"
    }
  ],
  "alerts": [
    {
      "type": "negative_tone|missed_deadline|important_missed",
      "email_index": 1,
      "description": "description de l'alerte",
      "severity": "high|medium|low"
    }
  ],
  "analytics": {
    "total_received": 0,
    "total_sent": 0,
    "busiest_day": "YYYY-MM-DD",
    "avg_daily_received": 0,
    "top_contacts": [
      {"email": "addr", "count": 5, "direction": "both|received|sent"}
    ],
    "categories": [
      {"name": "categorie", "count": 5, "percentage": 25}
    ],
    "response_insights": "observations sur les temps de reponse et patterns"
  },
  "contact_suggestions": [
    {
      "email": "adresse de l'expediteur",
      "name": "nom detecte dans les headers",
      "suggested_group": "nom du groupe propose (existant ou nouveau)",
      "reason": "pourquoi ce regroupement (domaine, type d'echange, sujet...)",
      "email_count": 3
    }
  ]
}

IMPORTANT pour contact_suggestions:
- Ne propose QUE les expediteurs qui ne sont PAS dans la liste des contacts connus.
- Regroupe par domaine d'entreprise, type d'interaction (newsletter, commercial, support, collegue, client...).
- Si un groupe existant correspond, utilise son nom exact. Sinon, propose un nom de nouveau groupe.
- Trie par nombre d'emails decroissant (les plus frequents d'abord)."""


@router.get("/weekly")
async def weekly_digest(
    days: int = Query(7, ge=1, le=30),
    force_refresh: bool = Query(False),
    provider_id: int | None = None,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Generate an AI-powered weekly email digest."""

    cache_key = f"digest:{user.id}:days={days}"

    # Check cache
    if not force_refresh:
        try:
            r = await _get_redis()
            cached = await r.get(cache_key)
            if cached:
                return json.loads(cached)
        except Exception:
            pass

    # Fetch user's mail accounts
    accounts_result = await db.execute(
        select(MailAccount).where(MailAccount.user_id == user.id)
    )
    accounts = accounts_result.scalars().all()
    if not accounts:
        raise HTTPException(status_code=400, detail="No mail accounts configured")

    user_emails = [a.imap_user for a in accounts]

    # Fetch user's contacts for new contact detection
    contacts_result = await db.execute(
        select(Contact).where(Contact.user_id == user.id)
    )
    contacts = contacts_result.scalars().all()
    known_emails = set()
    for c in contacts:
        for e in (c.emails or []):
            known_emails.add(e.lower())
    for ue in user_emails:
        known_emails.add(ue.lower())

    # Fetch emails from Elasticsearch
    emails = await _fetch_emails_from_es(user.id, days=days)

    if not emails:
        return {
            "status": "empty",
            "message": "Aucun email indexe sur cette periode",
            "period_days": days,
            "digest": None,
        }

    # Build email summary for AI
    email_text = _build_email_summary(emails, user_emails)

    # Build the AI prompt
    # Fetch existing groups
    groups_result = await db.execute(
        select(ContactGroup).where(ContactGroup.user_id == user.id)
    )
    groups = groups_result.scalars().all()
    groups_list = ", ".join([g.name for g in groups]) if groups else "Aucun"

    known_list = ", ".join(list(known_emails)[:50])
    user_prompt = f"""Voici les {len(emails)} emails des {days} derniers jours.

Comptes de l'utilisateur: {', '.join(user_emails)}
Contacts connus: {known_list}
Groupes de contacts existants: {groups_list}
Date actuelle: {datetime.now().strftime('%Y-%m-%d %H:%M')}

--- EMAILS ---
{email_text}
--- FIN ---

Analyse ces emails et genere le rapport JSON complet."""

    # Call AI
    llm = await get_llm_for_user(db, user, provider_id)
    messages = [
        AIMessage("system", DIGEST_SYSTEM_PROMPT),
        AIMessage("user", user_prompt),
    ]

    try:
        response = await llm.chat(messages)
    except Exception as e:
        logger.error(f"AI digest failed: {e}")
        raise HTTPException(status_code=502, detail=f"AI analysis failed: {e}")

    # Parse JSON from response
    raw = response.content.strip()
    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
    if raw.endswith("```"):
        raw = raw[:-3].strip()
    if raw.startswith("json"):
        raw = raw[4:].strip()

    try:
        digest = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning(f"AI returned non-JSON digest: {raw[:500]}")
        digest = {"raw_response": raw, "parse_error": True}

    result = {
        "status": "ok",
        "period_days": days,
        "email_count": len(emails),
        "model": response.model,
        "provider": response.provider,
        "generated_at": datetime.now().isoformat(),
        "digest": digest,
    }

    # Cache result
    try:
        r = await _get_redis()
        await r.set(cache_key, json.dumps(result, ensure_ascii=False), ex=DIGEST_CACHE_TTL)
    except Exception:
        pass

    return result

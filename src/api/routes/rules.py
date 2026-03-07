from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.db.session import get_db
from src.db.models import User, AIRule
from src.api.deps import get_current_user
from src.rules.parser import parse_rules_markdown

router = APIRouter()


class RuleCreate(BaseModel):
    name: str
    rules_markdown: str
    priority: int = 100
    ai_provider_id: int | None = None


class RuleUpdate(BaseModel):
    name: str | None = None
    rules_markdown: str | None = None
    priority: int | None = None
    is_active: bool | None = None
    ai_provider_id: int | None = None


class RuleResponse(BaseModel):
    id: int
    name: str
    priority: int
    is_active: bool
    rules_markdown: str
    ai_provider_id: int | None
    parsed_count: int

    model_config = {"from_attributes": True}


@router.get("/", response_model=list[RuleResponse])
async def list_rules(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(AIRule).where(AIRule.user_id == user.id).order_by(AIRule.priority)
    )
    rules = result.scalars().all()
    return [
        RuleResponse(
            id=r.id, name=r.name, priority=r.priority, is_active=r.is_active,
            rules_markdown=r.rules_markdown, ai_provider_id=r.ai_provider_id,
            parsed_count=len(parse_rules_markdown(r.rules_markdown)),
        )
        for r in rules
    ]


@router.post("/", response_model=RuleResponse)
async def create_rule(
    req: RuleCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # Validate markdown parses correctly
    parsed = parse_rules_markdown(req.rules_markdown)

    rule = AIRule(
        user_id=user.id,
        name=req.name,
        rules_markdown=req.rules_markdown,
        priority=req.priority,
        ai_provider_id=req.ai_provider_id,
    )
    db.add(rule)
    await db.commit()
    await db.refresh(rule)

    return RuleResponse(
        id=rule.id, name=rule.name, priority=rule.priority, is_active=rule.is_active,
        rules_markdown=rule.rules_markdown, ai_provider_id=rule.ai_provider_id,
        parsed_count=len(parsed),
    )


@router.put("/{rule_id}", response_model=RuleResponse)
async def update_rule(
    rule_id: int,
    req: RuleUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(AIRule).where(AIRule.id == rule_id, AIRule.user_id == user.id)
    )
    rule = result.scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    if req.name is not None:
        rule.name = req.name
    if req.rules_markdown is not None:
        parse_rules_markdown(req.rules_markdown)  # validate
        rule.rules_markdown = req.rules_markdown
    if req.priority is not None:
        rule.priority = req.priority
    if req.is_active is not None:
        rule.is_active = req.is_active
    if req.ai_provider_id is not None:
        rule.ai_provider_id = req.ai_provider_id

    await db.commit()
    await db.refresh(rule)

    return RuleResponse(
        id=rule.id, name=rule.name, priority=rule.priority, is_active=rule.is_active,
        rules_markdown=rule.rules_markdown, ai_provider_id=rule.ai_provider_id,
        parsed_count=len(parse_rules_markdown(rule.rules_markdown)),
    )


@router.delete("/{rule_id}")
async def delete_rule(
    rule_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(AIRule).where(AIRule.id == rule_id, AIRule.user_id == user.id)
    )
    rule = result.scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    await db.delete(rule)
    await db.commit()
    return {"status": "deleted"}


@router.post("/{rule_id}/preview")
async def preview_rule(
    rule_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Preview parsed rules from markdown — shows what the engine will execute."""
    result = await db.execute(
        select(AIRule).where(AIRule.id == rule_id, AIRule.user_id == user.id)
    )
    rule = result.scalar_one_or_none()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    parsed = parse_rules_markdown(rule.rules_markdown)
    return {
        "rules": [
            {
                "name": r.name,
                "condition": {
                    "raw": r.condition.raw,
                    "keywords": r.condition.keywords,
                    "from_patterns": r.condition.from_patterns,
                    "subject_patterns": r.condition.subject_patterns,
                    "has_attachment": r.condition.has_attachment,
                    "needs_ai": r.condition.needs_ai,
                },
                "actions": [{"type": a.action_type, "target": a.target} for a in r.actions],
                "notify": r.notify,
                "notify_summary": r.notify_summary,
            }
            for r in parsed
        ]
    }

"""
Rule execution engine — evaluates rules against emails and performs IMAP actions.
"""
import logging
from dataclasses import dataclass

from src.ai.base import LLMProvider
from src.rules.parser import ParsedRule, RuleCondition

logger = logging.getLogger(__name__)


@dataclass
class EmailContext:
    uid: str
    folder: str
    from_addr: str
    to_addr: str
    subject: str
    body_text: str
    has_attachments: bool
    attachment_names: list[str]
    date: str


@dataclass
class RuleMatch:
    rule: ParsedRule
    confidence: float = 1.0
    ai_explanation: str = ""


async def evaluate_rules(
    email: EmailContext,
    rules: list[ParsedRule],
    llm: LLMProvider | None = None,
) -> list[RuleMatch]:
    """Evaluate all rules against an email, return matching rules sorted by priority."""
    matches = []

    for rule in rules:
        match = await _check_condition(email, rule.condition, llm)
        if match:
            matches.append(RuleMatch(rule=rule, confidence=match[0], ai_explanation=match[1]))

    return matches


async def _check_condition(
    email: EmailContext,
    condition: RuleCondition,
    llm: LLMProvider | None,
) -> tuple[float, str] | None:
    """Check if an email matches a condition. Returns (confidence, explanation) or None."""

    # Simple pattern matching first
    if condition.from_patterns:
        from_lower = email.from_addr.lower()
        if not any(p.lower() in from_lower for p in condition.from_patterns):
            return None

    if condition.subject_patterns:
        subj_lower = email.subject.lower()
        if not any(p.lower() in subj_lower for p in condition.subject_patterns):
            return None

    if condition.keywords:
        text_lower = (email.subject + " " + email.body_text).lower()
        if not any(k.lower() in text_lower for k in condition.keywords):
            if not condition.needs_ai:
                return None
            # Keywords not found but AI might still match — fall through

    if condition.has_attachment is not None:
        if condition.has_attachment != email.has_attachments:
            return None

    # AI evaluation if needed
    if condition.needs_ai and llm:
        try:
            email_summary = (
                f"From: {email.from_addr}\n"
                f"To: {email.to_addr}\n"
                f"Subject: {email.subject}\n"
                f"Date: {email.date}\n"
                f"Has attachments: {email.has_attachments}\n\n"
                f"{email.body_text[:2000]}"
            )
            result = await llm.evaluate_rule(email_summary, condition.raw)
            if result:
                return (0.8, "AI matched")
            return None
        except Exception as e:
            logger.error(f"AI evaluation failed: {e}")
            return None

    # All simple conditions passed
    return (1.0, "pattern match")

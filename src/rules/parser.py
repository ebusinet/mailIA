"""
Parse Markdown rule files into executable rule objects.

Expected format:
    # Rule Set Name

    ## Rule Name
    - **Si**: condition text
    - **Alors**: action (move to FOLDER, flag as X)
    - **Et**: additional action
    - **Notifier**: oui/non [, with summary]
"""
import re
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class RuleCondition:
    raw: str
    keywords: list[str] = field(default_factory=list)
    from_patterns: list[str] = field(default_factory=list)
    subject_patterns: list[str] = field(default_factory=list)
    has_attachment: bool | None = None
    needs_ai: bool = False  # condition requires AI evaluation


@dataclass
class RuleAction:
    action_type: str  # move, flag, label, mark_read, forward
    target: str = ""  # folder name, flag name, email address


@dataclass
class ParsedRule:
    name: str
    condition: RuleCondition
    actions: list[RuleAction] = field(default_factory=list)
    notify: bool = False
    notify_summary: bool = False


def parse_rules_markdown(markdown: str) -> list[ParsedRule]:
    """Parse a markdown rules file into structured rules."""
    rules = []
    current_rule_name = None
    current_sections: dict[str, str] = {}

    for line in markdown.split("\n"):
        line = line.strip()

        # H2 = new rule
        if line.startswith("## "):
            if current_rule_name:
                rules.append(_build_rule(current_rule_name, current_sections))
            current_rule_name = line[3:].strip()
            current_sections = {}
            continue

        # Parse bold-prefixed lines
        match = re.match(r"^-\s+\*\*(\w+)\*\*\s*:\s*(.+)$", line)
        if match and current_rule_name:
            key = match.group(1).lower()
            value = match.group(2).strip()
            if key in current_sections:
                current_sections[key] += "\n" + value
            else:
                current_sections[key] = value

    # Don't forget the last rule
    if current_rule_name:
        rules.append(_build_rule(current_rule_name, current_sections))

    logger.info(f"Parsed {len(rules)} rules from markdown")
    return rules


def _build_rule(name: str, sections: dict[str, str]) -> ParsedRule:
    condition = _parse_condition(sections.get("si", sections.get("if", "")))
    actions = _parse_actions(sections)
    notify_raw = sections.get("notifier", sections.get("notify", "non")).lower()
    notify = notify_raw.startswith("oui") or notify_raw.startswith("yes")
    notify_summary = "resum" in notify_raw or "summary" in notify_raw

    return ParsedRule(
        name=name,
        condition=condition,
        actions=actions,
        notify=notify,
        notify_summary=notify_summary,
    )


def _parse_condition(text: str) -> RuleCondition:
    if not text:
        return RuleCondition(raw="", needs_ai=True)

    condition = RuleCondition(raw=text)
    text_lower = text.lower()

    # Detect AI-needed conditions
    ai_keywords = ["ia detecte", "ai detects", "ton urgent", "urgent tone",
                    "semantique", "semantic", "l'ia", "the ai", "sentiment"]
    if any(kw in text_lower for kw in ai_keywords):
        condition.needs_ai = True

    # Extract "from contains" patterns
    from_matches = re.findall(r'(?:expediteur|from)\s+(?:contient|contains)\s+"([^"]+)"', text_lower)
    condition.from_patterns = from_matches

    # Extract "subject contains" patterns
    subj_matches = re.findall(r'(?:sujet|subject)\s+(?:contient|contains)\s+"([^"]+)"', text_lower)
    condition.subject_patterns = subj_matches

    # Extract quoted keywords for body matching
    keyword_matches = re.findall(r'"([^"]+)"', text)
    condition.keywords = keyword_matches

    # Attachment conditions
    if "piece jointe" in text_lower or "attachment" in text_lower or "pj" in text_lower:
        condition.has_attachment = True

    # If we have patterns but no simple match is possible, AI is needed
    if not condition.from_patterns and not condition.subject_patterns and not condition.keywords:
        condition.needs_ai = True

    return condition


def _parse_actions(sections: dict[str, str]) -> list[RuleAction]:
    actions = []

    for key in ["alors", "then", "et", "and", "action"]:
        text = sections.get(key, "")
        if not text:
            continue

        for line in text.split("\n"):
            line_lower = line.lower().strip()

            if "deplacer" in line_lower or "move" in line_lower:
                folder = re.search(r"(?:vers|to)\s+(.+)", line_lower)
                if folder:
                    actions.append(RuleAction("move", folder.group(1).strip()))

            elif "marquer comme lu" in line_lower or "mark as read" in line_lower:
                actions.append(RuleAction("mark_read"))

            elif "marquer comme important" in line_lower or "mark as important" in line_lower:
                actions.append(RuleAction("flag", "important"))

            elif "flag" in line_lower:
                flag = re.search(r"flag(?:ger)?\s+(?:comme|as)?\s*(.+)", line_lower)
                actions.append(RuleAction("flag", flag.group(1).strip() if flag else "flagged"))

            elif "extraire" in line_lower or "extract" in line_lower:
                actions.append(RuleAction("extract", line.strip()))

    return actions

import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from langchain_core.messages import AIMessage

from shipment_qna_bot.graph.state import GraphState
from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger
from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool
from shipment_qna_bot.utils.runtime import is_test_mode

_chat_tool: Optional[AzureOpenAIChatTool] = None

_OVERVIEW_CACHE: Dict[str, object] = {
    "path": None,
    "mtime": None,
    "text": None,
}


def _get_chat_tool() -> AzureOpenAIChatTool:
    global _chat_tool
    if _chat_tool is None:
        _chat_tool = AzureOpenAIChatTool()
    return _chat_tool


_COMPANY_TOKENS = {
    "mcs",
    "mol",
    "mol consolidation",
    "mol consolidation service",
    "mol consolidation services",
    "mol logistics",
    "molmcs",
    "molmcs.com",
    "starlink",
    "mitsui osk",
    "mitsui osk lines",
}

_OVERVIEW_HINTS = {
    "overview",
    "about",
    "company",
    "who are you",
    "what is",
    "stand for",
    "full form",
    "meaning",
    "mean",
    "tell me about",
    "history",
    "vision",
    "mission",
    "values",
    "ceo",
    "leadership",
    "office",
    "directory",
    "contact",
    "address",
    "phone",
    "service",
    "services",
    "freight",
    "warehouse",
    "distribution",
    "customs",
    "website",
    "social",
    "linkedin",
    "youtube",
    "facebook",
    "starlink",
    "message",
}

_RETRIEVAL_HINTS = {
    "eta",
    "ata",
    "status",
    "track",
    "tracking",
    "shipment",
    "container",
    "booking",
    "po",
    "obl",
    "bol",
    "delay",
    "delayed",
    "arrival",
}

_SOCIAL_CHANNEL_TOKENS = {
    "youtube",
    "facebook",
    "linkedin",
    "linked in",
}

_STARLINK_TOKENS = {
    "starlink",
    "star link",
}

_SECTION_MARKERS = {
    "company_overview": ("**Company Overview**", "**History**"),
    "history": ("**History**", "**Vision Statement**"),
    "vision": ("**Vision Statement**", "**CEO Message**"),
    "ceo": ("**CEO Message**", "**Office Directory list**"),
    "offices": ("**Office Directory list**", "**Services Details:**"),
    "services": ("**Services Details:**", "**MOL Official Website**"),
    "website": ("**MOL Official Website**", "**MOL Official Social Media**"),
    "social": ("**MOL Official Social Media**", None),
}

_REGION_TOKENS = {
    "africa",
    "middle east",
    "america",
    "china",
    "east and south east asia",
    "europe",
    "india subcontinent",
    "oceania",
}


def _find_repo_root(start: Path) -> Path:
    current = start
    for _ in range(8):
        if (current / "pyproject.toml").exists() or (
            current / "requirements.txt"
        ).exists():
            return current
        if current.parent == current:
            break
        current = current.parent
    return start


def _overview_path() -> Path:
    env_path = os.getenv("SHIPMENT_QNA_BOT_OVERVIEW_PATH")
    if env_path:
        return Path(env_path)
    repo_root = _find_repo_root(Path(__file__).resolve())
    return repo_root / "docs" / "overview_info.md"


def _read_overview_text() -> str:
    path = _overview_path()
    try:
        stat = path.stat()
    except FileNotFoundError:
        logger.warning("overview_info.md not found at %s", path)
        return ""

    cached_path = _OVERVIEW_CACHE.get("path")
    cached_mtime = _OVERVIEW_CACHE.get("mtime")
    if cached_path == str(path) and cached_mtime == stat.st_mtime:
        return str(_OVERVIEW_CACHE.get("text") or "")

    try:
        text = path.read_text(encoding="utf-8")
    except Exception as exc:
        logger.warning("Failed to read overview_info.md: %s", exc)
        return ""

    _OVERVIEW_CACHE["path"] = str(path)
    _OVERVIEW_CACHE["mtime"] = stat.st_mtime
    _OVERVIEW_CACHE["text"] = text
    return text


def _extract_keywords(text: str) -> List[str]:
    keywords: List[str] = []
    for line in text.splitlines():
        cleaned = re.sub(r"\*+", "", line).strip()
        if cleaned.lower().startswith("keywords:"):
            raw = cleaned.split(":", 1)[1]
            parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
            keywords.extend(parts)
            break
    return keywords


def _contains_any(text: str, items: Iterable[str]) -> bool:
    for token in items:
        if not token:
            continue
        if len(token) <= 3 and token.isalnum():
            if re.search(rf"\b{re.escape(token)}\b", text):
                return True
        elif token in text:
            return True
    return False


def _looks_like_company_definition_query(
    text: str, company_tokens: Iterable[str]
) -> bool:
    if not _contains_any(text, company_tokens):
        return False

    token_pattern = "|".join(
        sorted(
            {re.escape(t) for t in company_tokens if t and len(t) <= 40},
            key=len,
            reverse=True,
        )
    )
    if not token_pattern:
        return False

    patterns = [
        rf"\bwhat\s+does\s+(?:{token_pattern})\s+stand\s+for\b",
        rf"\bwhat\s+is\s+the\s+full\s+form\s+of\s+(?:{token_pattern})\b",
        rf"\bwhat\s+is\s+(?:{token_pattern})\b",
        rf"\bwho\s+is\s+(?:{token_pattern})\b",
        rf"\b(?:{token_pattern})\s+means?\b",
        rf"\bmeaning\s+of\s+(?:{token_pattern})\b",
    ]
    return any(re.search(p, text) for p in patterns)


def _looks_like_specific_shipment_lookup(text: str) -> bool:
    if re.search(r"\b[a-z]{4}\d{7}\b", text):
        return True
    if re.search(
        r"\b(container|po|booking|obl|bol)\s*(number|no|#)?\s*[:\-]?\s*[a-z0-9]{6,}\b",
        text,
    ):
        return True
    if re.search(r"\bstatus\s+of\s+\w+", text):
        return True
    return False


def should_handle_overview(question: str) -> bool:
    lowered = (question or "").strip().lower()
    if not lowered:
        return False

    overview_text = _read_overview_text()
    keywords = _extract_keywords(overview_text)
    company_tokens = set(_COMPANY_TOKENS)
    for kw in keywords:
        if kw and len(kw) > 2 and kw not in _RETRIEVAL_HINTS:
            company_tokens.add(kw)

    has_company = _contains_any(lowered, company_tokens)
    if not has_company:
        return False

    if _looks_like_company_definition_query(lowered, company_tokens):
        return True

    has_overview_hint = _contains_any(lowered, _OVERVIEW_HINTS) or re.search(
        r"\b(what|who)\s+is\b", lowered
    )

    if not has_overview_hint:
        return False

    if _contains_any(lowered, _RETRIEVAL_HINTS) and _looks_like_specific_shipment_lookup(
        lowered
    ):
        return False

    return True


def _extract_section(text: str, start_marker: str, end_marker: Optional[str]) -> str:
    if not text:
        return ""
    lowered = text.lower()
    start_idx = lowered.find(start_marker.lower())
    if start_idx == -1:
        return ""
    end_idx = len(text)
    if end_marker:
        end_idx = lowered.find(end_marker.lower(), start_idx + len(start_marker))
        if end_idx == -1:
            end_idx = len(text)
    return text[start_idx:end_idx].strip()


def _select_section_key(question: str) -> str:
    lowered = (question or "").lower()
    if _contains_any(lowered, _SOCIAL_CHANNEL_TOKENS) or "social" in lowered:
        return "social"
    if _contains_any(lowered, {"website", "site"}):
        return "website"
    if _contains_any(
        lowered, {"office", "directory", "contact", "address", "phone", "branch"}
    ):
        return "offices"
    if _contains_any(
        lowered,
        {
            "service",
            "services",
            "freight",
            "warehouse",
            "distribution",
            "customs",
            "ocean",
            "air",
        },
    ):
        return "services"
    if _contains_any(lowered, {"history", "founded", "anniversary", "established"}):
        return "history"
    if _contains_any(lowered, {"vision", "mission"}):
        return "vision"
    if "value" in lowered:
        # If they specifically ask for "values" (plural) or "charts",
        # they might want the MOL CHARTS in the CEO section.
        # But "value" (singular) is in the Vision section.
        # We'll return vision as primary but maybe expand context in synthesis.
        return "vision"
    if _contains_any(lowered, {"ceo", "leadership", "management", "message"}):
        return "ceo"
    return "company_overview"


def _parse_office_directory(text: str) -> List[Dict[str, object]]:
    section = _extract_section(
        text,
        _SECTION_MARKERS["offices"][0],
        _SECTION_MARKERS["offices"][1],
    )
    if not section:
        return []

    entries: List[Dict[str, object]] = []
    region: Optional[str] = None
    current: Optional[Dict[str, object]] = None

    for line in section.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("##"):
            region = re.sub(r"[#*]", "", stripped).strip()
            continue
        if stripped.startswith("- **"):
            if current:
                entries.append(current)
            match = re.search(r"\*\*(.+?)\*\*", stripped)
            location = (
                match.group(1).strip() if match else stripped.lstrip("- ").strip()
            )
            current = {
                "region": region,
                "location": location,
                "details": [],
            }
            continue
        if current is not None:
            details = current["details"]
            if isinstance(details, list):
                details.append(stripped)

    if current:
        entries.append(current)

    return entries


def _extract_subsection(section_text: str, header: str) -> str:
    if not section_text:
        return ""
    lines = section_text.splitlines()
    header_line = f"**{header}**".lower()
    start_idx = None
    for i, line in enumerate(lines):
        if line.strip().lower() == header_line:
            start_idx = i
            break
    if start_idx is None:
        return ""
    end_idx = len(lines)
    for j in range(start_idx + 1, len(lines)):
        line = lines[j].strip()
        if line.startswith("**") and line.endswith("**"):
            end_idx = j
            break
    return "\n".join(lines[start_idx:end_idx]).strip()


def _extract_paragraphs_with_keywords(text: str, keywords: Iterable[str]) -> List[str]:
    if not text:
        return []
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    matches: List[str] = []
    for para in paragraphs:
        lowered = para.lower()
        if any(k in lowered for k in keywords):
            matches.append(para)
    return matches


def _split_ceo_block(block: str) -> tuple[str, List[str], List[str]]:
    lines = [l.strip() for l in block.splitlines() if l.strip()]
    if not lines:
        return "", [], []
    if lines[0].startswith("**") and lines[0].endswith("**"):
        lines = lines[1:]
    if not lines:
        return "", [], []

    name = lines[0]
    title_lines: List[str] = []
    message_lines: List[str] = []

    for idx, line in enumerate(lines[1:], start=1):
        if (
            len(title_lines) < 2
            and len(line) <= 80
            and not line.endswith((".", "!", "?"))
        ):
            title_lines.append(line)
            continue
        message_lines = lines[idx:]
        break

    return name, title_lines, message_lines


def _format_ceo_name(region: str, name: str, titles: List[str]) -> str:
    title_text = "; ".join(titles) if titles else "CEO"
    if region:
        return f"{region}: {name} — {title_text}"
    return f"{name} — {title_text}"


def _answer_social_query(question: str, text: str) -> str:
    section = _extract_section(
        text,
        _SECTION_MARKERS["social"][0],
        _SECTION_MARKERS["social"][1],
    )
    if not section:
        return "Social media information is not available in the overview file."

    lowered = (question or "").lower()
    requested_channels = [c for c in _SOCIAL_CHANNEL_TOKENS if c in lowered]

    lines = section.splitlines()
    if requested_channels:
        results: List[str] = []
        for line in lines:
            for channel in requested_channels:
                if channel in line.lower():
                    results.append(line.strip())
                    break
        if results:
            return "\n".join(results).strip()

    return section.strip()


def _collect_location_tokens(
    question: str, locations: Optional[List[str]]
) -> List[str]:
    tokens = []
    if locations:
        tokens.extend([l.strip().lower() for l in locations if l.strip()])

    lowered = (question or "").lower()
    for region in _REGION_TOKENS:
        if region in lowered:
            tokens.append(region)

    if "america" in lowered or "usa" in lowered or "u.s." in lowered:
        tokens.append("america")
        tokens.append("united states")
        tokens.append("usa")

    return list(dict.fromkeys(tokens))


def _entry_matches_tokens(entry: Dict[str, object], tokens: List[str]) -> bool:
    if not tokens:
        return False
    location = str(entry.get("location") or "").lower()
    region = str(entry.get("region") or "").lower()
    for token in tokens:
        if _contains_any(location, [token]) or _contains_any(region, [token]):
            return True
    return False


def _office_city(location: str) -> str:
    parts = [p.strip() for p in location.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[1]
    return location.strip()


def _answer_office_query(
    question: str, text: str, locations: Optional[List[str]]
) -> str:
    entries = _parse_office_directory(text)
    if not entries:
        return "Office directory information is not available in the overview file."

    tokens = _collect_location_tokens(question, locations)
    if not tokens:
        region_counts: Dict[str, int] = {}
        for entry in entries:
            region = str(entry.get("region") or "").strip()
            if not region:
                region = "Other"
            region_counts[region] = region_counts.get(region, 0) + 1

        lines = ["Office summary by region:"]
        for region in sorted(region_counts.keys()):
            count = region_counts[region]
            office_word = "office" if count == 1 else "offices"
            lines.append(f"- {region}: {count} {office_word}")
        lines.append("Ask for a specific country or city for contact details.")
        return "\n".join(lines)

    matches = [e for e in entries if _entry_matches_tokens(e, tokens)]
    if not matches:
        return f"No office entry found for {', '.join(tokens)}."

    lowered = (question or "").lower()
    is_count_query = any(
        phrase in lowered for phrase in ("how many", "number of", "count of")
    )

    if is_count_query:
        cities = sorted({_office_city(str(e.get("location") or "")) for e in matches})
        label = ", ".join(tokens)
        city_part = f" — {', '.join(cities)}" if cities else ""
        office_word = "office" if len(matches) == 1 else "offices"
        return f"{label}: {len(matches)} {office_word}{city_part}."

    lines = []
    for entry in matches:
        location = str(entry.get("location") or "-")
        details = entry.get("details") or []
        detail_str = "; ".join([str(d) for d in details if str(d).strip()])
        if detail_str:
            lines.append(f"- {location} — {detail_str}")
        else:
            lines.append(f"- {location}")

    return "Office details:\n" + "\n".join(lines)


def _answer_ceo_query(question: str, text: str) -> str:
    section = _extract_section(
        text,
        _SECTION_MARKERS["ceo"][0],
        _SECTION_MARKERS["ceo"][1],
    )
    if not section:
        return "CEO information is not available in the overview file."

    lowered = (question or "").lower()
    is_name_query = "name" in lowered or re.search(r"\bwho\s+is\b", lowered)
    wants_message = "message" in lowered or "note" in lowered or "statement" in lowered

    if "america" in lowered or "usa" in lowered or "u.s." in lowered:
        block = _extract_subsection(section, "MCS America")
        if block:
            name, titles, message_lines = _split_ceo_block(block)
            if is_name_query and not wants_message:
                return _format_ceo_name("MCS America", name, titles)
            if wants_message and message_lines:
                return "\n".join(message_lines).strip()
            return f"**CEO Message**\n\n{block}"
    if "hong kong" in lowered or "hk" in lowered:
        block = _extract_subsection(section, "MCS Hong Kong")
        if block:
            name, titles, message_lines = _split_ceo_block(block)
            if is_name_query and not wants_message:
                return _format_ceo_name("MCS Hong Kong", name, titles)
            if wants_message and message_lines:
                return "\n".join(message_lines).strip()
            return f"**CEO Message**\n\n{block}"

    return section.strip()


def _synthesize_static_answer(question: str, context_text: str) -> Dict[str, Any]:
    """
    Uses LLM to synthesize a concise answer from the extracted static context.
    """
    if is_test_mode():
        return {"answer": context_text, "usage": {}}

    system_prompt = (
        "You are a helpful logistics assistant for MCS (MOL Consolidation Service).\n"
        "Answer the user's question using ONLY the provided context from the company's internal documentation.\n"
        "Guidelines:\n"
        "1. Be concise and professional.\n"
        "2. Do not include unrelated sections, headers, or internal directory markers unless directly asked.\n"
        "3. If the answer is not in the context, politely say you don't have that specific information.\n"
        "4. Use Markdown for formatting (bolding, lists) to make the answer readable.\n"
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": f"Context:\n{context_text}\n\nQuestion: {question}",
        },
    ]

    try:
        tool = _get_chat_tool()
        response = tool.chat_completion(messages, temperature=0.0)
        return {"answer": response["content"], "usage": response.get("usage", {})}
    except Exception as exc:
        logger.error("Failed to synthesize static answer: %s", exc)
        return {"answer": context_text, "usage": {}}


def build_static_overview_answer(
    question: str, extracted_locations: Optional[List[str]] = None
) -> str:
    text = _read_overview_text()
    if not text.strip():
        return (
            "Company overview information is not configured yet. "
            "Please update docs/overview_info.md."
        )

    lowered = (question or "").lower()
    if _contains_any(lowered, _STARLINK_TOKENS):
        matches = _extract_paragraphs_with_keywords(text, _STARLINK_TOKENS)
        if matches:
            return "\n\n".join(matches[:2]).strip()

    # Special handling for Vision & Values
    # Since "Vision" is its own section but "Values" (MOL CHARTS) is in the CEO Message,
    # if the question mentions both, we should provide both sections to the LLM.
    if ("vision" in lowered or "mission" in lowered) and (
        "value" in lowered or "chart" in lowered
    ):
        vision_sec = _extract_section(
            text, _SECTION_MARKERS["vision"][0], _SECTION_MARKERS["vision"][1]
        )
        ceo_sec = _extract_section(
            text, _SECTION_MARKERS["ceo"][0], _SECTION_MARKERS["ceo"][1]
        )
        return f"{vision_sec}\n\n{ceo_sec}"

    # If asking for values/charts specifically
    if "values" in lowered or "mol chart" in lowered or "charts" in lowered:
        ceo_sec = _extract_section(
            text, _SECTION_MARKERS["ceo"][0], _SECTION_MARKERS["ceo"][1]
        )
        vision_sec = _extract_section(
            text, _SECTION_MARKERS["vision"][0], _SECTION_MARKERS["vision"][1]
        )
        return f"{vision_sec}\n\n{ceo_sec}"

    section_key = _select_section_key(question)
    if section_key == "offices":
        return _answer_office_query(question, text, extracted_locations)
    if section_key == "ceo":
        return _answer_ceo_query(question, text)
    if section_key == "social":
        return _answer_social_query(question, text)

    start_marker, end_marker = _SECTION_MARKERS.get(section_key, (None, None))
    section_text = ""
    if start_marker:
        section_text = _extract_section(text, start_marker, end_marker)

    if section_key == "website":
        website = _extract_section(
            text,
            _SECTION_MARKERS["website"][0],
            _SECTION_MARKERS["website"][1],
        )
        if website.strip():
            return website.strip()

    if section_text.strip():
        return section_text

    # Fallback to company overview only, never dump the entire file.
    overview = _extract_section(
        text,
        _SECTION_MARKERS["company_overview"][0],
        _SECTION_MARKERS["company_overview"][1],
    )
    if overview.strip():
        return overview.strip()

    return "Company overview information is not available in the overview file."


def static_greet_info_node(state: GraphState) -> GraphState:
    with log_node_execution(
        "StaticInfo",
        {
            "question": (
                state.get("normalized_question") or state.get("question_raw") or ""
            )[:120]
        },
        state_ref=state,
    ):
        question = state.get("normalized_question") or state.get("question_raw") or ""
        extracted = state.get("extracted_ids") or {}
        locations = extracted.get("location") or []

        # 1. Get raw context from the markdown file based on keywords
        raw_context = build_static_overview_answer(question, locations)

        # 2. Refine the answer using LLM for better context awareness
        synthesis = _synthesize_static_answer(question, raw_context)
        answer_text = synthesis["answer"]
        usage = synthesis["usage"]

        # 3. Update usage metadata
        usage_metadata = state.get("usage_metadata") or {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
        for k, v in usage.items():
            usage_metadata[k] = usage_metadata.get(k, 0) + v

        state.update(
            {
                "intent": "company_overview",
                "answer_text": answer_text,
                "is_satisfied": True,
                "messages": [AIMessage(content=answer_text)],
                "usage_metadata": usage_metadata,
            }
        )

        if "not configured yet" in answer_text.lower():
            state["notices"] = [
                "Static overview file missing or empty. Update docs/overview_info.md.",
            ]

        return state

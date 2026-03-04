from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from langchain_core.messages import AIMessage

from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context
from shipment_qna_bot.tools.blob_manager import BlobAnalyticsManager
from shipment_qna_bot.tools.weather_tool import WeatherTool
from shipment_qna_bot.utils.config import is_weather_enabled

_BLOB_MGR: Optional[BlobAnalyticsManager] = None
_WEATHER_TOOL: Optional[WeatherTool] = None

_CLOSED_STATUS_TERMS = (
    "deliver",
    "empty return",
    "returned empty",
    "closed",
    "cancel",
)


def _get_blob_manager() -> BlobAnalyticsManager:
    global _BLOB_MGR
    if _BLOB_MGR is None:
        _BLOB_MGR = BlobAnalyticsManager()
    return _BLOB_MGR


def _get_weather_tool() -> WeatherTool:
    global _WEATHER_TOOL
    if _WEATHER_TOOL is None:
        _WEATHER_TOOL = WeatherTool()
    return _WEATHER_TOOL


def _normalize_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _dedupe(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None or value == "" or str(value).lower() in {"nat", "nan", "none"}:
        return None
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _string_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip().upper() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip().upper() for item in value if str(item).strip()]
    if isinstance(value, set):
        return [str(item).strip().upper() for item in value if str(item).strip()]
    raw = str(value).strip()
    if not raw:
        return []
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    parts = [part.strip(" '\"").upper() for part in raw.split(",") if part.strip()]
    return [part for part in parts if part]


def _question_mentions_final_destination(question: str) -> bool:
    lowered = (question or "").lower()
    return any(
        phrase in lowered
        for phrase in (
            "final destination",
            "distribution center",
            "distribution centre",
            "place of delivery",
            "delivery point",
        )
    )


def _question_mentions_origin(question: str) -> bool:
    lowered = (question or "").lower()
    return any(
        phrase in lowered
        for phrase in (
            "load port",
            "origin",
            "port of loading",
            "place of receipt",
            "departure port",
        )
    )


def _selected_location_columns(question: str) -> List[str]:
    if _question_mentions_final_destination(question):
        return ["final_destination", "place_of_delivery", "discharge_port", "load_port"]
    if _question_mentions_origin(question):
        return ["load_port", "final_load_port", "place_of_receipt", "discharge_port"]
    return ["discharge_port", "final_destination", "load_port", "place_of_delivery"]


def _selected_eta_columns(question: str) -> List[str]:
    if _question_mentions_final_destination(question):
        return ["best_eta_fd_date", "optimal_eta_fd_date", "eta_fd_date"]
    if _question_mentions_origin(question):
        return ["etd_flp_date", "etd_lp_date", "atd_flp_date", "atd_lp_date"]
    return ["best_eta_dp_date", "eta_dp_date", "derived_ata_dp_date", "ata_dp_date"]


def _get_now_utc(state: Dict[str, Any]) -> datetime:
    raw = state.get("now_utc")
    if raw:
        try:
            parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            pass
    return datetime.now(timezone.utc)


def _row_matches_requested_ids(row: pd.Series, extracted: Dict[str, Any]) -> bool:
    containers = {
        item.upper() for item in _string_list(extracted.get("container_number"))
    }
    po_numbers = set(_string_list(extracted.get("po_numbers")))
    booking_numbers = set(_string_list(extracted.get("booking_numbers")))
    obl_numbers = set(_string_list(extracted.get("obl_nos")))

    if not any((containers, po_numbers, booking_numbers, obl_numbers)):
        return True

    container = str(row.get("container_number") or "").strip().upper()
    if container and container in containers:
        return True

    if po_numbers and po_numbers.intersection(_string_list(row.get("po_numbers"))):
        return True
    if booking_numbers and booking_numbers.intersection(
        _string_list(row.get("booking_numbers"))
    ):
        return True
    if obl_numbers and obl_numbers.intersection(_string_list(row.get("obl_nos"))):
        return True
    return False


def _row_matches_locations(
    row: pd.Series, location_terms: List[str], location_columns: List[str]
) -> bool:
    if not location_terms:
        return True
    candidate_values = [str(row.get(col) or "") for col in location_columns]
    normalized_values = [_normalize_text(value) for value in candidate_values if value]
    return any(term in value for term in location_terms for value in normalized_values)


def _row_is_open(row: pd.Series) -> bool:
    status = str(row.get("shipment_status") or "").lower()
    if status and any(term in status for term in _CLOSED_STATUS_TERMS):
        return False
    return True


def _row_relevant_in_window(
    row: pd.Series, eta_columns: List[str], now_utc: datetime, window_days: int
) -> bool:
    window_end = now_utc + timedelta(days=window_days)
    for col in eta_columns:
        dt = _parse_dt(row.get(col))
        if dt is None:
            continue
        if now_utc - timedelta(days=2) <= dt <= window_end:
            return True
    return False


def _row_sort_key(
    row: pd.Series, eta_columns: List[str], now_utc: datetime
) -> tuple[int, float]:
    hot_flag = 1 if bool(row.get("hot_container_flag")) else 0
    dt = None
    for col in eta_columns:
        dt = _parse_dt(row.get(col))
        if dt:
            break
    timestamp = dt.timestamp() if dt else (now_utc + timedelta(days=999)).timestamp()
    return (-hot_flag, timestamp)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if hasattr(value, "isoformat") and not isinstance(value, str):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return value


def _build_table_row(
    location: str,
    shipment_count: int,
    hot_shipments: int,
    impact: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if impact is None:
        return {
            "location": location,
            "impact_level": "unavailable",
            "shipment_count": shipment_count,
            "hot_shipments": hot_shipments,
            "forecast_window": "next 72h",
            "current_condition": "unavailable",
            "peak_wind_kph": None,
            "peak_gust_kph": None,
            "precipitation_mm": None,
            "impact_reasons": "Live weather lookup failed",
            "resolution": "unavailable",
        }

    reasons = (
        ", ".join(impact.get("impact_reasons") or []) or "No material disruption signal"
    )
    return {
        "location": impact.get("location") or location,
        "impact_level": impact.get("impact_level") or "none",
        "shipment_count": shipment_count,
        "hot_shipments": hot_shipments,
        "forecast_window": f"next {impact.get('forecast_window_hours', 72)}h",
        "current_condition": impact.get("condition"),
        "peak_wind_kph": impact.get("peak_wind_kph"),
        "peak_gust_kph": impact.get("peak_gust_kph"),
        "precipitation_mm": impact.get("precipitation_total_mm"),
        "impact_reasons": reasons,
        "resolution": impact.get("resolution_confidence") or "medium",
    }


def _build_answer(
    question: str,
    table_rows: List[Dict[str, Any]],
    total_rows: int,
    total_shipments: int,
) -> str:
    available_rows = [
        row for row in table_rows if row.get("impact_level") not in {"unavailable"}
    ]
    high_rows = [row for row in available_rows if row.get("impact_level") == "high"]
    medium_rows = [row for row in available_rows if row.get("impact_level") == "medium"]

    if not available_rows:
        return (
            f"I found {total_shipments} shipment records tied to {total_rows} locations, "
            "but live weather data could not be retrieved for them right now."
        )

    if len(available_rows) == 1:
        row = available_rows[0]
        return (
            f"Weather impact for {row['location']} is {row['impact_level']} over {row['forecast_window']} "
            f"across {row['shipment_count']} scoped shipment(s). "
            f"Current condition: {row['current_condition']}. Drivers: {row['impact_reasons']}."
        )

    summary_parts = [
        f"Weather outlook across {len(available_rows)} locations covering {total_shipments} scoped shipments",
    ]
    if high_rows:
        summary_parts.append(
            "shows highest operational risk at "
            + ", ".join(row["location"] for row in high_rows[:3])
        )
    elif medium_rows:
        summary_parts.append(
            "shows moderate risk at "
            + ", ".join(row["location"] for row in medium_rows[:3])
        )
    else:
        summary_parts.append("does not show a strong disruption signal")

    detail_rows = sorted(
        available_rows,
        key=lambda row: (
            {"high": 3, "medium": 2, "low": 1, "none": 0}.get(
                str(row.get("impact_level")), 0
            ),
            row.get("shipment_count") or 0,
        ),
        reverse=True,
    )
    top_details = [
        f"{row['location']} ({row['impact_level']}, {row['shipment_count']} shipments, {row['impact_reasons']})"
        for row in detail_rows[:3]
    ]
    answer = " ".join(summary_parts) + "."
    if top_details:
        answer += " Key ports: " + "; ".join(top_details) + "."
    if "weather" not in question.lower():
        answer += " This is a live port-weather impact view."
    return answer


def _build_citations(
    grouped_rows: List[Dict[str, Any]],
    location_columns: List[str],
    eta_columns: List[str],
) -> List[Dict[str, Any]]:
    citations: List[Dict[str, Any]] = []
    seen = set()
    field_used = _dedupe(["shipment_status", *location_columns[:1], *eta_columns[:1]])
    for group in grouped_rows:
        for row in group.get("sample_rows") or []:
            doc_id = (
                row.get("document_id")
                or row.get("carr_eqp_uid")
                or row.get("container_number")
                or row.get("location")
            )
            doc_id = str(doc_id or "").strip()
            if not doc_id or doc_id in seen:
                continue
            seen.add(doc_id)
            citations.append(
                {
                    "doc_id": doc_id,
                    "container_number": row.get("container_number"),
                    "field_used": field_used,
                }
            )
            if len(citations) >= 5:
                return citations
    return citations


def _append_notice(state: Dict[str, Any], message: str) -> None:
    if not message:
        return
    notices = state.setdefault("notices", [])
    if message not in notices:
        notices.append(message)


def weather_impact_node(state: Dict[str, Any]) -> Dict[str, Any]:
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )

    with log_node_execution(
        "WeatherImpact",
        {"intent": state.get("intent", "-")},
        state_ref=state,
    ):
        if not is_weather_enabled():
            answer = "Live weather impact lookups are currently disabled."
            state["answer_text"] = answer
            state["messages"] = [AIMessage(content=answer)]
            state["is_satisfied"] = True
            return state

        consignee_codes = state.get("consignee_codes") or []
        if not consignee_codes:
            answer = "No consignee codes were provided, so I cannot evaluate shipment weather impact."
            state["answer_text"] = answer
            state["messages"] = [AIMessage(content=answer)]
            state["is_satisfied"] = True
            return state

        try:
            df = _get_blob_manager().load_filtered_data(consignee_codes)
        except Exception as exc:
            logger.error("Weather impact data load failed: %s", exc)
            state.setdefault("errors", []).append(f"Weather data load failed: {exc}")
            answer = "I could not load the scoped shipment dataset needed for weather impact analysis."
            state["answer_text"] = answer
            state["messages"] = [AIMessage(content=answer)]
            state["is_satisfied"] = True
            return state

        if df.empty:
            answer = "I could not find any shipments in your authorized scope to analyze for weather impact."
            state["answer_text"] = answer
            state["messages"] = [AIMessage(content=answer)]
            state["is_satisfied"] = True
            return state

        question = str(
            state.get("question_raw") or state.get("normalized_question") or ""
        )
        extracted = state.get("extracted_ids") or {}
        location_columns = _selected_location_columns(question)
        eta_columns = _selected_eta_columns(question)
        now_utc = _get_now_utc(state)
        window_days = int(state.get("time_window_days") or 14)

        scoped_df = df[
            df.apply(lambda row: _row_matches_requested_ids(row, extracted), axis=1)
        ].copy()

        location_terms = [
            _normalize_text(item)
            for item in (extracted.get("location") or [])
            if _normalize_text(item)
        ]
        if location_terms:
            scoped_df = scoped_df[
                scoped_df.apply(
                    lambda row: _row_matches_locations(
                        row, location_terms, location_columns
                    ),
                    axis=1,
                )
            ].copy()

        if scoped_df.empty:
            answer = "I could not match the requested shipment or location filters to any scoped records for weather analysis."
            state["answer_text"] = answer
            state["messages"] = [AIMessage(content=answer)]
            state["is_satisfied"] = True
            return state

        if not any(
            _string_list(extracted.get(key))
            for key in ("container_number", "po_numbers", "booking_numbers", "obl_nos")
        ):
            open_df = scoped_df[scoped_df.apply(_row_is_open, axis=1)].copy()
            if not open_df.empty:
                scoped_df = open_df

            window_df = scoped_df[
                scoped_df.apply(
                    lambda row: _row_relevant_in_window(
                        row, eta_columns, now_utc, window_days
                    ),
                    axis=1,
                )
            ].copy()
            if not window_df.empty:
                scoped_df = window_df

        if not scoped_df.empty:
            ordered = sorted(
                [row for _, row in scoped_df.iterrows()],
                key=lambda row: _row_sort_key(row, eta_columns, now_utc),
            )
            scoped_df = pd.DataFrame(ordered)

        grouped: List[Dict[str, Any]] = []
        location_map: Dict[str, Dict[str, Any]] = {}
        for _, row in scoped_df.iterrows():
            location = ""
            for col in location_columns:
                candidate = str(row.get(col) or "").strip()
                if candidate:
                    location = candidate
                    break
            if not location:
                continue

            key = _normalize_text(location)
            bucket = location_map.setdefault(
                key,
                {
                    "location": location,
                    "rows": [],
                    "shipment_count": 0,
                    "hot_shipments": 0,
                    "sample_rows": [],
                },
            )
            row_dict = {
                column: _serialize_value(row.get(column)) for column in row.index
            }
            bucket["rows"].append(row_dict)
            bucket["shipment_count"] += 1
            if bool(row.get("hot_container_flag")):
                bucket["hot_shipments"] += 1
            if len(bucket["sample_rows"]) < 2:
                bucket["sample_rows"].append(row_dict)

        grouped = sorted(
            location_map.values(),
            key=lambda item: (
                -item["hot_shipments"],
                -item["shipment_count"],
                item["location"],
            ),
        )

        if not grouped:
            answer = "I found shipments, but none of them contained a usable port or destination value for weather analysis."
            state["answer_text"] = answer
            state["messages"] = [AIMessage(content=answer)]
            state["is_satisfied"] = True
            return state

        max_locations = int(os.getenv("WEATHER_MAX_LOCATIONS", "12"))
        selected_groups = grouped[:max_locations]
        if len(grouped) > max_locations:
            _append_notice(
                state,
                f"Weather impact was calculated for the top {max_locations} locations by scoped shipment count; {len(grouped) - max_locations} additional locations were not queried live.",
            )

        weather_tool = _get_weather_tool()
        table_rows: List[Dict[str, Any]] = []
        chart_rows: List[Dict[str, Any]] = []
        low_confidence_count = 0

        for group in selected_groups:
            impact = weather_tool.get_impact_for_location(
                group["location"], forecast_days=3
            )
            for notice in weather_tool.consume_transport_notices():
                _append_notice(state, notice)

            if impact and impact.get("resolution_confidence") in {"medium", "low"}:
                low_confidence_count += 1

            table_row = _build_table_row(
                location=group["location"],
                shipment_count=group["shipment_count"],
                hot_shipments=group["hot_shipments"],
                impact=impact,
            )
            table_rows.append(table_row)
            if impact is not None:
                chart_rows.append(
                    {
                        "location": table_row["location"],
                        "impact_score": impact.get("impact_score", 0),
                        "shipment_count": group["shipment_count"],
                        "impact_level": impact.get("impact_level", "none"),
                    }
                )

        if low_confidence_count:
            _append_notice(
                state,
                f"{low_confidence_count} location lookups used a city-level weather proxy because a direct port geocode was not available.",
            )

        total_shipments = sum(row["shipment_count"] for row in table_rows)
        answer = _build_answer(question, table_rows, len(table_rows), total_shipments)

        state["answer_text"] = answer
        state["table_spec"] = {
            "columns": [
                "location",
                "impact_level",
                "shipment_count",
                "hot_shipments",
                "forecast_window",
                "current_condition",
                "peak_wind_kph",
                "peak_gust_kph",
                "precipitation_mm",
                "impact_reasons",
                "resolution",
            ],
            "rows": table_rows,
            "title": "Weather Impact Outlook",
        }
        state["chart_spec"] = (
            {
                "kind": "bar",
                "title": "Weather Impact by Location",
                "data": chart_rows,
                "encodings": {
                    "x": "location",
                    "y": "impact_score",
                    "color": "impact_level",
                    "size": "shipment_count",
                },
            }
            if chart_rows
            else None
        )
        state["citations"] = _build_citations(
            selected_groups, location_columns, eta_columns
        )
        state["messages"] = [AIMessage(content=answer)]
        state["is_satisfied"] = True
        return state

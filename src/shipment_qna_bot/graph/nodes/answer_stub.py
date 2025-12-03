# src/shipment_qna_bot/graph/nodes/answer_stub.py

from __future__ import annotations

from typing import Any, Dict, List

from shipment_qna_bot.logging.graph_tracing import log_node_execution
from shipment_qna_bot.logging.logger import logger, set_log_context


def _sync_ctx(state: Dict[str, Any]) -> None:
    set_log_context(
        conversation_id=state.get("conversation_id", "-"),
        consignee_codes=state.get("consignee_codes", []),
        intent=state.get("intent", "-"),
    )


def answer_stub_node(state: Dict[str, Any]) -> Dict[str, Any]:
    _sync_ctx(state)

    with log_node_execution(
        "AnswerStub",
        {
            "intent": state.get("intent", "-"),
            "hits": len(state.get("hits") or []),
            "errors": (state.get("errors") or [])[-1:] if state.get("errors") else [],
        },
    ):
        hits = state.get("hits") or []
        notices: List[str] = list(state.get("notices") or [])

        if state.get("errors"):
            notices.append("Some tools failed; results may be incomplete.")

        if not hits:
            state["answer_text"] = (
                "I couldn't find matching shipments within your authorized consignee scope.\n"
                "Try including an identifier (container / PO / OBL / booking) or rephrase the request."
            )
            state["evidence"] = []
            state["notices"] = notices
            return state

        lines: List[str] = [
            f"[DEV] Retrieval succeeded. Showing top {min(5, len(hits))} candidates (no answer synthesis yet):"
        ]
        evidences: List[Dict[str, Any]] = []
        for hit in hits[:5]:
            doc_id = hit.get("doc_id") or "-"
            container = hit.get("container") or "-"
            score = hit.get("score")
            lines.append(f"- doc={doc_id} container={container} score={score}")
            evidences.append(
                {
                    "doc_id": doc_id,
                    "container": container,
                    "fields_used": ["content"],
                    "score": score,
                }
            )
        state["answer_text"] = "\n".join(lines)
        state["evidence"] = evidences
        state["notices"] = notices

        return state

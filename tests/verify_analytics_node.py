import json
import os
import sys
from pathlib import Path

import pytest

# Add src to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))

# Ensure we are NOT in test mode to hit real LLM and Real Data
if "SHIPMENT_QNA_BOT_TEST_MODE" in os.environ:
    del os.environ["SHIPMENT_QNA_BOT_TEST_MODE"]

from shipment_qna_bot.graph.nodes.analytics_planner import \
    analytics_planner_node


def _run_real_analytics_node() -> bool:
    print("Testing Real Analytics Node (End-to-End)...")

    # Setup state
    # We use '0002990' which we know has data from previous inspection
    state = {
        "question_raw": "How many shipments do I have in total?",
        "normalized_question": "Total shipment count",
        "consignee_codes": ["0002990"],
        "intent": "analytics",
        "conversation_id": "test_e2e_node",
        "errors": [],
        "notices": [],
    }

    try:
        print("Invoking analytics_planner_node...")
        new_state = analytics_planner_node(state)

        if new_state.get("errors"):
            print(f"FAILURE: Node reported errors: {new_state['errors']}")
            return False

        ans = new_state.get("answer_text")
        print("\n--- NODE RESPONSE ---")
        print(ans)
        print("---------------------\n")

        if ans and "Here is what I found:" in ans:
            print("SUCCESS: Node returned a valid analysis answer.")
            return True
        else:
            print("FAILURE: No valid answer_text found.")
            return False

    except Exception as e:
        print(f"CRITICAL FAILURE: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_real_analytics_node():
    if os.getenv("SHIPMENT_QNA_BOT_RUN_INTEGRATION") != "1":
        pytest.skip("Integration-only verification.")
    assert _run_real_analytics_node() is True


if __name__ == "__main__":
    if _run_real_analytics_node():
        print("Final E2E Verification Passed")
        sys.exit(0)
    else:
        sys.exit(1)

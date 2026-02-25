import json
import os
import sys
from pathlib import Path

import pandas as pd

# Add src to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))

# Ensure we are NOT in test mode to hit real LLM and Real Data
if "SHIPMENT_QNA_BOT_TEST_MODE" in os.environ:
    del os.environ["SHIPMENT_QNA_BOT_TEST_MODE"]

from shipment_qna_bot.graph.nodes.analytics_planner import \
    analytics_planner_node

CONSIGNEE_CODES = "0000866,0001363,0001540,0001615,0002679,0002990,0003427,0003905,0004932,0005052,0005053,0005056,0005171,0005176,0009633,0013505,0021472,0023453,0028662,0028664,0029594,0030961,0030962,0037361,0048392".split(
    ","
)

TEST_QUERIES = [
    {"category": "carrier", "query": "Which carriers are used for my shipments?"},
    {"category": "vessel", "query": "List the vessels for my recent shipments."},
    {
        "category": "weight",
        "query": "What is the total weight (wt) of all my shipments?",
    },
    {
        "category": "volume",
        "query": "What is the total volume (vol) of all my shipments?",
    },
    {"category": "count", "query": "Count the number of shipments I have."},
    {
        "category": "details_count",
        "query": "Give me the total cargo detail count for all shipments.",
    },
    {
        "category": "shipper",
        "query": "Who are the shippers/suppliers for my shipments?",
    },
    {
        "category": "manufacturer",
        "query": "List the manufacturers involved in my shipments.",
    },
]


def run_tests():
    results = []

    # We can test with a subset of consignee codes or all of them.
    # Testing all might be slow and expensive. Let's pick a few that likely have data.
    # The user asked to use these codes to filter.

    codes_to_use = CONSIGNEE_CODES

    for q_item in TEST_QUERIES:
        print(f"\nTesting Category: {q_item['category']}")
        print(f"Query: {q_item['query']}")

        state = {
            "question_raw": q_item["query"],
            "normalized_question": q_item["query"],
            "consignee_codes": codes_to_use,
            "intent": "analytics",
            "conversation_id": f"accuracy_test_{q_item['category']}",
            "errors": [],
            "notices": [],
        }

        try:
            new_state = analytics_planner_node(state)

            error_occured = len(new_state.get("errors", [])) > 0
            ans = new_state.get("answer_text", "")

            results.append(
                {
                    "category": q_item["category"],
                    "query": q_item["query"],
                    "success": not error_occured,
                    "errors": new_state.get("errors", []),
                    "answer": ans,
                }
            )

            print(f"Success: {not error_occured}")
            if error_occured:
                print(f"Errors: {new_state.get('errors')}")
            else:
                print(f"Answer Sample: {ans[:200]}...")

        except Exception as e:
            print(f"Exception: {e}")
            results.append(
                {
                    "category": q_item["category"],
                    "query": q_item["query"],
                    "success": False,
                    "errors": [str(e)],
                    "answer": "",
                }
            )

    # Save results
    with open("tests/accuracy_test_results.json", "w") as f:
        json.dump(results, f, indent=2)

    print("\nTest run complete. Results saved to tests/accuracy_test_results.json")


if __name__ == "__main__":
    run_tests()

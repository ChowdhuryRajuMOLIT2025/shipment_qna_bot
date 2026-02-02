__test__ = False

import asyncio
import os
import sys

# Ensure src is in python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from shipment_qna_bot.graph.builder import graph_app
from shipment_qna_bot.logging.logger import logger


async def test_trace(question: str, consignee_codes: list[str]):
    print(f"\n--- Testing Question: {question} ---")
    print(f"Scope: {consignee_codes}")

    # Initial state
    initial_state = {
        "question_raw": question,
        "consignee_codes": consignee_codes,
        "conversation_id": "test-dev-001",
    }

    # config
    config = {"configurable": {"thread_id": "test-thread-1"}}

    # Execute graph
    final_state = None
    try:
        final_state = await graph_app.ainvoke(initial_state, config=config)
    except Exception as e:
        import traceback

        traceback.print_exc()
        print(f"Graph execution failed: {e}")
        return

    # Check results
    print("\n--- Final State ---")
    intent = final_state.get("intent")
    print(f"Intent: {intent}")

    if intent in ["retrieval", "status", "eta", "delay"] or final_state.get(
        "answer_text"
    ):
        hits = final_state.get("hits", [])
        print(f"Hits: {len(hits)}")
        for idx, hit in enumerate(hits[:3]):
            print(
                f"  Hit {idx+1}: {hit.get('container_number')} (Score: {hit.get('score')})"
            )

        answer = final_state.get("answer_text")
        print("\n--- Answer Text ---")
        print(answer)
    else:
        print("Intent was not retrieval/status.")


def main():
    if len(sys.argv) > 1:
        question = sys.argv[1]
        consignees = ["0025833"]  # default
        if len(sys.argv) > 2:
            consignees = sys.argv[2].split(",")
    else:
        # Default Test case
        container = "OOCU8049862"
        consignees = ["0025833"]
        question = f"Where is container {container}?"

    asyncio.run(test_trace(question, consignees))


if __name__ == "__main__":
    main()

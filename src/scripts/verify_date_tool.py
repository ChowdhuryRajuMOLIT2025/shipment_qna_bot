import sys

from shipment_qna_bot.graph.builder import run_graph


def test_date_query():
    questions = ["Show me shipments arriving today", "What is today's date?"]

    with open("verify_output.txt", "w", encoding="utf-8") as f:
        for q in questions:
            f.write(f"\n--- Testing Question: {q} ---\n")
            print(f"Testing: {q}")
            state = {
                "conversation_id": "test_date_tool_fix",
                "question_raw": q,
                "consignee_codes": ["123"],
            }

            try:
                result = run_graph(state)
                plan = result.get("retrieval_plan")
                f.write(f"Retrieval Plan: {plan}\n")
                if "retry_count" in result:
                    f.write(f"Retry Count: {result['retry_count']}\n")
            except Exception as e:
                f.write(f"Error: {e}\n")
                print(f"Error: {e}")


if __name__ == "__main__":
    test_date_query()

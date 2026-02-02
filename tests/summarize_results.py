import json
import os

results_path = (
    r"c:\Users\CHOWDHURYRaju\Desktop\shipment_qna_bot\tests\accuracy_test_results.json"
)

if os.path.exists(results_path):
    with open(results_path, "r", encoding="utf-8") as f:
        results = json.load(f)

    print(f"{'Category':<15} | {'Success':<8} | {'Details'}")
    print("-" * 80)
    for r in results:
        category = r.get("category", "N/A")
        success = r.get("success", False)
        answer = r.get("answer", "")
        if category in ["weight", "volume", "count", "details_count"]:
            print(
                f"{category:<15} | {str(success):<8} | {answer.replace(chr(10), ' ')}"
            )
        else:
            preview = answer.split("\n")[0][:40] if answer else "No answer"
            print(f"{category:<15} | {str(success):<8} | {preview}...")
else:
    print(f"Error: {results_path} not found.")

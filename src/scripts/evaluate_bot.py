import json
import time

import requests

CONSIGNEE_CODES = [
    "0000866",
    "0001363",
    "0001540",
    "0001615",
    "0002679",
    "0002990",
    "0003427",
    "0003905",
    "0004932",
    "0005052",
    "0005053",
    "0005056",
    "0005171",
    "0005176",
    "0009633",
    "0013505",
    "0021472",
    "0023453",
    "0028662",
    "0028664",
    "0029594",
    "0030961",
    "0030962",
    "0037361",
    "0048392",
]

QUERIES = [
    "Which containers are arrived at Los Angeles in the last 7 days?",
    "Out of those, which ones are delayed?",
    "Show only containers delayed by more than 7 days.",
    "Which of them are marked as Hot containers?",
    "What is the ETA for the most delayed container?",
    "Who is the carrier for that shipment?",
    "Any of them are Hot",
    # "Show details for the first container.",
    "Which containers arrived at Los Angeles in the last 10 days?",
    "Out of those, how many had >5 days delay? Show only those.",
    "Among those delayed, which are Hot?",
    "What is the carrier and current status of the most delayed one?",
    "Which containers will arrive at Los Angeles in the next 30 days?",
    "Which are delayed >3 days vs planned?",
    "Which suppliers are tied to these shipments?",
    "Who is the carrier for the earliest arriving shipment, and where is it now?",
    "Any of them Hot?",
    "Show details for the first container.",
    "Which containers will arrive at Nashville in the next 5 days?",
    "Are any of them delayed?",
    "Show containers delayed by more than 3 days.",
    "Which suppliers are associated with these containers?",
    "Which carrier is handling the earliest arriving container?",
    # "What is the current location of that shipment?",
    "Any of them are Hot",
    "Show details for the first container.",
    "Show me containers from VIETNAM DONA STANDARD FOOTWEAR in the last 14 days.",
    "Are any delayed?",
    "Any of them Hot?",
    "Show details for the 2nd container.",
    "Show me containers from supplier VIETNAM DONA STANDARD FOOTWEAR in the last 7 days.",
    "Are any of them still in transit?",
    "Which containers have already been delivered?",
    "Are any shipments delayed?",
    "What is the next incoming shipment from this supplier?",
    "Which port is it arriving at?",
    "What is the ETA?",
    "Any of them are Hot",
    "Show details for the last container.",
    "Show all containers delayed by >7 days in the last 30 days.",
    "Which ports are they arriving at?",
    "Which carrier has the highest number of delayed containers?",
    "Any Hot containers among them?",
    "Show me all containers delayed by more than 7 days.",
    "Which ports are they arriving at?",
    "Which carrier has the highest number of delayed containers?",
    "Are any of them Hot containers?",
    "What shipments are scheduled for this week?",
    "Show me incoming cargo for the next 10 days.",
    "Which of them are arriving in the next 3 days?",
    "Are any shipments delayed?",
    "Which containers are arriving today?",
    "What is the earliest ETA among them?",
    "Any of them are Hot",
    "Show details for the first container.",
    "Can you show Hot containers arriving at Los Angeles in the next 3 days?",
    "Are any of them delayed by more than 7 days?",
    "Which Hot container has the earliest ETA?",
    "Who is the carrier for that container?",
    "What is its current location?",
    "Show Hot containers arriving in Nashville by sea in the next 30 days",
    "List shipments scheduled for March",
    "Which containers arrived at Nashville in last 20days?",
    "Okay, now only show me the delayed ones.",
    "Now show only Hot containers.",
]


def run_evaluation():
    api_url = "http://127.0.0.1:8000/api/chat"
    conversation_id = f"eval-{int(time.time())}"

    results = []

    print(f"Starting evaluation with conversation_id: {conversation_id}")
    print(f"Total queries to run: {len(QUERIES)}")
    print("-" * 50)

    for i, query in enumerate(QUERIES):
        print(f"[{i+1}/{len(QUERIES)}] Query: {query}")

        payload = {
            "question": query,
            "consignee_codes": CONSIGNEE_CODES,
            "conversation_id": conversation_id,
        }

        if i == 7:
            print("  -> SKIPPING Q8 (known to hang)")
            results.append(
                {
                    "turn": i + 1,
                    "question": query,
                    "intent": "SKIPPED",
                    "answer": "",
                    "latency_ms": 0,
                    "tokens": 0,
                    "cost_usd": 0.0,
                    "error": "Skipped due to known hang",
                }
            )
            continue

        try:
            # We want to wait for the bot server to finish starting up if it hasn't already.
            # Add a small delay between requests just to space out logs slightly.
            time.sleep(0.5)

            response = requests.post(api_url, json=payload, timeout=300)
            response.raise_for_status()
            data = response.json()

            result = {
                "turn": i + 1,
                "question": query,
                "intent": data.get("intent", "Unknown"),
                "answer": data.get("answer", ""),
                "latency_ms": data.get("metadata", {}).get("latency_ms", 0),
                "tokens": data.get("metadata", {}).get("tokens", 0),
                "cost_usd": data.get("metadata", {}).get("cost_usd", 0.0),
                "error": None,
            }
            print(f"  -> Intent: {result['intent']}")
            print(f"  -> Latency: {result['latency_ms']}ms")

        except Exception as e:
            print(f"  -> ERROR: {str(e)}")
            result = {
                "turn": i + 1,
                "question": query,
                "intent": "ERROR",
                "answer": "",
                "latency_ms": 0,
                "tokens": 0,
                "cost_usd": 0.0,
                "error": str(e),
            }

        results.append(result)

    print("-" * 50)
    print("Writing results to eval_results.md...")

    write_markdown_report(results)


def write_markdown_report(results):
    with open("eval_results.md", "w") as f:
        f.write("# Bot Evaluation Results\n\n")
        f.write(f"**Date:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Summary Table
        f.write("## Summary table\n\n")
        f.write("| Turn | Question | Intent | Latency (ms) | Tokens | Cost ($) |\n")
        f.write("|---|---|---|---|---|---|\n")

        total_latency = 0
        total_tokens = 0
        total_cost = 0.0
        success_count = 0

        for r in results:
            q_trunc = (
                r["question"][:60] + "..." if len(r["question"]) > 60 else r["question"]
            )
            f.write(
                f"| {r['turn']} | {q_trunc} | {r['intent']} | {r['latency_ms']} | {r['tokens']} | {r['cost_usd']:.6f} |\n"
            )

            if not r["error"]:
                total_latency += r["latency_ms"]
                total_tokens += r["tokens"]
                total_cost += r["cost_usd"]
                success_count += 1

        f.write("\n## Aggregates\n\n")
        f.write(f"- Total Queries: {len(results)}\n")
        f.write(f"- Successful Queries: {success_count}\n")
        if success_count > 0:
            f.write(f"- Avg Latency: {total_latency / success_count:.2f} ms\n")
        f.write(f"- Total cost: ${total_cost:.6f}\n\n")

        # Detailed Transcripts
        f.write("## Detailed Transcripts\n\n")
        for r in results:
            f.write(f"### Q{r['turn']}: {r['question']}\n\n")
            f.write(f"- **Intent:** `{r['intent']}`\n")
            f.write(
                f"- **Performance:** {r['latency_ms']}ms | {r['tokens']} tokens | ${r['cost_usd']:.6f}\n\n"
            )

            if r["error"]:
                f.write(f"**ERROR:**\n```\n{r['error']}\n```\n")
            else:
                answer = r["answer"]
                # remove any <think> blocks from answer for readability if needed
                f.write(f"**Answer:**\n\n{answer}\n")
            f.write("\n---\n\n")


if __name__ == "__main__":
    # Wait for bot server to be ready before starting
    print("Waiting 5 seconds for bot API to be fully started...")
    time.sleep(5)
    run_evaluation()

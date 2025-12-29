import json
import re

from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool
from shipment_qna_bot.tools.date_tools import (GET_TODAY_DATE_SCHEMA,
                                               get_today_date)


def test_planner_tool_flow():
    print("Testing planner tool flow simulation...")
    chat = AzureOpenAIChatTool()

    today_str = "2025-12-29"
    system_prompt = f"""
        You are a Search Planner for a logistics bot. Given a user question and extracted entities, generate an Azure Search Plan.
        
        Current Date: {today_str}

        Output JSON only:
        {{
            "query_text": "text for hybrid search",
            "extra_filter": "OData filter string or null",
            "skip": 0,
            "order_by": "optimal_eta_fd_date desc" or null,
            "reason": "short explanation"
        }}
    """

    q = "Show me shipments arriving today"
    user_content = f"Question: {q}\nExtracted Entities: {{}}"

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]

    # First pass
    print("1. First pass calling...")
    response = chat.chat_completion(
        messages, tools=[GET_TODAY_DATE_SCHEMA], tool_choice="auto"
    )

    if response.get("tool_calls"):
        print("2. Tool calls found.")
        tool_calls = response["tool_calls"]
        messages.append({"role": "assistant", "tool_calls": tool_calls})

        for tc in tool_calls:
            if tc.function.name == "get_today_date":
                print("3. Executing tool...")
                date_result = get_today_date()
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "name": "get_today_date",
                        "content": date_result,
                    }
                )

        print("4. Second pass calling...")
        # Simulating planner.py second pass exactly
        response = chat.chat_completion(messages, tool_choice="none")
        print(f"5. Final content:\n{response['content']}")

        res = response["content"]
        json_match = re.search(r"\{.*\}", res, re.DOTALL)
        if json_match:
            print("SUCCESS: JSON found!")
            print(json_match.group(0))
        else:
            print("FAILURE: No JSON found!")
    else:
        print("No tool call, using first response.")
        print(response["content"])


if __name__ == "__main__":
    test_planner_tool_flow()

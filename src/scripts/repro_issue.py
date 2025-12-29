import traceback

from shipment_qna_bot.tools.azure_openai_chat import AzureOpenAIChatTool
from shipment_qna_bot.tools.date_tools import (GET_TODAY_DATE_SCHEMA,
                                               get_today_date)


def test_tool_call_flow():
    print("Testing tool call flow...")
    chat = AzureOpenAIChatTool()

    messages = [
        {
            "role": "system",
            "content": "You are a helpful assistant. Use get_today_date to find the date.",
        },
        {"role": "user", "content": "What is the date today?"},
    ]

    try:
        # First pass
        print("1. First pass calling...")
        response = chat.chat_completion(
            messages, tools=[GET_TODAY_DATE_SCHEMA], tool_choice="auto"
        )
        print(f"1. Response type: {type(response)}")

        if response.get("tool_calls"):
            print("2. Tool calls found.")
            tool_calls = response["tool_calls"]
            print(f"Tool calls object: {tool_calls}")

            # This is what planner.py does:
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
            response = chat.chat_completion(messages)
            print("5. Success!")
            print(f"Final content: {response['content']}")
        else:
            print("No tool calls triggered.")

    except Exception:
        traceback.print_exc()


if __name__ == "__main__":
    test_tool_call_flow()

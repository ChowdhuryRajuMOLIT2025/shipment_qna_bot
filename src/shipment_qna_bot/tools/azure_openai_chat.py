import os
from typing import Any, Dict, List, Optional

from dotenv import find_dotenv, load_dotenv
from openai import AzureOpenAI

# Load environment variables
load_dotenv(find_dotenv(), override=True)


class AzureOpenAIChatTool:
    def __init__(self):
        self.api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self.api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")

        # Support multiple naming conventions
        self.azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT") or os.getenv(
            "ENDPOINT_URL"
        )
        self.deployment_name = (
            os.getenv("AZURE_OPENAI_DEPLOYMENT")
            or os.getenv("DEPLOYMENT_NAME")
            or "gpt-4o"
        )

        if not self.api_key or not self.azure_endpoint or not self.deployment_name:
            # Debug info to help user if it still fails
            missing = []
            if not self.api_key:
                missing.append("AZURE_OPENAI_API_KEY")
            if not self.azure_endpoint:
                missing.append("AZURE_OPENAI_ENDPOINT/ENDPOINT_URL")
            if not self.deployment_name:
                missing.append("AZURE_OPENAI_DEPLOYMENT/DEPLOYMENT_NAME")

            raise ValueError(
                f"Missing Azure OpenAI credentials: {', '.join(missing)}. "
                "Please check your .env file."
            )

        self.client = AzureOpenAI(
            api_key=self.api_key,
            api_version=self.api_version,
            azure_endpoint=self.azure_endpoint,
        )

    def chat_completion(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
        max_tokens: int = 800,
        tools: Optional[List[Dict[str, Any]]] = None,
        tool_choice: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generates a chat completion using the Azure OpenAI client.
        Returns a dict with 'content', 'usage', and optionally 'tool_calls'.
        """
        try:
            kwargs = {
                "model": self.deployment_name,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
            if tools:
                kwargs["tools"] = tools
            if tool_choice:
                kwargs["tool_choice"] = tool_choice

            response = self.client.chat.completions.create(**kwargs)
            choice = response.choices[0]
            message = choice.message

            result = {
                "content": message.content or "",
                "usage": {
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens,
                },
            }

            if message.tool_calls:
                result["tool_calls"] = message.tool_calls
                # Store the tool_call_id of the first call for convenience if needed,
                # though usually we iterate over tool_calls.
                result["tool_call_id"] = message.tool_calls[0].id

            return result
        except Exception as e:
            raise RuntimeError(f"Azure OpenAI Chat Completion failed: {e}")

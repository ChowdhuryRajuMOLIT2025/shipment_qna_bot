# src/shipment_qna_bot/tools/azure_openai_chat.py

import os
from typing import Any, Dict, List, Optional

# Load environment variables
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=True)

from openai import AzureOpenAI

from shipment_qna_bot.utils.runtime import is_test_mode


class AzureOpenAIChatTool:
    def __init__(self):
        self._test_mode = is_test_mode()
        if self._test_mode:
            self.api_key = "test"
            self.api_version = "test"
            self.azure_endpoint = "test"
            self.deployment_name = "test"
            self.client = None
            self.timeout_s = None
            self.max_retries = 0
            return

        self.api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self.api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
        self.timeout_s = float(os.getenv("AZURE_OPENAI_TIMEOUT", "60"))
        self.max_retries = int(os.getenv("AZURE_OPENAI_MAX_RETRIES", "2"))

        self.azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT") or os.getenv(
            "ENDPOINT_URL"
        )
        self.deployment_name = (
            os.getenv("AZURE_OPENAI_DEPLOYMENT")
            or os.getenv("DEPLOYMENT_NAME")
            or "gpt-4o"
        )

        if not self.api_key or not self.azure_endpoint or not self.deployment_name:
            missing = []
            if not self.api_key:
                missing.append("AZURE_OPENAI_API_KEY")  # type: ignore
            if not self.azure_endpoint:
                missing.append("AZURE_OPENAI_ENDPOINT/ENDPOINT_URL")  # type: ignore
            if not self.deployment_name:
                missing.append("AZURE_OPENAI_DEPLOYMENT/DEPLOYMENT_NAME")  # type: ignore

            raise ValueError(
                f"Missing Azure OpenAI credentials: {', '.join(missing)}. "  # type: ignore
                "Please check your .env file."
            )

        self.client = AzureOpenAI(
            api_key=self.api_key,
            api_version=self.api_version,
            azure_endpoint=self.azure_endpoint,
            timeout=self.timeout_s,
            max_retries=self.max_retries,
        )

    def chat_completion(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.01,
        max_tokens: int = 800,
        tools: Optional[List[Dict[str, Any]]] = None,
        tool_choice: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generates a chat completion using the Azure OpenAI client.
        Returns a dict with 'content', 'usage', and optionally 'tool_calls'.
        """
        if self._test_mode:
            return {
                "content": "",
                "usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                },
            }

        try:
            kwargs = {  # type: ignore
                "model": self.deployment_name,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
            if self.timeout_s:
                kwargs["timeout"] = self.timeout_s
            if tools:
                kwargs["tools"] = tools
            if tool_choice:
                kwargs["tool_choice"] = tool_choice

            response = self.client.chat.completions.create(**kwargs)  # type: ignore
            choice = response.choices[0]  # type: ignore
            message = choice.message  # type: ignore

            result = {  # type: ignore
                "content": message.content or "",  # type: ignore
                "usage": {
                    "prompt_tokens": response.usage.prompt_tokens,  # type: ignore
                    "completion_tokens": response.usage.completion_tokens,  # type: ignore
                    "total_tokens": response.usage.total_tokens,  # type: ignore
                },
            }

            if message.tool_calls:  # type: ignore
                result["tool_calls"] = message.tool_calls  # type: ignore
                result["tool_call_id"] = message.tool_calls[0].id  # type: ignore

            return result  # type: ignore
        except Exception as e:
            raise RuntimeError(f"Azure OpenAI Chat Completion failed: {e}")

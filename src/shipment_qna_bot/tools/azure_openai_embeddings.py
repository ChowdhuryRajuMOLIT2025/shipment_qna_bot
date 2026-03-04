# src/shipment_qna_bot/tools/azure_openai_embeddings.py

from __future__ import annotations

import os
import time

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=True)

from typing import List

from openai import AzureOpenAI

from shipment_qna_bot.utils.runtime import is_test_mode


class AzureOpenAIEmbeddingsClient:
    def __init__(self) -> None:
        self._test_mode = is_test_mode()
        if self._test_mode:
            self._deployment = "test"
            self._client = None
            self._timeout_s = None
            return

        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        api_key = os.getenv("AZURE_OPENAI_API_KEY")
        api_version = os.getenv("AZURE_OPENAI_API_VERSION")
        deployment = os.getenv("AZURE_OPENAI_EMBED_DEPLOYMENT")

        if not endpoint or not api_key or not api_version or not deployment:
            raise RuntimeError(
                "Missing Azure OpenAI env vars. "
                "Need AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, AZURE_OPENAI_EMBEDDING_DEPLOYMENT "
                "(and optionally OPENAI_API_VERSION)."
            )

        self._deployment = deployment
        self._timeout_s = float(
            os.getenv(
                "AZURE_OPENAI_EMBED_TIMEOUT", os.getenv("AZURE_OPENAI_TIMEOUT", "30")
            )
        )
        self._client = AzureOpenAI(
            azure_endpoint=endpoint,
            # azure_deployment=deployment,
            api_key=api_key,
            api_version=api_version,
            timeout=self._timeout_s,
            max_retries=int(os.getenv("AZURE_OPENAI_EMBED_MAX_RETRIES", "3")),
        )

    def embed_query(self, text: str) -> List[float]:
        if self._test_mode:
            return []
        text = (text or "").strip()
        if not text:
            return []
        max_retries = int(os.getenv("AZURE_OPENAI_EMBED_MAX_RETRIES", "3"))
        base_delay = float(os.getenv("AZURE_OPENAI_EMBED_RETRY_DELAY", "1.0"))
        last_error: Exception | None = None
        transient_markers = [
            "ratelimit",
            "rate limit",
            "429",
            "timeout",
            "timed out",
            "service unavailable",
            "temporarily unavailable",
            "500",
            "502",
            "503",
            "504",
            "connection reset",
            "connection aborted",
            "econnreset",
            "gateway timeout",
        ]

        for attempt in range(1, max_retries + 1):
            try:
                text_input = str(text)
                resp = self._client.embeddings.create(  # type: ignore
                    model=self._deployment,
                    input=text_input,
                    timeout=self._timeout_s,
                )
                return list(resp.data[0].embedding)
            except Exception as e:
                last_error = e
                msg = str(e)
                msg_lower = msg.lower()
                if any(marker in msg_lower for marker in transient_markers):
                    time.sleep(base_delay * attempt)
                    continue
                break

        raise RuntimeError(
            f"Azure OpenAI Embedding failed: {last_error}"
        ) from last_error

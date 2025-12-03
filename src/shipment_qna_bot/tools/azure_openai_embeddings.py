# src/shipment_qna_bot/tools/azure_openai_embeddings.py

from __future__ import annotations

import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=True)

from typing import List

from openai import AzureOpenAI

# from azure.ai.openai import AzureOpenAI
# from azure.ai.openai.schemas import Embeddings


class AzureOpenAIEmbeddingsClient:
    def __init__(self) -> None:
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
        self._client = AzureOpenAI(
            azure_endpoint=endpoint,
            # azure_deployment=deployment,
            api_key=api_key,
            api_version=api_version,
        )

    def embed_query(self, text: str) -> List[float]:
        text = (text or "").strip()
        if not text:
            return []
        resp = self._client.embeddings.create(
            model=self._deployment,
            input=text,
        )
        return list(resp.data[0].embedding)

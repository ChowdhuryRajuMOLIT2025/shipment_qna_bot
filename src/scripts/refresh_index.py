# src/scripts/refresh_index.py

import os
import sys

# Ensure src is in python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from scripts.ingest_all import ingest_all
from shipment_qna_bot.tools.azure_ai_search import AzureAISearchTool


def refresh_index():
    """
    Clears the search index and re-ingests all data from the data directory.
    """
    print("--- Refreshing Index ---")

    tool = AzureAISearchTool()

    print("Step 1: Clearing existing data from index...")
    tool.clear_index()

    print("\nStep 2: Starting fresh ingestion from data directory...")
    # This will process files in 'data/' and move them to 'data/processed/'
    ingest_all()

    print("\n--- Refresh Complete ---")


if __name__ == "__main__":
    refresh_index()

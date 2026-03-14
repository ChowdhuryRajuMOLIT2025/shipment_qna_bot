import os
import sys

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=True)

from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
from azure.search.documents.indexes import SearchIndexClient


def inspect_index():
    endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
    api_key = os.getenv("AZURE_SEARCH_API_KEY")
    index_name = os.getenv("AZURE_SEARCH_INDEX_NAME")

    if not endpoint or not index_name:
        print("Missing env vars")
        return

    cred = AzureKeyCredential(api_key) if api_key else DefaultAzureCredential()
    client = SearchIndexClient(endpoint=endpoint, credential=cred)

    try:
        index = client.get_index(index_name)
        print(f"Index: {index.name}")
        print("Fields:")

        def print_fields(fields, indent=""):
            for field in fields:
                attr = []
                if getattr(field, "filterable", False):
                    attr.append("Filterable")
                if getattr(field, "searchable", False):
                    attr.append("Searchable")
                print(f"{indent}- {field.name} (Type: {field.type}) [{' '.join(attr)}]")
                if field.type == "Edm.ComplexType" or (
                    hasattr(field, "fields") and field.fields
                ):
                    print_fields(field.fields, indent + "  ")

        print_fields(index.fields)

        print("\nVector Search Profiles:")
        if index.vector_search:
            for p in index.vector_search.profiles:
                print(f"- {p.name} (Alg: {p.algorithm_configuration_name})")

        print("\nScoring Profiles:")
        if index.scoring_profiles:
            for s in index.scoring_profiles:
                print(f"- {s.name}")

        print("\nSemantic Search Configurations:")
        if index.semantic_search:
            for c in index.semantic_search.configurations:
                print(f"- {c.name}")
    except Exception as e:
        print(f"Error getting index: {e}")


if __name__ == "__main__":
    inspect_index()

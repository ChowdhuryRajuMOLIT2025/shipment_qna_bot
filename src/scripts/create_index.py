# $env:PYTHONPATH='c:\Users\CHOWDHURYRaju\Desktop\shipment_qna_bot\src'; python src/scripts/create_index.py

import os

from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    HnswAlgorithmConfiguration,
    ScoringProfile,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SemanticConfiguration,
    SemanticField,
    SemanticPrioritizedFields,
    SemanticSearch,
    SimpleField,
    TextWeights,
    VectorSearch,
    VectorSearchProfile,
)
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=True)


def create_index():
    endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
    api_key = os.getenv("AZURE_SEARCH_API_KEY")
    index_name = "shipment-idx"

    if not endpoint:
        print("Missing AZURE_SEARCH_ENDPOINT")
        return

    cred = AzureKeyCredential(api_key) if api_key else DefaultAzureCredential()
    client = SearchIndexClient(endpoint=endpoint, credential=cred)

    # Define the fields
    fields = [
        SimpleField(
            name="document_id",
            type=SearchFieldDataType.String,
            key=True,
            filterable=True,
        ),
        SearchField(name="content", type=SearchFieldDataType.String, searchable=True),
        # Vector field for hybrid search
        SearchField(
            name="content_vector",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            searchable=True,
            vector_search_dimensions=1536,  # Default for text-embedding-ada-002 and text-embedding-3-small
            vector_search_profile_name="my-vector-profile",
        ),
        # RLS Field: Critical for filtering
        SearchField(
            name="consignee_code_ids",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            filterable=True,
        ),
        # Lookup Fields
        SearchField(
            name="container_number",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="po_numbers",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="obl_nos",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="booking_numbers",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            searchable=True,
            filterable=True,
        ),
        # User-filterable shipment attributes
        SearchField(
            name="container_type",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="destination_service",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="load_port",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="final_load_port",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="discharge_port",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="last_cy_location",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="place_of_receipt",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="place_of_delivery",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="final_destination",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="first_vessel_name",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="final_carrier_name",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="final_vessel_name",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="shipment_status",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="true_carrier_scac_name",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SimpleField(
            name="hot_container_flag",
            type=SearchFieldDataType.Boolean,
            filterable=True,
            sortable=True,
        ),
        # Date Fields
        SearchField(
            name="etd_lp_date",
            type=SearchFieldDataType.DateTimeOffset,
            filterable=True,
            sortable=True,
        ),
        SearchField(
            name="etd_flp_date",
            type=SearchFieldDataType.DateTimeOffset,
            filterable=True,
            sortable=True,
        ),
        SearchField(
            name="eta_dp_date",
            type=SearchFieldDataType.DateTimeOffset,
            filterable=True,
            sortable=True,
        ),
        SearchField(
            name="eta_fd_date",
            type=SearchFieldDataType.DateTimeOffset,
            filterable=True,
            sortable=True,
        ),
        SearchField(
            name="best_eta_dp_date",
            type=SearchFieldDataType.DateTimeOffset,
            filterable=True,
            sortable=True,
        ),
        SearchField(
            name="best_eta_fd_date",
            type=SearchFieldDataType.DateTimeOffset,
            filterable=True,
            sortable=True,
        ),
        SearchField(
            name="atd_lp_date",
            type=SearchFieldDataType.DateTimeOffset,
            filterable=True,
            sortable=True,
        ),
        SearchField(
            name="ata_flp_date",
            type=SearchFieldDataType.DateTimeOffset,
            filterable=True,
            sortable=True,
        ),
        SearchField(
            name="atd_flp_date",
            type=SearchFieldDataType.DateTimeOffset,
            filterable=True,
            sortable=True,
        ),
        SearchField(
            name="ata_dp_date",
            type=SearchFieldDataType.DateTimeOffset,
            filterable=True,
            sortable=True,
        ),
        SearchField(
            name="supplier_vendor_name",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="manufacturer_name",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="ship_to_party_name",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="job_type",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="mcs_hbl",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        SearchField(
            name="transport_mode",
            type=SearchFieldDataType.String,
            searchable=True,
            filterable=True,
        ),
        # Additional metadata as blob
        SearchField(
            name="metadata_json",
            type=SearchFieldDataType.String,
            searchable=False,
            filterable=False,
        ),
    ]

    # Configure vector search
    vector_search = VectorSearch(
        algorithms=[
            HnswAlgorithmConfiguration(name="my-hnsw"),
        ],
        profiles=[
            VectorSearchProfile(
                name="my-vector-profile", algorithm_configuration_name="my-hnsw"
            ),
        ],
    )

    # Configure scoring profiles
    scoring_profiles = [
        ScoringProfile(
            name="logistics-score",
            text_weights=TextWeights(weights={"content": 2.5}),
        )
    ]

    # Configure semantic search
    semantic_search = SemanticSearch(
        configurations=[
            SemanticConfiguration(
                name="default",
                prioritized_fields=SemanticPrioritizedFields(
                    content_fields=[SemanticField(field_name="content")],
                ),
            )
        ]
    )

    index = SearchIndex(
        name=index_name,
        fields=fields,
        vector_search=vector_search,
        scoring_profiles=scoring_profiles,
        semantic_search=semantic_search,
    )

    print(f"Deleting index '{index_name}' if it exists...")
    try:
        client.delete_index(index_name)
        print(f"Index '{index_name}' deleted.")
    except Exception as e:
        print(f"Index '{index_name}' not found or could not be deleted: {e}")

    print(f"Creating index '{index_name}'...")
    try:
        result = client.create_index(index)
        print(f"Index '{result.name}' created successfully.")
    except Exception as e:
        print(f"Failed to create index: {e}")


if __name__ == "__main__":
    create_index()

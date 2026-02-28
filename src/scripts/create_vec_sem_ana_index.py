# call sign: $ python src\scripts\create_vec_sem_ana_index.py

import os

from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (HnswAlgorithmConfiguration,
                                                   ScoringProfile, SearchField,
                                                   SearchFieldDataType,
                                                   SearchIndex,
                                                   SemanticConfiguration,
                                                   SemanticField,
                                                   SemanticPrioritizedFields,
                                                   SemanticSearch, SimpleField,
                                                   TextWeights, VectorSearch,
                                                   VectorSearchProfile)
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=True)


INDEX_NAME = "vec-sem-ana-idx"
VECTOR_PROFILE_NAME = "vec-sem-ana-profile"
VECTOR_ALGO_NAME = "vec-sem-ana-hnsw"
SCORING_PROFILE_NAME = "vec-sem-ana-score"
SEMANTIC_CONFIG_NAME = "vec-sem-ana-semantic"
VECTOR_DIMENSIONS = 1536


def _text_field(
    name: str,
    *,
    searchable: bool = True,
    filterable: bool = True,
    sortable: bool = False,
    facetable: bool = False,
) -> SearchField:
    return SearchField(
        name=name,
        type=SearchFieldDataType.String,
        searchable=searchable,
        filterable=filterable,
        sortable=sortable,
        facetable=facetable,
    )


def _text_collection_field(
    name: str,
    *,
    searchable: bool = True,
    filterable: bool = True,
) -> SearchField:
    return SearchField(
        name=name,
        type=SearchFieldDataType.Collection(SearchFieldDataType.String),
        searchable=searchable,
        filterable=filterable,
    )


def _datetime_field(name: str) -> SearchField:
    return SearchField(
        name=name,
        type=SearchFieldDataType.DateTimeOffset,
        filterable=True,
        sortable=True,
    )


def _double_field(name: str) -> SimpleField:
    return SimpleField(
        name=name,
        type=SearchFieldDataType.Double,
        filterable=True,
        sortable=True,
        facetable=True,
    )


def _int_field(name: str) -> SimpleField:
    return SimpleField(
        name=name,
        type=SearchFieldDataType.Int64,
        filterable=True,
        sortable=True,
        facetable=True,
    )


def _bool_field(name: str) -> SimpleField:
    return SimpleField(
        name=name,
        type=SearchFieldDataType.Boolean,
        filterable=True,
        sortable=True,
        facetable=True,
    )


def build_index() -> SearchIndex:
    must_have_fields = [
        # Core identity and retrieval
        SimpleField(
            name="document_id",
            type=SearchFieldDataType.String,
            key=True,
            filterable=True,
        ),
        _text_field("content", searchable=True, filterable=False),
        SearchField(
            name="content_vector",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            searchable=True,
            vector_search_dimensions=VECTOR_DIMENSIONS,
            vector_search_profile_name=VECTOR_PROFILE_NAME,
        ),
        _text_collection_field("consignee_code_ids", searchable=False, filterable=True),
        # Existing shipment-idx business fields
        _text_field("container_number"),
        _text_collection_field("po_numbers"),
        _text_collection_field("obl_nos"),
        _text_collection_field("booking_numbers"),
        _text_field("container_type", facetable=True),
        _text_field("destination_service", facetable=True),
        _text_field("load_port"),
        _text_field("final_load_port"),
        _text_field("discharge_port"),
        _text_field("last_cy_location"),
        _text_field("place_of_receipt"),
        _text_field("place_of_delivery"),
        _text_field("final_destination"),
        _text_field("first_vessel_name"),
        _text_field("final_carrier_name"),
        _text_field("final_vessel_name"),
        _text_field("shipment_status", facetable=True),
        _text_field("true_carrier_scac_name"),
        _bool_field("hot_container_flag"),
        _datetime_field("etd_lp_date"),
        _datetime_field("etd_flp_date"),
        _datetime_field("eta_dp_date"),
        _datetime_field("eta_fd_date"),
        _datetime_field("best_eta_dp_date"),
        _datetime_field("best_eta_fd_date"),
        _datetime_field("atd_lp_date"),
        _datetime_field("ata_flp_date"),
        _datetime_field("atd_flp_date"),
        _datetime_field("ata_dp_date"),
        _text_field("supplier_vendor_name"),
        _text_field("manufacturer_name"),
        _text_field("ship_to_party_name"),
        _text_field("job_type", facetable=True),
        _text_field("mcs_hbl"),
        _text_field("transport_mode", facetable=True),
        # Previously missing high-value operational fields
        _datetime_field("derived_ata_dp_date"),
        _datetime_field("optimal_eta_fd_date"),
        _datetime_field("delivery_to_consignee_date"),
        _datetime_field("empty_container_return_date"),
        _datetime_field("out_gate_from_dp_date"),
        _datetime_field("equipment_arrived_at_last_cy_date"),
        _datetime_field("out_gate_at_last_cy_date"),
        _datetime_field("vehicle_arrival_date"),
        _datetime_field("carrier_vehicle_unload_date"),
        _text_field("delayed_dp", facetable=True),
        _int_field("dp_delayed_dur"),
        _text_field("delayed_fd", facetable=True),
        _int_field("fd_delayed_dur"),
        _double_field("cargo_weight_kg"),
        _double_field("cargo_measure_cubic_meter"),
        _int_field("cargo_count"),
        _int_field("cargo_detail_count"),
        _text_field("consignee_name"),
        _text_field("seal_number"),
        _text_collection_field("fcr_numbers"),
        # Keep full metadata for back-compat and deep answer grounding
        _text_field("metadata_json", searchable=False, filterable=False),
    ]

    nice_to_have_fields = [
        _text_field("job_no"),
        _text_field("final_voyage_code"),
        _text_field("booking_approval_status", facetable=True),
        _text_field("service_contract_number"),
        _text_field("carrier_vehicle_load_lcn"),
        _text_field("vehicle_departure_lcn"),
        _text_field("vehicle_arrival_lcn"),
        _text_field("carrier_vehicle_unload_lcn"),
        _text_field("out_gate_from_dp_lcn"),
        _text_field("equipment_arrived_at_last_cy_lcn"),
        _text_field("out_gate_at_last_cy_lcn"),
        _text_field("delivery_to_consignee_lcn"),
        _text_field("empty_container_return_lcn"),
        _text_field("rail_load_dp_lcn"),
        _text_field("rail_departure_dp_lcn"),
        _text_field("rail_arrival_destination_lcn"),
        _text_field("in_gate_lcn"),
        _text_field("empty_container_dispatch_lcn"),
        _datetime_field("carrier_vehicle_load_date"),
        _datetime_field("vehicle_departure_date"),
        _datetime_field("rail_load_dp_date"),
        _datetime_field("rail_departure_dp_date"),
        _datetime_field("rail_arrival_destination_date"),
        # Source column `in-dc_date` must be aliased because Azure field names
        # cannot contain hyphens.
        _datetime_field("in_dc_date"),
        _datetime_field("in_gate_date"),
        _datetime_field("empty_container_dispatch_date"),
        _double_field("detention_free_days"),
        _double_field("demurrage_free_days"),
        _double_field("co2_tank_on_wheel"),
        _double_field("co2_well_to_wheel"),
        _text_field("cargo_um", facetable=True),
        _text_field("detail_cargo_um", facetable=True),
        # Source column `856_filing_status` must be aliased because Azure field
        # names must start with a letter.
        _text_field("f856_filing_status", facetable=True),
        _text_field("get_isf_submission_date"),
        _text_collection_field("cargo_ready_date"),
        _text_collection_field("cargo_receiveds_date"),
        _text_field("critical_dates_summary", searchable=True, filterable=False),
        _text_field("delay_reason_summary", searchable=True, filterable=False),
        _text_collection_field("workflow_gap_flags", searchable=True, filterable=True),
        _text_field("milestones", searchable=True, filterable=False),
        _text_field("vessel_summary", searchable=True, filterable=False),
        _text_field("carrier_summary", searchable=True, filterable=False),
        _text_field("port_route_summary", searchable=True, filterable=False),
        _text_field("source_group", facetable=True),
        _text_field("source_month_tag", facetable=True),
    ]

    fields = must_have_fields + nice_to_have_fields

    vector_search = VectorSearch(
        algorithms=[HnswAlgorithmConfiguration(name=VECTOR_ALGO_NAME)],
        profiles=[
            VectorSearchProfile(
                name=VECTOR_PROFILE_NAME,
                algorithm_configuration_name=VECTOR_ALGO_NAME,
            )
        ],
    )

    scoring_profiles = [
        ScoringProfile(
            name=SCORING_PROFILE_NAME,
            text_weights=TextWeights(
                weights={
                    "content": 3.0,
                    "critical_dates_summary": 2.0,
                    "delay_reason_summary": 1.8,
                    "milestones": 1.6,
                    "port_route_summary": 1.3,
                    "carrier_summary": 1.2,
                    "vessel_summary": 1.2,
                }
            ),
        )
    ]

    semantic_search = SemanticSearch(
        configurations=[
            SemanticConfiguration(
                name=SEMANTIC_CONFIG_NAME,
                prioritized_fields=SemanticPrioritizedFields(
                    content_fields=[
                        SemanticField(field_name="content"),
                        SemanticField(field_name="critical_dates_summary"),
                        SemanticField(field_name="delay_reason_summary"),
                        SemanticField(field_name="milestones"),
                        SemanticField(field_name="port_route_summary"),
                        SemanticField(field_name="carrier_summary"),
                        SemanticField(field_name="vessel_summary"),
                    ],
                    keywords_fields=[
                        SemanticField(field_name="container_number"),
                        SemanticField(field_name="shipment_status"),
                        SemanticField(field_name="discharge_port"),
                        SemanticField(field_name="final_destination"),
                        SemanticField(field_name="seal_number"),
                        SemanticField(field_name="consignee_name"),
                    ],
                ),
            )
        ]
    )

    return SearchIndex(
        name=INDEX_NAME,
        fields=fields,
        vector_search=vector_search,
        scoring_profiles=scoring_profiles,
        semantic_search=semantic_search,
    )


def create_index() -> None:
    endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
    api_key = os.getenv("AZURE_SEARCH_API_KEY")

    if not endpoint:
        print("Missing AZURE_SEARCH_ENDPOINT")
        return

    cred = AzureKeyCredential(api_key) if api_key else DefaultAzureCredential()
    client = SearchIndexClient(endpoint=endpoint, credential=cred)
    index = build_index()

    print(
        f"Creating or updating index '{INDEX_NAME}' without touching 'shipment-idx'..."
    )
    try:
        result = client.create_or_update_index(index)
        print(f"Index '{result.name}' created/updated successfully.")
    except Exception as exc:
        print(f"Failed to create/update index '{INDEX_NAME}': {exc}")


if __name__ == "__main__":
    create_index()

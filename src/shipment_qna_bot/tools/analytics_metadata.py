"""
Centralized metadata for shipment analytics.
This file defines the schema, types, and descriptions used for both 
data type casting and LLM dynamic prompt generation.
"""

# Searchable columns with their technical and human-friendly attributes
ANALYTICS_METADATA = {
    "container_number": {
        "desc": "The unique 11-character container identifier.",
        "type": "string",
    },
    "container_type": {"desc": "Definition for container type.", "type": "categorical"},
    "destination_service": {
        "desc": "Definition for destination service.",
        "type": "categorical",
    },
    # "consignee_raw": {
    #     "desc": "Definition for consignee raw.",
    #     "type": "string"
    # },
    "po_numbers": {"desc": "Customer Purchase Order numbers.", "type": "list"},
    "booking_numbers": {
        "desc": "Internal shipment booking identifiers.",
        "type": "list",
    },
    "fcr_numbers": {"desc": "Definition for fcr numbers.", "type": "list"},
    "obl_nos": {"desc": "Original Bill of Lading numbers (OBL).", "type": "list"},
    "load_port": {
        "desc": "The port where the cargo was initially loaded.",
        "type": "string",
    },
    "final_load_port": {"desc": "Definition for final load port.", "type": "string"},
    "discharge_port": {
        "desc": "The port where the cargo is unloaded from the final vessel.",
        "type": "string",
    },
    "last_cy_location": {"desc": "Definition for last cy location.", "type": "string"},
    "place_of_receipt": {"desc": "Definition for place of receipt.", "type": "string"},
    "place_of_delivery": {
        "desc": "Definition for place of delivery.",
        "type": "string",
    },
    "final_destination": {
        "desc": "The final point of delivery (often a city or warehouse).",
        "type": "string",
    },
    "first_vessel_name": {
        "desc": "The name of the vessel for the first leg of ocean transport.",
        "type": "string",
    },
    "final_carrier_name": {
        "desc": "The name of the carrier handling the final leg.",
        "type": "string",
    },
    "final_vessel_name": {
        "desc": "The name of the vessel for the final ocean leg.",
        "type": "string",
    },
    "true_carrier_scac_name": {
        "desc": "The primary carrier shipping line name.",
        "type": "string",
    },
    "etd_lp_date": {
        "desc": "Estimated Time of Departure from Load Port.",
        "type": "datetime",
    },
    "etd_flp_date": {"desc": "Definition for etd flp date.", "type": "datetime"},
    "eta_dp_date": {
        "desc": "Estimated Time of Arrival at Discharge Port.",
        "type": "datetime",
    },
    "eta_fd_date": {
        "desc": "Estimated Time of Arrival at Final Destination.",
        "type": "datetime",
    },
    # "revised_eta_date": {
    #     "desc": "Definition for revised eta date.",
    #     "type": "datetime"
    # },
    # "predictive_eta_date": {
    #     "desc": "Definition for predictive eta date.",
    #     "type": "datetime"
    # },
    # "atd_lp_date": {
    #     "desc": "Actual Time of Departure from Load Port.",
    #     "type": "datetime"
    # },
    # "ata_flp_date": {
    #     "desc": "Definition for ata flp date.",
    #     "type": "datetime"
    # },
    "atd_flp_date": {"desc": "Definition for atd flp date.", "type": "datetime"},
    # "ata_dp_date": {
    #     "desc": "Actual Time of Arrival at Discharge Port.",
    #     "type": "datetime"
    # },
    # "derived_ata_dp_date": {
    #     "desc": "Definition for derived ata dp date.",
    #     "type": "datetime"
    # },
    # "revised_eta_fd_date": {
    #     "desc": "Definition for revised eta fd date.",
    #     "type": "datetime"
    # },
    # "predictive_eta_fd_date": {
    #     "desc": "Definition for predictive eta fd date.",
    #     "type": "datetime"
    # },
    "cargo_receiveds_date": {
        "desc": "Definition for cargo receiveds date.",
        "type": "string",
    },
    "detention_free_days": {
        "desc": "Definition for detention free days.",
        "type": "numeric",
    },
    "demurrage_free_days": {
        "desc": "Definition for demurrage free days.",
        "type": "numeric",
    },
    "hot_container_flag": {
        "desc": "Flag indicating if the container is hot.",
        "type": "boolean",
    },
    "supplier_vendor_name": {
        "desc": "The shipper or supplier of the goods.",
        "type": "string",
    },
    "manufacturer_name": {
        "desc": "The company that manufactured the goods.",
        "type": "string",
    },
    "ship_to_party_name": {
        "desc": "Definition for ship to party name.",
        "type": "string",
    },
    "booking_approval_status": {
        "desc": "Definition for booking approval status.",
        "type": "string",
    },
    "service_contract_number": {
        "desc": "Definition for service contract number.",
        "type": "string",
    },
    "carrier_vehicle_load_date": {
        "desc": "Definition for carrier vehicle load date.",
        "type": "datetime",
    },
    "carrier_vehicle_load_lcn": {
        "desc": "Definition for carrier vehicle load lcn.",
        "type": "string",
    },
    "vehicle_departure_date": {
        "desc": "Definition for vehicle departure date.",
        "type": "datetime",
    },
    "vehicle_departure_lcn": {
        "desc": "Definition for vehicle departure lcn.",
        "type": "string",
    },
    "vehicle_arrival_date": {
        "desc": "Definition for vehicle arrival date.",
        "type": "datetime",
    },
    "vehicle_arrival_lcn": {
        "desc": "Definition for vehicle arrival lcn.",
        "type": "string",
    },
    "carrier_vehicle_unload_date": {
        "desc": "Definition for carrier vehicle unload date.",
        "type": "datetime",
    },
    "carrier_vehicle_unload_lcn": {
        "desc": "Definition for carrier vehicle unload lcn.",
        "type": "string",
    },
    "out_gate_from_dp_date": {
        "desc": "Definition for out gate from dp date.",
        "type": "datetime",
    },
    "out_gate_from_dp_lcn": {
        "desc": "Definition for out gate from dp lcn.",
        "type": "string",
    },
    "equipment_arrived_at_last_cy_date": {
        "desc": "Definition for equipment arrived at last cy date.",
        "type": "datetime",
    },
    "equipment_arrived_at_last_cy_lcn": {
        "desc": "Definition for equipment arrived at last cy lcn.",
        "type": "string",
    },
    "out_gate_at_last_cy_date": {
        "desc": "Definition for out gate at last cy date.",
        "type": "datetime",
    },
    "out_gate_at_last_cy_lcn": {
        "desc": "Definition for out gate at last cy lcn.",
        "type": "string",
    },
    "delivery_to_consignee_date": {
        "desc": "Definition for delivery to consignee date.",
        "type": "datetime",
    },
    "delivery_to_consignee_lcn": {
        "desc": "Definition for delivery to consignee lcn.",
        "type": "string",
    },
    "empty_container_return_date": {
        "desc": "Definition for empty container return date.",
        "type": "datetime",
    },
    "empty_container_return_lcn": {
        "desc": "Definition for empty container return lcn.",
        "type": "string",
    },
    "co2_tank_on_wheel": {
        "desc": "Definition for co2 tank on wheel.",
        "type": "numeric",
    },
    "co2_well_to_wheel": {
        "desc": "Definition for co2 well to wheel.",
        "type": "numeric",
    },
    "job_type": {"desc": "Definition for job type.", "type": "categorical"},
    "mcs_hbl": {"desc": "Definition for mcs hbl.", "type": "string"},
    "transport_mode": {"desc": "Definition for transport mode.", "type": "categorical"},
    "rail_load_dp_date": {
        "desc": "Definition for rail load dp date.",
        "type": "datetime",
    },
    "rail_load_dp_lcn": {"desc": "Definition for rail load dp lcn.", "type": "string"},
    "rail_departure_dp_date": {
        "desc": "Definition for rail departure dp date.",
        "type": "datetime",
    },
    "rail_departure_dp_lcn": {
        "desc": "Definition for rail departure dp lcn.",
        "type": "string",
    },
    "rail_arrival_destination_date": {
        "desc": "Definition for rail arrival destination date.",
        "type": "datetime",
    },
    "rail_arrival_destination_lcn": {
        "desc": "Definition for rail arrival destination lcn.",
        "type": "string",
    },
    "cargo_ready_date": {"desc": "Definition for cargo ready date.", "type": "string"},
    "in-dc_date": {"desc": "Definition for in-dc date.", "type": "datetime"},
    "cargo_weight_kg": {
        "desc": "Total weight of the cargo in kilograms.",
        "type": "numeric",
    },
    "cargo_measure_cubic_meter": {
        "desc": "Total volume of the cargo in cubic meters (CBM).",
        "type": "numeric",
    },
    "cargo_count": {
        "desc": "Total number of packages or units (e.g. cartons).",
        "type": "numeric",
    },
    "cargo_um": {"desc": "Unit of measure for the cargo count.", "type": "string"},
    "cargo_detail_count": {
        "desc": "Total sum of all cargo line item quantities.",
        "type": "numeric",
    },
    "detail_cargo_um": {
        "desc": "Unit of measure for the cargo detail count.",
        "type": "string",
    },
    "856_filing_status": {
        "desc": "Definition for 856 filing status.",
        "type": "categorical",
    },
    "get_isf_submission_date": {
        "desc": "Definition for get isf submission date.",
        "type": "categorical",
    },
    "seal_number": {"desc": "Definition for seal number.", "type": "string"},
    "in_gate_date": {"desc": "Definition for in gate date.", "type": "datetime"},
    "in_gate_lcn": {"desc": "Definition for in gate lcn.", "type": "string"},
    "empty_container_dispatch_date": {
        "desc": "Definition for empty container dispatch date.",
        "type": "datetime",
    },
    "empty_container_dispatch_lcn": {
        "desc": "Definition for empty container dispatch lcn.",
        "type": "string",
    },
    "consignee_name": {"desc": "Definition for consignee name.", "type": "string"},
    "optimal_ata_dp_date": {
        "desc": "The best available date for arrival at discharge port.",
        "type": "datetime",
    },
    "optimal_eta_fd_date": {
        "desc": "The best available date for arrival at final destination.",
        "type": "datetime",
    },
    "delayed_dp": {"desc": "Definition for delayed dp.", "type": "categorical"},
    "dp_delayed_dur": {
        "desc": "Number of days the shipment is delayed at the discharge port.",
        "type": "numeric",
    },
    "delayed_fd": {"desc": "Definition for delayed fd.", "type": "categorical"},
    "fd_delayed_dur": {
        "desc": "Number of days the shipment is delayed at the final destination.",
        "type": "numeric",
    },
    "shipment_status": {
        "desc": "Current phase of the shipment (e.g., DELIVERED, IN_OCEAN, READY_FOR_PICKUP).",
        "type": "categorical",
    },
    # "critical_dates_summary": {
    #     "desc": "Definition for critical dates summary.",
    #     "type": "string"
    # },
    "delay_reason_summary": {
        "desc": "Definition for delay reason summary.",
        "type": "string",
    },
    "workflow_gap_flags": {
        "desc": "Definition for workflow gap flags.",
        "type": "list",
    },
    # "milestones": {
    #     "desc": "Definition for milestones.",
    #     "type": "list"
    # },
    "vessel_summary": {"desc": "Definition for vessel summary.", "type": "string"},
    "carrier_summary": {"desc": "Definition for carrier summary.", "type": "string"},
    "port_route_summary": {
        "desc": "Definition for port route summary.",
        "type": "string",
    },
    "source_group": {"desc": "Definition for source group.", "type": "categorical"},
    # "source_month_tag": {
    #     "desc": "Definition for source month tag.",
    #     "type": "categorical"
    # },
    # "combined_content": {
    #     "desc": "Definition for combined content.",
    #     "type": "string"
    # }
}

# Technical columns that should NOT be visible to the LLM or used in UI reports
INTERNAL_COLUMNS = [
    "carr_eqp_uid",
    "job_no",
    "consignee_codes",
    "document_id",
    "combined_content",
    "source_group",
    "source_month_tag",
    "consignee_raw",
]

COLUMN_SYNONYMS = {
    "weight": "cargo_weight_kg",
    "vol": "cargo_measure_cubic_meter",
    "volume": "cargo_measure_cubic_meter",
    "count": "cargo_count",
    "carrier": "final_carrier_name",
    "vessel": "final_vessel_name",
    "status": "shipment_status",
    "shipper": "supplier_vendor_name",
    "arrival": "optimal_ata_dp_date",
    "destination_eta": "optimal_eta_fd_date",
    "delay": "dp_delayed_dur",
    "delivery_delay": "fd_delayed_dur",
    "departure": "etd_lp_date",
    "actual_departure": "atd_lp_date",
    "estimated_departure": "etd_lp_date",
    "etd": "etd_lp_date",
    "atd": "atd_lp_date",
    "ready_date": "cargo_ready_date",
    "po": "po_numbers",
    "container": "container_number",
    "obl": "obl_nos",
}

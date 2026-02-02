"""
Centralized metadata for shipment analytics.
This file defines the schema, types, and descriptions used for both 
data type casting and LLM dynamic prompt generation.
"""

# Searchable columns with their technical and human-friendly attributes
ANALYTICS_METADATA = {
    "container_number": {
        "desc": "The unique 11-character container identifier.",
        "type": "string"
    },
    "hot_container_flag": {
        "desc": "Flag indicating if the container is hot.",
        "type": "boolean"
    },
    "shipment_status": {
        "desc": "Current phase of the shipment (e.g., DELIVERED, IN_OCEAN, READY_FOR_PICKUP).",
        "type": "categorical"
    },
    "cargo_weight_kg": {
        "desc": "Total weight of the cargo in kilograms.",
        "type": "numeric"
    },
    "cargo_measure_cubic_meter": {
        "desc": "Total volume of the cargo in cubic meters (CBM).",
        "type": "numeric"
    },
    "cargo_count": {
        "desc": "Total number of packages or units (e.g. cartons).",
        "type": "numeric"
    },
    "cargo_detail_count": {
        "desc": "Total sum of all cargo line item quantities.",
        "type": "numeric"
    },
    "true_carrier_scac_name": {
        "desc": "The primary carrier shipping line name.",
        "type": "string"
    },
    "final_carrier_name": {
        "desc": "The name of the carrier handling the final leg.",
        "type": "string"
    },
    "first_vessel_name": {
        "desc": "The name of the vessel for the first leg of ocean transport.",
        "type": "string"
    },
    "final_vessel_name": {
        "desc": "The name of the vessel for the final ocean leg.",
        "type": "string"
    },
    "supplier_vendor_name": {
        "desc": "The shipper or supplier of the goods.",
        "type": "string"
    },
    "manufacturer_name": {
        "desc": "The company that manufactured the goods.",
        "type": "string"
    },
    "load_port": {
        "desc": "The port where the cargo was initially loaded.",
        "type": "string"
    },
    "discharge_port": {
        "desc": "The port where the cargo is unloaded from the final vessel.",
        "type": "string"
    },
    "final_destination": {
        "desc": "The final point of delivery (often a city or warehouse).",
        "type": "string"
    },
    "dp_delayed_dur": {
        "desc": "Number of days the shipment is delayed at the discharge port.",
        "type": "numeric"
    },
    "fd_delayed_dur": {
        "desc": "Number of days the shipment is delayed at the final destination.",
        "type": "numeric"
    },
    "eta_dp_date": {
        "desc": "Estimated Time of Arrival at Discharge Port.",
        "type": "datetime"
    },
    "ata_dp_date": {
        "desc": "Actual Time of Arrival at Discharge Port.",
        "type": "datetime"
    },
    "eta_fd_date": {
        "desc": "Estimated Time of Arrival at Final Destination.",
        "type": "datetime"
    },
    "optimal_ata_dp_date": {
        "desc": "The best available date for arrival at discharge port.",
        "type": "datetime"
    },
    "optimal_eta_fd_date": {
        "desc": "The best available date for arrival at final destination.",
        "type": "datetime"
    },
    "atd_lp_date": {
        "desc": "Actual Time of Departure from Load Port.",
        "type": "datetime"
    },
    "etd_lp_date": {
        "desc": "Estimated Time of Departure from Load Port.",
        "type": "datetime"
    },
    "booking_numbers": {
        "desc": "Internal shipment booking identifiers.",
        "type": "list"
    },
    "po_numbers": {
        "desc": "Customer Purchase Order numbers.",
        "type": "list"
    },
    "obl_number": {
        "desc": "Original Bill of Lading number.",
        "type": "string"
    },

}

# Technical columns that should NOT be visible to the LLM or used in UI reports
INTERNAL_COLUMNS = ["carr_eqp_uid", "job_no", "consignee_codes", "document_id"]

# Common synonyms mapping to assist intent matching or prompt generation
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
}

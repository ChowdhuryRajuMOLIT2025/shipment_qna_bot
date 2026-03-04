# Shipment Q&A Bot - Analytics Reference

This file serves as a **Ready Reference** for the LLM to understand the dataset schema, column definitions, and how to construct DuckDB SQL queries for common operational questions.

## 0. Response Style And Sorting Policy

### Communication Style
- Tone: soft, calm, respectful.
- Role: critical thinker.
- Behavior: acute professional.
- Keep responses concise, factual, and grounded in the data.
- If an assumption is used, state it briefly.

### Sorting Policy (Global)
- For tabular/list outputs with date columns, sort by latest date first (descending) before formatting dates.
- Date priority for sorting: `best_eta_dp_date` -> `best_eta_fd_date` -> `ata_dp_date` -> `derived_ata_dp_date` -> `eta_dp_date` -> `eta_fd_date`.
- Apply `.dt.strftime('%d-%b-%Y')` only after sorting.

## 1. Dataset Columns (Schema)

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `container_number` | string | The unique 11-character container identifier. |
| `container_type` | categorical | Definition for container type. (e.g., 'S4' = 40' Flat Rack, 'D4' = 40' Dry) |
| `destination_service` | categorical | Definition for destination service. |
| `po_numbers` | list | Customer Purchase Order numbers. |
| `booking_numbers` | list | Internal shipment booking identifiers. |
| `fcr_numbers` | list | Definition for fcr numbers. |
| `obl_nos` | list | Original Bill of Lading numbers (OBL). |
| `load_port` | string | The port where the cargo was initially loaded. |
| `final_load_port` | string | Definition for final load port. |
| `discharge_port` | string | The port where the cargo is unloaded from the final vessel. |
| `last_cy_location` | string | Definition for last cy location. |
| `place_of_receipt` | string | Definition for place of receipt. |
| `place_of_delivery` | string | Definition for place of delivery. |
| `final_destination` | string | The final point of delivery (often a city or warehouse). |
| `first_vessel_name` | string | The name of the vessel for the first leg of ocean transport. |
| `final_carrier_name` | string | The name of the carrier handling the final leg. |
| `final_vessel_name` | string | The name of the vessel for the final ocean leg. |
| `true_carrier_scac_name` | string | The primary carrier shipping line name. |
| `etd_lp_date` | datetime | Estimated Time of Departure from Load Port. |
| `etd_flp_date` | datetime | Definition for etd flp date. |
| `eta_dp_date` | datetime | Estimated Time of Arrival at Discharge Port. |
| `eta_fd_date` | datetime | Estimated Time of Arrival at Final Destination. |
| `ata_dp_date` | datetime | Actual Time of Arrival at Discharge Port (raw/source value). |
| `best_eta_dp_date` | datetime | Best expected ETA at Discharge Port. **DEFAULT** for DP ETA window and overdue checks. |
| `atd_flp_date` | datetime | Definition for atd flp date. |
| `cargo_receiveds_date` | string | Definition for cargo receiveds date. |
| `detention_free_days` | numeric | Definition for detention free days. |
| `demurrage_free_days` | numeric | Definition for demurrage free days. |
| `hot_container_flag` | boolean | Flag indicating if the container is hot (Priority). |
| `supplier_vendor_name` | string | The shipper or supplier of the goods. |
| `manufacturer_name` | string | The company that manufactured the goods. |
| `ship_to_party_name` | string | Definition for ship to party name. |
| `booking_approval_status` | string | Definition for booking approval status. |
| `service_contract_number` | string | Definition for service contract number. |
| `carrier_vehicle_load_date` | datetime | Definition for carrier vehicle load date. |
| `carrier_vehicle_load_lcn` | string | Definition for carrier vehicle load lcn. |
| `vehicle_departure_date` | datetime | Definition for vehicle departure date. |
| `vehicle_departure_lcn` | string | Definition for vehicle departure lcn. |
| `vehicle_arrival_date` | datetime | Definition for vehicle arrival date. |
| `vehicle_arrival_lcn` | string | Definition for vehicle arrival lcn. |
| `carrier_vehicle_unload_date` | datetime | Definition for carrier vehicle unload date. |
| `carrier_vehicle_unload_lcn` | string | Definition for carrier vehicle unload lcn. |
| `out_gate_from_dp_date` | datetime | Definition for out gate from dp date. |
| `out_gate_from_dp_lcn` | string | Definition for out gate from dp lcn. |
| `equipment_arrived_at_last_cy_date` | datetime | Definition for equipment arrived at last cy date. |
| `equipment_arrived_at_last_cy_lcn` | string | Definition for equipment arrived at last cy lcn. |
| `out_gate_at_last_cy_date` | datetime | Definition for out gate at last cy date. |
| `out_gate_at_last_cy_lcn` | string | Definition for out gate at last cy lcn. |
| `delivery_to_consignee_date` | datetime | Definition for delivery to consignee date. |
| `delivery_to_consignee_lcn` | string | Definition for delivery to consignee lcn. |
| `empty_container_return_date` | datetime | Definition for empty container return date. |
| `empty_container_return_lcn` | string | Definition for empty container return lcn. |
| `co2_tank_on_wheel` | numeric | Definition for co2 tank on wheel. |
| `co2_well_to_wheel` | numeric | Definition for co2 well to wheel. |
| `job_type` | categorical | Definition for job type. |
| `mcs_hbl` | string | Definition for mcs hbl. |
| `transport_mode` | categorical | Definition for transport mode. |
| `rail_load_dp_date` | datetime | Definition for rail load dp date. |
| `rail_load_dp_lcn` | string | Definition for rail load dp lcn. |
| `rail_departure_dp_date` | datetime | Definition for rail departure dp date. |
| `rail_departure_dp_lcn` | string | Definition for rail departure dp lcn. |
| `rail_arrival_destination_date` | datetime | Definition for rail arrival destination date. |
| `rail_arrival_destination_lcn` | string | Definition for rail arrival destination lcn. |
| `cargo_ready_date` | string | Definition for cargo ready date. |
| `in-dc_date` | datetime | Definition for in-dc date. |
| `cargo_weight_kg` | numeric | Total weight of the cargo in kilograms. |
| `cargo_measure_cubic_meter` | numeric | Total volume of the cargo in cubic meters (CBM). |
| `cargo_count` | numeric | Total number of packages or units (e.g. cartons). |
| `cargo_um` | string | Unit of measure for the cargo count. |
| `cargo_detail_count` | numeric | Total sum of all cargo line item quantities. |
| `detail_cargo_um` | string | Unit of measure for the cargo detail count. |
| `856_filing_status` | categorical | Definition for 856 filing status. |
| `get_isf_submission_date` | categorical | Definition for get isf submission date. |
| `seal_number` | string | Definition for seal number. |
| `in_gate_date` | datetime | Definition for in gate date. |
| `in_gate_lcn` | string | Definition for in gate lcn. |
| `empty_container_dispatch_date` | datetime | Definition for empty container dispatch date. |
| `empty_container_dispatch_lcn` | string | Definition for empty container dispatch lcn. |
| `consignee_name` | string | Definition for consignee name. |
| `optimal_ata_dp_date` | datetime | Legacy consolidated arrival date at discharge port (not default). |
| `best_eta_fd_date` | datetime | Best expected ETA at final destination. |
| `delayed_dp` | categorical | Definition for delayed dp and handy filteration for shipment categoriezed as delay, On time or early reached |
| `dp_delayed_dur` | numeric | Number of days the shipment is delayed/on_time/early at the discharge port. |
| `delayed_fd` | categorical | Definition for delayed fd. |
| `fd_delayed_dur` | numeric | Number of days the shipment is delayed at the final destination. |
| `shipment_status` | categorical | Current phase of the shipment (e.g., DELIVERED, IN_OCEAN, READY_FOR_PICKUP). |
| `delay_reason_summary` | string | Definition for delay reason summary. |
| `workflow_gap_flags` | list | Definition for workflow gap flags. |
| `vessel_summary` | string | Definition for vessel summary. |
| `carrier_summary` | string | Definition for carrier summary. |
| `port_route_summary` | string | Definition for port route summary. |
| `source_group` | categorical | Definition for source group. |



## 2. Reference Scenarios (Operational Queries)

### Scenario A: Delayed Shipments (Discharge Port)
**User Query:** "How many shipments are delayed?" (or "Show delayed shipments")
**Logic:**
- Filter: `dp_delayed_dur > 0`
- Date Column: `best_eta_dp_date` (Format: '%d-%b-%Y')
- Display Protocol: Show container,po_numbers, best_eta_dp_date, and delay days.

**DuckDB SQL:**
```sql
SELECT
    container_number,
    po_numbers,
    strftime(best_eta_dp_date, '%d-%b-%Y') AS best_eta_dp_date,
    dp_delayed_dur,
    shipment_status
FROM df
WHERE dp_delayed_dur > 0
ORDER BY best_eta_dp_date DESC;
```

### Scenario B: Final Destination (FD) Delays
**User Query:** "Show me delayed FD shipments" (or "Check FD delays")
**Logic:**
- Filter: `fd_delayed_dur > 0`
- Date Column: `eta_fd_date` or `best_eta_fd_date`
- Display Protocol: Show container, FD date, and FD delay days.

**DuckDB SQL:**
```sql
SELECT
    container_number,
    po_numbers,
    strftime(best_eta_fd_date, '%d-%b-%Y') AS best_eta_fd_date,
    fd_delayed_dur,
    final_destination
FROM df
WHERE fd_delayed_dur > 0
ORDER BY best_eta_fd_date DESC;
```

### Scenario C: Hot / Priority Shipments
**User Query:** "List hot containers" (or "Show priority shipments")
**Logic:**
- Filter: `hot_container_flag == True`
- Columns: `container_number`,`po_numbers`, `hot_container_flag`, `shipment_status`

**DuckDB SQL:**
```sql
SELECT
    container_number,
    po_numbers,
    hot_container_flag,
    shipment_status,
    strftime(best_eta_dp_date, '%d-%b-%Y') AS best_eta_dp_date
FROM df
WHERE hot_container_flag = TRUE
ORDER BY best_eta_dp_date DESC NULLS LAST;
```

### Scenario D: Delivered Shipments to Consignee (Final Destination)
**User Query:** "Show delivered shipments to consignee" (or "Delivered to consignee")
**Logic:**
- DP Reached: `best_eta_dp_date` is not null **and** `< today`.
- Delivered: `delivery_to_consignee_date` **or** `empty_container_return_date` is not null.
- Not Delivered: If **both** delivery dates are null, then it is **not** delivered (even if DP reached).
- Display Protocol: Show container, PO, DP date, delivery/return dates, and status.

**DuckDB SQL:**
```sql
SELECT
    container_number,
    po_numbers,
    discharge_port,
    strftime(best_eta_dp_date, '%d-%b-%Y') AS best_eta_dp_date,
    final_destination,
    strftime(delivery_to_consignee_date, '%d-%b-%Y') AS delivery_to_consignee_date,
    strftime(empty_container_return_date, '%d-%b-%Y') AS empty_container_return_date,
    shipment_status
FROM df
WHERE best_eta_dp_date IS NOT NULL
  AND best_eta_dp_date < CURRENT_DATE
  AND (
      delivery_to_consignee_date IS NOT NULL
      OR empty_container_return_date IS NOT NULL
  )
ORDER BY best_eta_dp_date DESC;
```

### Scenario E: Next 5-Day Container Schedule (Nashville Example)
**User Query:** "Next 5 day container schedule for Nashville" (or "shipments coming in next 10 days at Savannah")
**Logic:**
- Arrival window based on `best_eta_dp_date`
- Filter: `discharge_port` contains the city
- Display Protocol: Show container, PO, arrival date, load port, discharge port.

**DuckDB SQL:**
```sql
SELECT
    container_number,
    po_numbers,
    load_port,
    discharge_port,
    strftime(best_eta_dp_date, '%d-%b-%Y') AS best_eta_dp_date
FROM df
WHERE discharge_port ILIKE '%nashville%'
  AND best_eta_dp_date >= CURRENT_DATE
  AND best_eta_dp_date <= CURRENT_DATE + INTERVAL 5 DAY
ORDER BY best_eta_dp_date ASC;
```

### Scenario F: Shipment Not Yet Arrived At DP (Missed ETA / Overdue)
**User Query:** "Which shipments failed to reach DP at Nashville?" (or "not yet arrived at DP in Nashville")
**Interpretation Rules:**
- "Not yet arrived at DP": `ata_dp_date` is null.
- "Failed/missed ETA at DP": `ata_dp_date` is null **and** `best_eta_dp_date <= today`.
**Logic:**
- Filter discharge port by location (e.g., Nashville).
- Keep only records where DP actual arrival is missing.
- For failed/missed ETA, keep only overdue expected arrivals.

**DuckDB SQL:**
```sql
SELECT
    container_number,
    po_numbers,
    load_port,
    discharge_port,
    strftime(best_eta_dp_date, '%d-%b-%Y') AS best_eta_dp_date,
    shipment_status
FROM df
WHERE discharge_port ILIKE '%nashville%'
  AND ata_dp_date IS NULL
  AND best_eta_dp_date IS NOT NULL
  AND best_eta_dp_date <= CURRENT_DATE
ORDER BY best_eta_dp_date DESC;
```

# Shipment Q&A Bot - Analytics Reference

This file serves as a **Ready Reference** for the LLM to understand the dataset schema, column definitions, and how to construct Pandas queries for common operational questions.

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

## 0A. STRICT Column Selection & Filtering Guardrails (Performance + Accuracy)

Use these rules to avoid wrong-column filtering. Correct column choice reduces failed analytics code generation, retries, and extra LLM calls.

### Canonical Column Priority (Use This First)
- **Discharge Port ETA / arrival window / overdue checks (DP):** `best_eta_dp_date` (fallback: `eta_dp_date`)
- **Discharge Port actual arrival reached/not reached:** `ata_dp_date` (fallback for display only: `derived_ata_dp_date`)
- **Final Destination ETA / arrival window / overdue checks (FD):** `best_eta_fd_date` (fallback: `eta_fd_date`)
- **DP delay filter (generic \"delay\" unless FD explicitly requested):** `dp_delayed_dur`
- **FD delay filter (only if user explicitly asks FD/final destination delay):** `fd_delayed_dur`
- **DP delay category label:** `delayed_dp`
- **FD delay category label:** `delayed_fd`

### Location Filter Mapping (Very Important)
- If user asks about **port arrival / DP / discharge port / arriving at port/city** -> filter `discharge_port`
- If user asks about **final destination / FD / DC / warehouse / in-dc** -> filter `final_destination`
- Do **NOT** use `load_port` for destination-arrival questions unless user explicitly says origin/load port.

### ID Filter Mapping (Strict)
- Container query -> `container_number`
- PO query -> `po_numbers`
- Booking query -> `booking_numbers`
- OBL query -> `obl_nos`
- Do not swap these list columns; if the identifier type is unclear, check all ID columns explicitly and state the assumption.

### Delay Interpretation (Strict Defaults)
- Generic \"delayed shipments\" -> use `dp_delayed_dur > 0`
- If user says delayed/early but does **not** specify a delay/early threshold, apply a default severity band (to avoid huge pulls):
  - DP delayed default -> `0 < dp_delayed_dur <= 7`
  - DP early default -> `-7 <= dp_delayed_dur < 0`
  - FD delayed default (explicit FD scope) -> `0 < fd_delayed_dur <= 7`
  - FD early default (explicit FD scope) -> `-7 <= fd_delayed_dur < 0`
- If user explicitly specifies delay/early severity (example: `more than 5 days`, `between 3 and 15 days`, `early by more than 3 days`), use the user threshold and **do not** apply the default 7-day severity band.
- When the default severity band is applied, mention the assumption briefly in the response (example: `No delay threshold was specified, so I used the default DP delay range: 1 to 7 days.`).
- \"FD delayed\" / \"final destination delayed\" -> use `fd_delayed_dur > 0`
- \"On time\" DP -> prefer `dp_delayed_dur <= 0` or `delayed_dp == 'on_time'` (depending on question wording)
- \"Missed ETA at DP\" -> `ata_dp_date.isna()` and `best_eta_dp_date <= today`

### "Received" Interpretation (Strict Default)
- If user says **shipment/container received** (without saying "cargo received"), default to `ata_dp_date.notna()`
- If user explicitly says **cargo received**, use `cargo_receiveds_date` (current dataset column name / legacy naming)
- <!-- If user says cargo received, use `cargo_received_date` -->
- If future schema exposes `cargo_received_date`, treat it as alias mapping to current `cargo_receiveds_date`.
- For "received within date", apply the date window on the same selected received-date column:
  - default received -> `ata_dp_date`
  - explicit cargo received -> `cargo_receiveds_date`

### Default Date Window Cap (When User Does Not Specify Duration/Date)
- To reduce huge data pull, if the user does **not** specify any date/month/range, apply a **default 90-day cap** using the correct anchor column.
- Explicit date/month/range (example: `Dec'2026`, `last 30 days`, `between ... and ...`) **overrides** the default cap.
- Delay/early queries without an explicit date window should still use this default 90-day cap in addition to the default 7-day severity band (unless the user provides a delay/early threshold).
- Generic **received / arrived at DP / shipment received / container received**:
  - anchor column: `ata_dp_date`
  - default window: `today - 90 days` to `today`
- Explicit **cargo received**:
  - anchor column: `cargo_receiveds_date`
  - default window: `today - 90 days` to `today`
- **Received but not delivered** (without explicit date window):
  - anchor column: `ata_dp_date`
  - default window: `today - 90 days` to `today`
- Future/incoming wording such as **will receive / incoming / arriving / expected / ETA**:
  - anchor column: `best_eta_dp_date` (or `best_eta_fd_date` when FD/final destination/DC is explicitly requested)
  - default window: `today` to `today + 90 days`
- Delay queries without explicit date window:
  - If query also says **received/arrived**, keep the default received anchor (`ata_dp_date`) for the 90-day lookback.
  - Otherwise, use ETA anchor (`best_eta_dp_date` by default, `best_eta_fd_date` for FD scope).
- Early queries without explicit date window:
  - If query says **arrived early**, use received anchor (`ata_dp_date`) for the 90-day lookback.
  - Otherwise, use ETA anchor (`best_eta_dp_date` by default, `best_eta_fd_date` for FD scope).
- Mention the assumption briefly in the response when this default cap is used (example: "Using default 90-day window because no date range was specified.").

### Anti-Mistake Rules (Do Not Use Unless Explicitly Asked)
- Do **NOT** use `optimal_ata_dp_date` for default DP filtering (legacy only).
- Do **NOT** use `optimal_eta_fd_date` as first-choice FD ETA if `best_eta_fd_date` is present.
- Do **NOT** use `derived_ata_dp_date` for overdue/not-arrived filtering; use `ata_dp_date` null check first.
- Do **NOT** filter DP questions using `final_destination`.
- Do **NOT** filter FD/DC questions using `discharge_port` unless the user explicitly asks for port + FD comparison.
- Do **NOT** use `cargo_receiveds_date` for generic "shipment/container received" questions unless user explicitly says cargo received.

### Filtering Checklist Before Writing Pandas Code
- Confirm question scope: DP vs FD vs load/origin
- Confirm ID type: container vs PO vs booking vs OBL
- Confirm metric type: ETA window vs actual arrival vs delay duration vs status
- Confirm canonical column above before writing the mask

### Example (Preferred Pattern)
```python
# DP arrival window -> use best_eta_dp_date (not ata_dp_date / optimal_ata_dp_date)
loc_mask = df['discharge_port'].str.contains('nashville', case=False, na=False, regex=True)
date_mask = df['best_eta_dp_date'].notna() & (df['best_eta_dp_date'] >= today) & (df['best_eta_dp_date'] <= next_5_days)
df_filtered = df[loc_mask & date_mask]
```

## 1. Dataset Columns (Schema)

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `job_no` | string | The job number associated with container. |
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
<!-- | `source_group` | categorical | Definition for source group. | -->



## 2. Reference Scenarios (Operational Queries)

### Scenario A: Delayed Shipments (Discharge Port)
**User Query:** "How many shipments are delayed?" (or "Show delayed shipments")
**Logic:**
- Filter: `dp_delayed_dur > 0`
- Date Column: `best_eta_dp_date` (Format: '%d-%b-%Y')
- Display Protocol: Show container,po_numbers, best_eta_dp_date, and delay days.

**Pandas Code:**
```python
# Filter for delays > 0
# df_filtered = df[df['dp_delayed_dur'] > 0].copy()
df_filtered = df[df['dp_delayed_dur'] > 0]

# Format Default Date Column
if 'best_eta_dp_date' in df_filtered.columns:
    df_filtered['best_eta_dp_date'] = df_filtered['best_eta_dp_date'].dt.strftime('%d-%b-%Y')

# Select Output Columns
result = df_filtered[[
    'container_number', 
    # 'po_numbers', 
    'best_eta_dp_date', 
    'dp_delayed_dur', 
    'shipment_status'
    ]]
```

### Scenario B: Final Destination (FD) Delays
**User Query:** "Show me delayed FD shipments" (or "Check FD delays")
**Logic:**
- Filter: `fd_delayed_dur > 0`
<!-- - Date Column: `eta_fd_date` or `best_eta_fd_date` -->
- Date Column (STRICT): `best_eta_fd_date` (fallback only if missing: `eta_fd_date`)
- Display Protocol: Show container, FD date, and FD delay days.

**Pandas Code:**
```python
# Filter for FD delays > 0
# df_filtered = df[df['fd_delayed_dur'] > 0].copy()
df_filtered = df[df['fd_delayed_dur'] > 0]

# Format FD Date Column
if 'best_eta_fd_date' in df_filtered.columns:
    df_filtered['best_eta_fd_date'] = df_filtered['best_eta_fd_date'].dt.strftime('%d-%b-%Y')

# Select Output Columns
result = df_filtered[[
    'container_number', 
    # 'po_numbers', 
    'best_eta_fd_date', 
    'fd_delayed_dur', 
    'final_destination'
    ]]
```

### Scenario C: Hot / Priority Shipments
**User Query:** "List hot containers" (or "Show priority shipments")
**Logic:**
- Filter: `hot_container_flag == True`
- Columns: `container_number`,`po_numbers`, `hot_container_flag`, `shipment_status`

**Pandas Code:**
```python
# Filter for Hot Containers
# df_filtered = df[df['hot_container_flag'] == True].copy()
df_filtered = df[df['hot_container_flag'] == True]

# Select Output Columns
result = df_filtered[[
    'container_number',
    # 'po_numbers', 
    'hot_container_flag', 
    'shipment_status', 
    'best_eta_dp_date']]
```

### Scenario D: Delivered Shipments to Consignee (Final Destination)
**User Query:** "Show delivered shipments to consignee" (or "Delivered to consignee")
**Logic:**
- DP Reached: `best_eta_dp_date` is not null **and** `< today`.
- Delivered: `delivery_to_consignee_date` **or** `empty_container_return_date` is not null.
- Not Delivered: If **both** delivery dates are null, then it is **not** delivered (even if DP reached).
- Display Protocol: Show container, PO, DP date, delivery/return dates, and status.

**Pandas Code:**
```python
# Shipment reached DP (before today) and delivered to consignee
today = pd.Timestamp.today().normalize()

# df_filtered = df[
#     df['best_eta_dp_date'].notna() &
#     (df['best_eta_dp_date'] < today) &
#     (df['delivery_to_consignee_date'].notna() | df['empty_container_return_date'].notna())
# ].copy()



df_filtered = df[
    df['best_eta_dp_date'].notna() &
    (df['best_eta_dp_date'] < today) &
    (df['delivery_to_consignee_date'].notna() | df['empty_container_return_date'].notna())
]

# Format key date columns
for col in ['best_eta_dp_date', 'delivery_to_consignee_date', 'empty_container_return_date']:
    if col in df_filtered.columns:
        df_filtered[col] = df_filtered[col].dt.strftime('%d-%b-%Y')

# Select Output Columns
result = df_filtered[[
    'container_number',
    # 'po_numbers',
    'discharge_port',
    'best_eta_dp_date',
    'final_destination',
    'delivery_to_consignee_date',
    'empty_container_return_date',
    'shipment_status'
]]
```

### Scenario E: Next 5-Day Container Schedule (Nashville Example)
**User Query:** "Next 5 day container schedule for Nashville" (or "shipments coming in next 10 days at Savannah")
**Logic:**
- Arrival window based on `best_eta_dp_date`
- Filter: `discharge_port` contains the city
- Display Protocol: Show container, PO, arrival date, load port, discharge port.

**Pandas Code:**
```python
today = pd.Timestamp.today().normalize()
next_5_days = today + pd.Timedelta(days=5)

# Filter for shipments with discharge port containing "Nashville" arriving within the next 5 days
df_filtered = df[
    df['discharge_port'].str.contains('nashville', na=False, case=False) &
    (df['best_eta_dp_date'] >= today) &
    (df['best_eta_dp_date'] <= next_5_days)
].copy()

# Format the arrival date column
df_filtered['best_eta_dp_date'] = df_filtered['best_eta_dp_date'].dt.strftime('%d-%b-%Y')

# Select relevant columns
result = df_filtered[[
    'container_number', 
    # 'po_numbers', 
    'load_port', 
    'discharge_port', 
    'best_eta_dp_date'
    ]]
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

**Pandas Code:**
```python
today = pd.Timestamp.today().normalize()

loc_mask = df['discharge_port'].str.contains(
    pat='nashville', case=False, na=False, regex=True
)
not_arrived_mask = df['ata_dp_date'].isna()
overdue_mask = df['best_eta_dp_date'].notna() & (df['best_eta_dp_date'] <= today)

mask = loc_mask & not_arrived_mask & overdue_mask
# df_filtered = df[mask].copy()
df_filtered = df[mask]

# Sort latest expected arrivals first (current date first)
df_filtered = df_filtered.sort_values('best_eta_dp_date', ascending=False)

df_filtered['best_eta_dp_date'] = df_filtered['best_eta_dp_date'].dt.strftime('%d-%b-%Y')

result = df_filtered[
    [
        'container_number', 
        # 'po_numbers', 
        'load_port', 
        'discharge_port', 
        'best_eta_dp_date', 
        'shipment_status'
        ]
]
```

### Scenario G: Shipment/Container Received But Not Yet Delivered Within Date
**User Query:** "Show shipment/container I received but not yet delivered within date" (or "containers received last week but not delivered")
**Interpretation Rules (STRICT):**
- Default **"received"** means shipment/container reached DP -> `ata_dp_date` is not null.
- Only if user explicitly says **"cargo received"**, use `cargo_receiveds_date` (current dataset column name).
- "Not yet delivered" means **both** `delivery_to_consignee_date` and `empty_container_return_date` are null.
- "Within date" applies to the selected received-date column (default `ata_dp_date`, explicit cargo-received -> `cargo_receiveds_date`).

**Logic (Default shipment/container received):**
- Received filter: `ata_dp_date.notna()`
- Date window on `ata_dp_date`
- Not delivered filter:
  - `delivery_to_consignee_date.isna()`
  - `empty_container_return_date.isna()`
- Optional location mapping:
  - DP/port wording -> filter `discharge_port`
  - FD/DC wording -> filter `final_destination`

**Pandas Code (Default = shipment/container received):**
```python
# Example date window (replace with user-derived dates)
start_date = pd.Timestamp('2025-01-01')
end_date = pd.Timestamp('2025-01-31')

received_mask = df['ata_dp_date'].notna()
date_mask = (
    df['ata_dp_date'].notna() &
    (df['ata_dp_date'] >= start_date) &
    (df['ata_dp_date'] <= end_date)
)
not_delivered_mask = (
    df['delivery_to_consignee_date'].isna() &
    df['empty_container_return_date'].isna()
)

# Optional DP location example
# loc_mask = df['discharge_port'].str.contains('nashville', case=False, na=False, regex=True)
# mask = received_mask & date_mask & not_delivered_mask & loc_mask

mask = received_mask & date_mask & not_delivered_mask
# df_filtered = df[mask].copy()
df_filtered = df[mask]

# Sort latest received first BEFORE formatting
df_filtered = df_filtered.sort_values('ata_dp_date', ascending=False)

for col in ['ata_dp_date', 'delivery_to_consignee_date', 'empty_container_return_date']:
    if col in df_filtered.columns:
        df_filtered[col] = pd.to_datetime(df_filtered[col], errors='coerce').dt.strftime('%d-%b-%Y')

result = df_filtered[
    [
        'container_number',
        # 'po_numbers',
        'discharge_port',
        'final_destination',
        'ata_dp_date',
        'delivery_to_consignee_date',
        'empty_container_return_date',
        'shipment_status'
    ]
]
```

**Pandas Code (Only when user explicitly says \"cargo received\"):**
```python
# NOTE: Current dataset column is `cargo_receiveds_date` (legacy naming)
start_date = pd.Timestamp('2025-01-01')
end_date = pd.Timestamp('2025-01-31')

cargo_recv_col = 'cargo_receiveds_date'

df_work = df.copy()
df_work[cargo_recv_col] = pd.to_datetime(df_work[cargo_recv_col], errors='coerce')

cargo_received_mask = df_work[cargo_recv_col].notna()
date_mask = (
    df_work[cargo_recv_col].notna() &
    (df_work[cargo_recv_col] >= start_date) &
    (df_work[cargo_recv_col] <= end_date)
)
not_delivered_mask = (
    df_work['delivery_to_consignee_date'].isna() &
    df_work['empty_container_return_date'].isna()
)

df_filtered = df_work[cargo_received_mask & date_mask & not_delivered_mask]
df_filtered = df_filtered.sort_values(cargo_recv_col, ascending=False)

for col in [cargo_recv_col, 'delivery_to_consignee_date', 'empty_container_return_date']:
    if col in df_filtered.columns:
        df_filtered[col] = pd.to_datetime(df_filtered[col], errors='coerce').dt.strftime('%d-%b-%Y')

result = df_filtered[
    [
        'container_number',
        # 'po_numbers',
        'discharge_port',
        'final_destination',
        cargo_recv_col,
        'delivery_to_consignee_date',
        'empty_container_return_date',
        'shipment_status'
    ]
]
```

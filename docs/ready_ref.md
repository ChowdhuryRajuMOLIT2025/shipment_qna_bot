# Shipment Q&A Bot - Analytics Reference

This file serves as a **Ready Reference** for the LLM to follow operational SQL patterns and response behavior for common logistics questions.

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

### Default Capping Policy (When User Gives No Duration)
- Future arrival/delivery intent (`will arrive`, `will be delivered`, `upcoming`) must be capped to:
  - `CURRENT_DATE` to `CURRENT_DATE + INTERVAL 30 DAY`
- Past arrived/received/delivered intent (`arrived`, `received`, `delivered`) must be capped to:
  - `CURRENT_DATE - INTERVAL 30 DAY` to `CURRENT_DATE`
- Delay/Early intent without explicit duration must use default threshold:
  - `>= 7 days` for delayed
  - `<= -7 days` for early
- If any default cap/threshold is applied, explicitly mention it in the response note.
- If user provides explicit date range/duration, do not apply these defaults.

## 1. Reference Scenarios (Operational Queries)

### Scenario A: Delayed Shipments (Discharge Port)
**User Query:** "How many shipments are delayed?" (or "Show delayed shipments")
**Logic:**
- Filter: `dp_delayed_dur > 0`
- Date Column: `best_eta_dp_date` (Format: '%d-%b-%Y')
- Display Protocol: Show container, discharge_port, best_eta_dp_date, and delay days.

**DuckDB SQL:**
```sql
SELECT
    container_number,
    discharge_port,
    strftime(best_eta_dp_date, '%d-%b-%Y') AS best_eta_dp_date,
    dp_delayed_dur,
    -- shipment_status
FROM df
WHERE dp_delayed_dur > 0
ORDER BY best_eta_dp_date DESC;
```

### Scenario B: Final Destination (FD) Delays
**User Query:** "Show me delayed FD shipments" (or "Check FD delays")
**Logic:**
- Filter: `fd_delayed_dur > 0`
- Date Column: `eta_fd_date` or `best_eta_fd_date`
- Display Protocol: Show container, FD, FD date, and FD delay days.

**DuckDB SQL:**
```sql
SELECT
    container_number,
    final_destination,
    strftime(best_eta_fd_date, '%d-%b-%Y') AS best_eta_fd_date,
    fd_delayed_dur,
    -- shipment_status
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
<!-- - DP Reached: `best_eta_dp_date` is not null **and** `< today`.
- Delivered: `delivery_to_consignee_date` **or** `empty_container_return_date` is not null.
- Not Delivered: If **both** delivery dates are null, then it is **not** delivered (even if DP reached). -->
- DP Reached: `ata_dp_date` is not null.
- Delivered: `delivery_to_consignee_date` **or** `empty_container_return_date` is not null.
- Not Delivered: If **both** delivery dates are null, then it is **not** delivered (even if DP reached).
- Display Protocol: Show container, PO, DP location, DP date, final_destination, delivery/return dates, and status.

**DuckDB SQL:**
```sql
SELECT
    container_number,
    po_numbers,
    discharge_port,
    strftime(ata_dp_date, '%d-%b-%Y') AS best_eta_dp_date,
    final_destination,
    strftime(delivery_to_consignee_date, '%d-%b-%Y') AS delivery_to_consignee_date,
    strftime(empty_container_return_date, '%d-%b-%Y') AS empty_container_return_date,
    shipment_status
FROM df
WHERE ata_dp_date IS NOT NULL
--   AND best_eta_dp_date < CURRENT_DATE
  AND (
      delivery_to_consignee_date IS NOT NULL
      OR empty_container_return_date IS NOT NULL
  )
ORDER BY ata_dp_date DESC;
```

### Scenario E: Next 5-Day PO/Container Schedule (Nashville Example)
**User Query:** "Next 5 day container schedule for Nashville" (or "shipments coming in next 10 days at Savannah")
**Logic:**
- Arrival window based on `best_eta_dp_date`
- Filter: `discharge_port` contains the city AND `ata_dp_date` NULL
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
ORDER BY best_eta_dp_date DESC;
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



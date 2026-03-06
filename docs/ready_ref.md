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
- Date priority for sorting: `best_eta_dp_date` -> `ata_dp_date` -> `eta_dp_date` -> `best_eta_fd_date` -> `eta_fd_date`.
- Apply `.dt.strftime('%d-%b-%Y')` only after sorting.

### Default Capping Policy (When User Gives No Duration)
- Future arrival/delivery intent (`will arrive`, `will be delivered`, `upcoming`, `will going`, `will come`, `will be coming`) must be capped to:
  - `CURRENT_DATE` to `CURRENT_DATE + INTERVAL 30 DAY`
- Past arrived/received/delivered intent (`arrived`, `received`, `delivered`) must be capped to:
  - `CURRENT_DATE - INTERVAL 30 DAY` to `CURRENT_DATE`
- Delay/Early intent without explicit duration must use default threshold:
  - `<= 7 days` for delayed
  - `<= -7 days` for early
- If any default cap/threshold is applied, explicitly mention it in the response note.
- If user provides explicit date range/duration, do not apply these defaults.

### Country Alias Handling (Location)
- If user asks by country without explicit `dp` or `fd`, apply location filter on both:
  - `(discharge_port ILIKE ... OR final_destination ILIKE ...)`
- If user explicitly says `dp`, apply country filter only on `discharge_port`.
- If user explicitly says `fd`, apply country filter only on `final_destination`.
- Alias mapping for common country asks:
  - `AMERICA` / `USA` / `US` / `UNITED STATES` -> use `%(us%`,`%(US%`
  - `CHAINA` / `CHINA` -> use `%(CN%`
  - `EUROPE` -> use common Europe markers like and EU-country code markers such as `%(DE%`, `%(FR%`, `%(NL%`, `%(BE%`, `%(ES%`, `%(IT%`, `%(GB%`

## 1. Reference Scenarios (Operational Queries)

### Scenario A: Delayed Shipments (Discharge Port)
**User Query:** “How many shipments are delayed?” OR
                “Show delayed shipments.” OR
                “Which DP shipments arrived late?”
**Logic:**
- Filter: `ata_dp_date` IS NOT NULL
- Date Column: `best_eta_dp_date` (Format: '%d-%b-%Y')
- Delayed if: `dp_delayed_dur > 0`
- If user provides explicit date range/duration, do not apply defaults.
- If user provides no duration `best_eta_dp_date >=  CURRENT_DATE AND best_eta_dp_date <= INTERVAL(CURRENT_DATE + 30)` Delay default threshold `dp_delayed_dur >= 7`
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
WHERE ata_dp_date IS NOT NULL
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

### Scenario G: Shipment Missed IN-DC: includes delivered-late and overdue-not-delivered (DELIVERED But Missed IN-DC / MISSED PLANNED DELIVERY)
**User Query:** "Which shipments failed planned delivery?" (or "not arrived at FD within IN-DC Date")
**Interpretation Rules:**
- "Delivered at FD": `delivery_to_consignee_date` not null **OR** `empty_container_return_date` not null.
- "Failed/missed IN-DC DATE": `in-dc_date` not null **and** `in-dc_date <= delivery_to_consignee_date` **OR** `in-dc_date <= empty_container_return_date` .
**Logic:**
- Keep only records where in_dc_date is not null
- Shipment has any FD delivery event (either consignee delivery OR empty container return)
- Delivered Late: The actual event occurred on or after the IN‑DC date 
- No FD delivery event yet and IN‑DC date is already passed (overdue)
**DuckDB SQL:**
```sql
SELECT
    container_number,
    po_numbers,
    load_port,
    final_destination,
    strftime("in-dc_date", '%d-%b-%Y') AS in_dc_date,
    strftime(delivery_to_consignee_date, '%d-%b-%Y') AS delivered_date,
    strftime(empty_container_return_date, '%d-%b-%Y') AS container_returned_date,
    CASE
	WHEN delivery_to_consignee_date IS NULL AND empty_container_return_date IS NULL AND "in-dc_date" <= CURRENT_DATE
	THEN 'Overdue - Not Delivered Yet'
	WHEN "in_dc_date" <= COALESCE(delivery_to_consignee_date, empty_container_return_date)
	THEN 'Missed Planned Delivery'
	ELSE 'Aligned As Planned'
    END AS in_dc_status_bucket

FROM df
WHERE "in-dc_date" IS NOT NULL
   AND ( 
		-- overdue and not delivered
		(
		delivery_to_consignee_date IS NULL AND empty_container_return_date IS NULL AND "in-dc_date" <= CURRENT_DATE
		)
  	OR 
		 -- Delivered late by calculating actual available date
		(
            	(delivery_to_consignee_date IS NOT NULL OR empty_container_return_date IS NOT NULL)
            	AND "in-dc_date" <= COALESCE(delivery_to_consignee_date, empty_container_return_date)
        	)

	)

ORDER BY "in-dc_date" ASC;
```



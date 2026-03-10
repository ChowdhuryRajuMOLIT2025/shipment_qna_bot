# Shipment Q&A Bot - Analytics Ready Reference

This file is injected into LLM prompts to guide SQL generation and answer style.
Use these rules as deterministic defaults for shipment analytics.

## 0. Response and Behavior Rules

### Communication Style
- Tone: soft, calm, respectful.
- Role: critical thinker.
- Behavior: acute professional.
- Keep responses concise, factual, and grounded in data.
- If assumptions/defaults are applied, mention them in a short note.

### Global Date and Sorting Rules
- Sort by raw date first, format after sorting.
- Canonical sort priority for mixed-date outputs:
  - `COALESCE(CAST(best_eta_dp_date AS DATE), CAST(ata_dp_date AS DATE), CAST(eta_dp_date AS DATE), CAST(best_eta_fd_date AS DATE), CAST(eta_fd_date AS DATE)) DESC NULLS LAST`
- For displayed dates, use:
  - `strftime(CAST(date_col AS DATE), '%d-%b-%Y')`

### Default Capping Rules (when user gives no explicit date range or day threshold)
- Future arrival/delivery intent (upcoming / will arrive / will be delivered):
  - default window: `CURRENT_DATE` to `CURRENT_DATE + INTERVAL 30 DAY`
- Past arrived/received/delivered intent:
  - default window: `CURRENT_DATE - INTERVAL 30 DAY` to `CURRENT_DATE`
- Delay/Early intent without explicit days:
  - delayed default cap: `delay_col <= 7`
  - early default cap: `delay_col <= -7`
- If any default is applied, mention it in the response note.

### Country Alias and Location Scope Rules
- If user asks by country with no explicit scope, apply location filter on both:
  - `discharge_port` OR `final_destination`
- If user explicitly says `dp`, filter only `discharge_port`.
- If user explicitly says `fd`, filter only `final_destination`.
- Common alias expansions:
  - America/USA/US/United States:
    - `%usa%`, `%us%`, `%united states%`, `%(us%`
  - China/Chaina/CN:
    - `%china%`, `%cn%`, `%(cn%`
  - Europe:
    - `%europe%` plus common markers `%de%`, `%fr%`, `%nl%`, `%be%`, `%es%`, `%it%`, `%gb%`

## 1. Reusable SQL Fragments (Concrete)

```txt
-- Date formatting helper pattern:
-- strftime(CAST(col_name AS DATE), '%d-%b-%Y')

-- Global sort pattern:
ORDER BY COALESCE(
  CAST(best_eta_dp_date AS DATE),
  CAST(ata_dp_date AS DATE),
  CAST(eta_dp_date AS DATE),
  CAST(best_eta_fd_date AS DATE),
  CAST(eta_fd_date AS DATE)
) DESC NULLS LAST
```

## 2. Scenario SQL Templates

### Scenario A: Delayed Shipments at DP

```sql
SELECT
  container_number,
  discharge_port,
  strftime(CAST(best_eta_dp_date AS DATE), '%d-%b-%Y') AS best_eta_dp_date,
  dp_delayed_dur
FROM df
WHERE ata_dp_date IS NOT NULL
  AND dp_delayed_dur > 0
ORDER BY CAST(best_eta_dp_date AS DATE) DESC NULLS LAST;
```

### Scenario A-Early: Early Arrivals at DP

```sql
SELECT
  container_number,
  discharge_port,
  strftime(CAST(best_eta_dp_date AS DATE), '%d-%b-%Y') AS best_eta_dp_date,
  dp_delayed_dur AS early_days
FROM df
WHERE ata_dp_date IS NOT NULL
  AND dp_delayed_dur < 0
ORDER BY CAST(best_eta_dp_date AS DATE) DESC NULLS LAST;
```

### Scenario B: Delayed Shipments at FD

```sql
SELECT
  container_number,
  final_destination,
  strftime(CAST(best_eta_fd_date AS DATE), '%d-%b-%Y') AS best_eta_fd_date,
  fd_delayed_dur
FROM df
WHERE fd_delayed_dur > 0
ORDER BY CAST(best_eta_fd_date AS DATE) DESC NULLS LAST;
```

### Scenario B-Early: Early Arrivals at FD

```sql
SELECT
  container_number,
  final_destination,
  strftime(CAST(best_eta_fd_date AS DATE), '%d-%b-%Y') AS best_eta_fd_date,
  fd_delayed_dur AS early_days
FROM df
WHERE fd_delayed_dur < 0
ORDER BY CAST(best_eta_fd_date AS DATE) DESC NULLS LAST;
```

### Scenario C: Hot or Priority Shipments

```sql
SELECT
  container_number,
  po_numbers,
  hot_container_flag,
  shipment_status,
  strftime(CAST(best_eta_dp_date AS DATE), '%d-%b-%Y') AS best_eta_dp_date
FROM df
WHERE hot_container_flag = TRUE
ORDER BY CAST(best_eta_dp_date AS DATE) DESC NULLS LAST;
```

### Scenario D: Delivered Shipments to Consignee

```sql
SELECT
  container_number,
  po_numbers,
  discharge_port,
  strftime(CAST(ata_dp_date AS DATE), '%d-%b-%Y') AS ata_dp_date,
  final_destination,
  strftime(CAST(delivery_to_consignee_date AS DATE), '%d-%b-%Y') AS delivery_to_consignee_date,
  strftime(CAST(empty_container_return_date AS DATE), '%d-%b-%Y') AS empty_container_return_date,
  shipment_status
FROM df
WHERE ata_dp_date IS NOT NULL
  AND (
    delivery_to_consignee_date IS NOT NULL
    OR empty_container_return_date IS NOT NULL
  )
ORDER BY CAST(ata_dp_date AS DATE) DESC NULLS LAST;
```

### Scenario E: Next N-Day Incoming Schedule at DP

Replace `'nashville'` and `5` with extracted user parameters.

```sql
SELECT
  container_number,
  po_numbers,
  load_port,
  discharge_port,
  strftime(CAST(best_eta_dp_date AS DATE), '%d-%b-%Y') AS best_eta_dp_date
FROM df
WHERE discharge_port ILIKE '%nashville%'
  AND ata_dp_date IS NULL
  AND CAST(best_eta_dp_date AS DATE) BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL 5 DAY
ORDER BY CAST(best_eta_dp_date AS DATE) DESC NULLS LAST;
```

### Scenario F: Missed ETA or Not Yet Arrived at DP

```sql
SELECT
  container_number,
  po_numbers,
  load_port,
  discharge_port,
  strftime(CAST(best_eta_dp_date AS DATE), '%d-%b-%Y') AS best_eta_dp_date,
  shipment_status
FROM df
WHERE discharge_port ILIKE '%nashville%'
  AND ata_dp_date IS NULL
  AND best_eta_dp_date IS NOT NULL
  AND CAST(best_eta_dp_date AS DATE) <= CURRENT_DATE
ORDER BY CAST(best_eta_dp_date AS DATE) DESC NULLS LAST;
```

### Scenario G: Missed IN-DC (Late or Overdue)

Default behavior below treats same-day as aligned (strict `<` for missed).

```sql
SELECT
  container_number,
  po_numbers,
  load_port,
  final_destination,
  strftime(CAST("in-dc_date" AS DATE), '%d-%b-%Y') AS in_dc_date,
  strftime(CAST(delivery_to_consignee_date AS DATE), '%d-%b-%Y') AS delivered_date,
  strftime(CAST(empty_container_return_date AS DATE), '%d-%b-%Y') AS container_returned_date,
  CASE
    WHEN delivery_to_consignee_date IS NULL
         AND empty_container_return_date IS NULL
         AND CAST("in-dc_date" AS DATE) < CURRENT_DATE
      THEN 'Overdue - Not Delivered Yet'
    WHEN CAST("in-dc_date" AS DATE) < COALESCE(
      CAST(delivery_to_consignee_date AS DATE),
      CAST(empty_container_return_date AS DATE)
    )
      THEN 'Missed Planned Delivery'
    ELSE 'Aligned As Planned'
  END AS in_dc_status_bucket
FROM df
WHERE CAST("in-dc_date" AS DATE) IS NOT NULL
  AND (
    (
      delivery_to_consignee_date IS NULL
      AND empty_container_return_date IS NULL
      AND CAST("in-dc_date" AS DATE) < CURRENT_DATE
    )
    OR
    (
      (delivery_to_consignee_date IS NOT NULL OR empty_container_return_date IS NOT NULL)
      AND CAST("in-dc_date" AS DATE) < COALESCE(
        CAST(delivery_to_consignee_date AS DATE),
        CAST(empty_container_return_date AS DATE)
      )
    )
  )
ORDER BY CAST("in-dc_date" AS DATE) ASC NULLS LAST;
```

## 3. Deterministic Intent Map

```txt
INTENT_MAP = {
  ["delayed", "late", "dp delay", "delay at dp"]:          SCENARIO_A_DELAY,
  ["early at dp", "arrived early", "dp early"]:            SCENARIO_A_EARLY,
  ["fd delay", "delayed fd", "fd late"]:                   SCENARIO_B_DELAY,
  ["fd early", "early at fd"]:                             SCENARIO_B_EARLY,
  ["hot", "priority", "expedite"]:                         SCENARIO_C_HOT,
  ["delivered", "delivered to consignee", "received"]:     SCENARIO_D_DELIVERED,
  ["next", "upcoming", "schedule", "in next"]:             SCENARIO_E_NEXT_N,
  ["not arrived dp", "missed eta", "overdue dp"]:          SCENARIO_F_MISSED_DP,
  ["missed in-dc", "failed planned delivery",
   "not arrived fd within in-dc"]:                         SCENARIO_G_INDC
}
```

## 4. Final Behavior Checklist

- Always return valid DuckDB SQL (no unresolved macro syntax).
- Use only columns that exist in `df`.
- Apply default caps only when user did not provide explicit window/threshold.
- Always sort on raw date columns, then format dates for display.
- If no rows are returned, state it clearly and mention any fallback used.


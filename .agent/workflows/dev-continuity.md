---
description: Procedures for maintaining the Shipment QnA Bot logic (IER Framework)
---

# Development Continuity Playbook

Whenever you are tasked with modifying the `shipment_qna_bot` codebase, you MUST follow this IER (Intent-Execution-Result) framework to ensure compatibility.

## 1. PRE-IMPLEMENTATION AUDIT
Before writing any code, verify:
- [ ] Is this search or analytics?
- [ ] If analytics, check `src/shipment_qna_bot/tools/analytics_metadata.py` for column names.
- [ ] If search, check `src/shipment_qna_bot/graph/nodes/retrieve.py` for `_FILTER_FIELDS`.

## 2. THE IER CONSTRAINTS

### INTENT: Classification Guardrails
- Queries with specific Shipment/Container IDs -> **Search Path**.
- Queries requiring aggregations/math -> **Analytics Path**.
- Abrupt topic shifts must trigger a context reset in the `normalizer.py`.

### EXECUTION: Code Generation Rules
- **OData (Search)**: Absolute prohibition of date math (`now()`, `add`).
- **Pandas (Analytics)**: Must use `result = ...` assignment. No global side effects.
- **Delay Logic**: Default to discharge port (`dp_delayed_dur`).

### RESULT: Presentation & Grounding
- All numbers must be grounded in the JSON hits or Pandas dataframes.
- Use markdown tables for multiple-row outputs.

## 3. VERIFICATION PROTOCOL
- Run `pytest tests/test_pandas_flow.py` for analytics changes.
- Run `pytest tests/test_integration_live.py` for retrieval changes.

## 4. OPEN RCA TODO (Added 2026-02-19)

### Issue A: Positive feedback closes session and creates context imbalance
- RCA Evidence:
  - `src/shipment_qna_bot/logs/app.log:5988` rewrote user praise (`"NO! you are working very good"`) to `"thank you! how can i assist you further?"`.
  - `src/shipment_qna_bot/logs/app.log:5994` then classified this as `intent=end`.
  - `src/shipment_qna_bot/logs/app.log:5996` cleared session due end intent.
- Probable Cause:
  - Normalizer is over-rewriting non-task feedback into assistant-like "thank you" text.
  - Intent policy treats "thank you" as end (`src/shipment_qna_bot/graph/nodes/intent.py:142`), and route clears session (`src/shipment_qna_bot/api/routes_chat.py:153`).
- TODO Action Plan:
  - Add non-task/feedback guardrail in normalizer to bypass LLM rewrite for short praise/acknowledgment messages.
  - Add intent safety rule: only map to `end` on explicit close phrases (`bye`, `quit`, `end chat`) or very short farewell-only utterances.
  - Add regression test: praise text must not trigger session clear.
  - Add trace metric for `end` classification source (`raw_text` vs `normalized_question`) and confidence.

### Issue B: Bar chart request returns pivot-style table
- RCA Evidence:
  - `src/shipment_qna_bot/logs/app.log:6108` request explicitly asks for "bar chart".
  - `src/shipment_qna_bot/logs/app.log:6122` generated pandas only builds grouped dataframe (`result = df_grouped`).
  - `src/shipment_qna_bot/logs/app.log:6127` final answer rendered markdown table, no chart payload.
- Probable Cause:
  - Analytics node currently returns text/table only; chart extraction is not implemented (`src/shipment_qna_bot/graph/nodes/analytics_planner.py:390`).
  - API can pass `chart_spec` if present (`src/shipment_qna_bot/api/routes_chat.py:180`), but planner never sets it.
- TODO Action Plan:
  - Introduce chart-intent detection in analytics planner for requests containing `bar|line|pie|chart|graph`.
  - Build deterministic `chart_spec` from grouped dataframe output while preserving existing answer/table response structure.
  - Keep matplotlib/seaborn blocked; chart generation should be declarative JSON (not plotting import).
  - Add regression tests:
    - bar-chart request returns non-null `chart_spec`.
    - table still present for backward compatibility.
    - no change to `ChatAnswer` schema contract.

## 5. MILESTONE CHECKLIST (Chart + Security, Added 2026-02-20)

### Non-Negotiable Contract Lock
- [ ] Keep `ChatAnswer.answer` unchanged as primary text response.
- [ ] Keep existing response envelope keys unchanged (`conversation_id`, `intent`, `answer`, `notices`, `evidence`, `chart`, `table`, `metadata`).
- [ ] Preserve frontend request payload shape (`question`, `conversation_id`, `consignee_codes`).
- [ ] Do not break general/retrieval rendering behavior while adding chart support.

### M0: Baseline + Rollback Anchor
- [ ] Create and record git tag before implementation (`milestone/pre-chart-security-hardening`).
- [ ] Capture baseline metrics from logs: p95 latency, token/cost outliers, analytics failure rate.
- [ ] Store a short baseline report under `docs/` for comparison after rollout.
- Expected impact: Safe rollback path and measurable progress tracking.

### M1: Frontend Chart Rendering (No Contract Change)
- [ ] Add `data.chart` rendering path in `src/shipment_qna_bot/static/index.html`.
- [ ] Keep existing table rendering as fallback when `chart` is null.
- [ ] Maintain current loader/table UX and avoid layout regression.
- [ ] Add lightweight telemetry logs for "chart requested vs chart rendered".
- Expected impact: User-visible charts without changing backend API contracts.

### M2: Deterministic ChartSpec from Analytics
- [ ] Implement chart-intent detection in `src/shipment_qna_bot/graph/nodes/analytics_planner.py`.
- [ ] Generate declarative `chart_spec` only (no matplotlib/seaborn imports).
- [ ] Keep `table_spec` present for backward compatibility.
- [ ] Add guardrails so invalid chart data falls back to table/text only.
- Expected impact: Bar/line/pie requests return renderable chart payload consistently.

### M3: Identity + Scope Hardening (Fail-Closed)
- [ ] Remove unsafe fallback that accepts payload scope when identity is missing in `src/shipment_qna_bot/security/scope.py`.
- [ ] Enforce deny-by-default when identity or registry mapping is absent (except explicit controlled dev override).
- [ ] Add explicit warning/notice for unauthorized scope attempts.
- [ ] Add regression tests in `tests/test_rls.py` for missing-identity denial path.
- Expected impact: Prevents unauthorized data access by payload spoofing.

### M4: Frontend/HTTP Security Hardening
- [ ] Sanitize markdown-rendered HTML before DOM insertion in `src/shipment_qna_bot/static/index.html`.
- [ ] Add secure session/cookie settings in `src/shipment_qna_bot/api/main.py` (HTTPS, httponly, samesite in non-local env).
- [ ] Add CORS/TrustedHost/HTTPS redirect policy by environment.
- [ ] Add response security headers (CSP, X-Content-Type-Options, frame protections).
- Expected impact: Reduces XSS/session abuse risk significantly.

### M5: Analytics Execution Hardening
- [ ] Replace unrestricted `exec` path in `src/shipment_qna_bot/tools/pandas_engine.py` with constrained execution strategy.
- [ ] Restrict builtins and disallow filesystem/network/process access from generated code.
- [ ] Add execution timeout/row limits and explicit abort messages.
- [ ] Add tests for malicious payload attempts (imports, dunder abuse, file/system access).
- Expected impact: Mitigates high-risk remote code execution vector.

### M6: Cost/Performance Guardrails
- [ ] Add token budget controls per request and per conversation.
- [ ] Truncate excessive context payloads before answer generation.
- [ ] Add circuit-breaker behavior for repeated judge retries on same query.
- [ ] Track and alert on extreme token/cost requests in logs.
- Expected impact: Lower latency/cost spikes and better reliability under heavy prompts.

### M7: Validation + Release Gate
- [ ] Run targeted tests:
  - `pytest tests/test_route_chat.py`
  - `pytest tests/test_schemas_chat.py`
  - `pytest tests/test_rls.py`
  - `pytest tests/test_hardening.py`
  - `pytest tests/test_pandas_flow.py`
- [ ] Run manual UAT script: chart request, retrieval request, clarification request, end-session request.
- [ ] Compare post-change metrics against baseline and approve only if no contract break + security gates pass.
- Expected impact: Controlled rollout with measurable quality and safety.

The **full-scale, production-aligned folder structure** we should converge to for the project, with **what each folder/file is responsible for** and **how the flow moves end-to-end**. This structure supports:

* LangGraph orchestration (Corrective/Reflective later)
* RLS/consignee scope (parent → children)
* Session memory (window + slots, later summary)
* Hybrid retrieval (Azure AI Search + embeddings)
* Deterministic analytics (SQL/Pandas) + chart specs
* FastAPI backend + Streamlit demo UI
* ETL/indexing + eval harness + tests

---

## Canonical folder structure (target)

```text
shipment_qna_bot/
├─ pyproject.toml
├─ README.md
├─ .env.example
├─ .gitignore
├─ scripts/
│  ├─ index_upload_jsonl.py
│  ├─ build_index_schema.py
│  └─ seed_eval_set.py
├─ docs/
│  ├─ architecture.md
│  ├─ rls_model.md
│  └─ index_schema.md
├─ src/
│  └─ shipment_qna_bot/
│     ├─ __init__.py
│
│     ├─ config/
│     │  ├─ __init__.py
│     │  └─ settings.py
│     │
│     ├─ logging/
│     │  ├─ __init__.py
│     │  ├─ logger.py
│     │  ├─ graph_tracing.py
│     │  └─ middleware.py
│     │
│     ├─ models/
│     │  ├─ __init__.py
│     │  └─ schemas.py
│     │
│     ├─ security/
│     │  ├─ __init__.py
│     │  ├─ scope.py
│     │  └─ rls.py
│     │
│     ├─ memory/
│     │  ├─ __init__.py
│     │  ├─ schema.py
│     │  └─ store.py
│     │
│     ├─ tools/
│     │  ├─ __init__.py
│     │  ├─ azure_openai_embeddings.py
│     │  ├─ azure_ai_search.py
│     │  ├─ sql/
│     │  │  ├─ __init__.py
│     │  │  ├─ engine.py
│     │  │  └─ executor.py
│     │  └─ analytics/
│     │     ├─ __init__.py
│     │     ├─ plans.py
│     │     ├─ compiler.py
│     │     └─ executor_pd.py
│     │
│     ├─ graph/
│     │  ├─ __init__.py
│     │  ├─ state.py
│     │  ├─ builder.py
│     │  └─ nodes/
│     │     ├─ __init__.py
│     │     ├─ memory_in.py
│     │     ├─ normalizer.py
│     │     ├─ extractor.py
│     │     ├─ intent.py
│     │     ├─ router.py
│     │     ├─ planner.py
│     │     ├─ retrieve.py
│     │     ├─ handlers/
│     │     │  ├─ __init__.py
│     │     │  ├─ status.py
│     │     │  ├─ eta_window.py
│     │     │  ├─ delay_reason.py
│     │     │  └─ route.py
│     │     ├─ analytics.py
│     │     ├─ judge.py
│     │     ├─ refine.py
│     │     ├─ memory_out.py
│     │     └─ formatter.py
│     │
│     ├─ api/
│     │  ├─ __init__.py
│     │  ├─ main.py
│     │  ├─ dependencies.py
│     │  └─ routes/
│     │     ├─ __init__.py
│     │     └─ chat.py
│     │
│     └─ ui/
│        ├─ __init__.py
│        └─ streamlit_app.py
│
└─ tests/
   ├─ test_schema.py
   ├─ test_scope_rules.py
   ├─ test_rls_filter.py
   ├─ test_graph_paths.py
   ├─ test_eta_logic.py
   └─ test_analytics_plans.py
```

---

# What each major part does (in plain language)

## 1) `api/` — FastAPI entrypoint (the “product boundary”)

* **main.py**: creates FastAPI app, adds middleware, registers `/api/chat`
* **routes/chat.py**: validates request (`schemas.ChatRequest`), injects scope, calls LangGraph, returns `ChatResponse`

**Rule:** API layer does not contain business logic. It only:

* validates input
* injects auth/scope
* calls graph
* returns response

---

## 2) `graph/` — LangGraph orchestration (the “brain”)

### `graph/state.py`

Single source of truth for everything carried between nodes:

* question, normalized_question
* extracted identifiers
* consignee scope
* retrieval plan + hits
* notices/errors
* final answer + evidence + chart specs

### `graph/builder.py`

Defines node wiring: the workflow itself.

### `graph/nodes/`

Each node does one job. No node should be “god node”.

---

## 3) `security/` — RLS you can trust (no leaks)

* **scope.py**: implements parent-child hierarchy enforcement
* **rls.py**: builds Azure Search filter strings *only from allowed scope*

**Rule:** Never trust payload scope blindly. Scope comes from auth/middleware.

---

## 4) `tools/` — talking to outside world (Search/OpenAI/SQL/Pandas)

* **azure_ai_search.py**: handles hybrid search + ALWAYS applies RLS filter
* **azure_openai_embeddings.py**: generates vectors only
* **tools/sql/**: SQLAlchemy engine + executor (if/when you have a mirror)
* **tools/analytics/**: safe “Plan → compile → execute” framework for charts/tables

**Rule:** Tools should be deterministic and testable. No hidden global state logic.

---

## 5) `memory/` — session continuity

* **store.py**: get/set per `conversation_id` (in-memory now; Redis later)
* **schema.py**: defines what we store:

  * last N messages
  * sticky slots (container/PO/OBL)
  * summary later

**Rule:** Memory stores *context*, not shipment facts. Facts come from retrieval.

---

## 6) `ui/streamlit_app.py` — demo UI (not production)

* calls FastAPI `/api/chat`
* shows answer + citations
* renders charts/tables if present

Later your .NET app will do the same: call the FastAPI service.

---

## 7) `scripts/` — ETL + index provisioning

* build index schema
* upload jsonl docs
* compute embeddings (if ingest-time embeddings)
* seed eval datasets

---

## 8) `tests/` — make it bulletproof

* schema normalization tests (consignee parsing)
* RLS filter tests (parent can see children; child can’t see siblings)
* graph path tests (node wiring)
* deterministic ETA logic tests
* analytics plan safety tests

---

# End-to-end flow (how a question becomes an answer)

1. **FastAPI `/api/chat`**

   * validate request (question + consignee payload)
   * resolve and enforce allowed scope (parent/child)
   * generate `conversation_id` if missing
   * build initial GraphState

2. **LangGraph**

   1. `memory_in`: load previous context/slots for conversation_id
   2. `normalizer`: normalize question text
   3. `extractor`: extract container/PO/OBL + parse time window
   4. `intent`: pick primary intent (+ optional sub-intents later)
   5. `router`: choose path:

      * retrieval handlers (status/eta/delay/route)
      * analytics path (charts)
   6. `planner`: build retrieval plan
   7. `retrieve`: call Azure Search with RLS filter
   8. `handlers/*`: deterministic logic using retrieved docs/metadata
   9. `judge/refine` (later): corrective/reflective loop if answer not grounded
   10. `memory_out`: store updated window + slots
   11. `formatter`: final response (answer + evidence + table/chart spec)

3. **Streamlit**

   * displays response and renders optional charts/tables

---

# Why this structure is “rightful”

* It prevents spaghetti.
* It isolates security and tools.
* It makes testing possible.
* I add analytics and memory without polluting the graph logic.
* It keeps on the critical path: **RLS + retrieval + deterministic answers**.

---

If you want the next step, we should implement **memory/** first (store + memory_in/out nodes) because it touches minimal code and immediately improves UX without risking hallucination.

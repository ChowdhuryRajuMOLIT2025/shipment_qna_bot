# Shipment Q&A Chatbot (Hardened Edition)

A high-performance, security-first Shipment Q&A system built with **LangGraph**, **FastAPI**, and **Azure AI Search**. This version features advanced "Bring Your Own Data" (BYOD) analytics over DuckDB with scoped SQL execution.

---

## 🏗️ Architecture Overview

The system utilizes a multi-agent orchestration pattern via **LangGraph** to handle complex logistics queries:

- **Intent Detection**: Advanced classification with praise-guardrails to maintain session continuity.
- **Hybrid Retrieval**: BM25 and Vector search integration via Azure AI Search with enforced Row-Level Security (RLS).
- **Hardened Analytics (BYOD)**: Dynamic analysis of Parquet/CSV datasets using DuckDB-backed SQL execution over scoped shipment data.
- **Response Synthesis**: Context-aware answering with integrated data visualization (Bar/Line charts).

## 🔒 Security Posture

- **Scoped DuckDB Analytics**: Analytics run only as DuckDB SQL against an authorized `df` view, removing the old Python code-execution path and keeping queries inside the shipment dataset boundary.
- **Identity Awareness**: Flexible identity scope resolution designed for VPN/Firewall deployments, balancing infrastructure-level trust with application-level authorization.
- **Secure API**: Hardened FastAPI implementation with CSP, HSTS, and Frame projection headers.
- **Persistent Sessions**: Reliable session management using environment-backed encryption keys.

## 📂 Project Structure

```text
shipment_qna_bot/
├── .agent/workflows/       # Agentic development continuity & RCA logs
├── data/                    # Local dataset samples (Parquet/CSV)
├── src/shipment_qna_bot/
│   ├── api/                 # FastAPI routes and middleware
│   ├── graph/               # LangGraph state machine & node logic
│   │   ├── nodes/           # Intent, Retrieval, Analytics, Answer nodes
│   ├── security/            # RLS & Scope resolution logic
│   ├── tools/               # Azure Search, DuckDB analytics, OpenAI clients
│   ├── logging/             # Structured JSON observability
│   └── models/              # Pydantic schemas and state definitions
├── tests/                   # Security, Logic, and Performance test suites
└── requirements.txt         # Project dependencies
```

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- Azure AI Search Service
- Azure OpenAI / OpenAI API Key

### Installation & Execution
1. **Clone & Install**:
   ```bash
   pip install -r requirements.txt
   ```
2. **Environment Setup**:
   Configure `.env` with required Azure/OpenAI credentials and `SESSION_SECRET_KEY`.
3. **Run Server**:
   ```bash
   uv run uvicorn shipment_qna_bot.api.main:app --reload --host=127.0.0.1 --port=8000
   ```

## 🛠️ Development & Support
The project uses automated formatting and linting:
- **Formatter**: `black`
- **Import Sort**: `isort`
- **Linting**: `flake8` / `pylint` (recommended)

---
*Maintained for MOLIT Shipments Project.*

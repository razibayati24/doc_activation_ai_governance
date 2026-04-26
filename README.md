# AT&T FinOps AI Governance Demo

A Databricks demo for **AI vendor governance and FinOps** — built on the Document Activation reference architecture (Lakeflow medallion + Genie + Knowledge Assistant + Multi-Agent Supervisor + a Streamlit App), adapted for AT&T's enterprise AI portfolio.

The demo answers two complementary classes of question over the same domain:

- **Structured analytics** — "Which contracts are over committed spend?", "Total AI spend by vendor and BU", "Which contracts are missing an AI Governance Addendum?" → routed to **Genie**.
- **Document language Q&A** — "What does the OpenAI agreement say about training data?", "Does Anthropic permit our customer data for model training?" → routed to a **Knowledge Assistant** indexing 15 vendor contract PDFs.

Two specialist agents are wrapped by a **Multi-Agent Supervisor** that performs find → understand → act routing, and the whole thing is fronted by a Databricks App with **per-user OBO authentication** so column-level masking on system audit data can be demonstrated live (one persona sees real data, another sees redacted PII — same query, same view).

---

## Architecture

```
                  Browser (razi.bayati  OR  daniel.perez)
                                │
                                │  user OAuth token (X-Forwarded-Access-Token)
                                ▼
                    Databricks App: att-finops-ai-gov
                       (Streamlit, OBO enabled)
                                │
                                ▼
                MAS endpoint: mas-c39464e9-endpoint
                   (Multi-Agent Supervisor)
                  ┌─────────────┴─────────────┐
                  ▼                           ▼
       finops_explorer              contract_analyst
       (Genie Space)                (Knowledge Assistant)
                  │                           │
                  ▼                           ▼
       Gold/Silver tables             15 vendor contract PDFs
       in cmegdemos_catalog.          in /Volumes/.../ai_contracts/
       ai_governance_doc_intelligence
```

Per-user identity propagates all the way to SQL: the Genie space is `run_as=VIEWER`, so when daniel asks a question through the app, the SQL on the masked view runs as `daniel.perez@databricks.com` and the column-mask UDFs return redacted values. Razi gets the real ones.

---

## Components

| Layer | What lives here |
|---|---|
| **`pipeline/`** | Lakeflow Spark Declarative Pipeline. 2 bronze streaming tables, 3 silver materialized views, 3 gold materialized views. SQL only. Asset Bundle config in `databricks.yml` + `resources/pipeline.pipeline.yml`. |
| **`scripts/`** | `load_bronze_data.py` — embeds the canonical bronze CSVs (services, contracts, spend transactions, extracted contracts) and uploads them to a UC Volume. `generate_pdfs_local.py` — manifest-driven PDF generator that calls the Databricks Foundation Model API to produce realistic AI vendor contracts. |
| **`config/`** | `demo_manifest.json` — single source of truth for all 15 contracts. `genie_config.json` — Genie space tables and starter questions. `app_config.json` — branding, demo paths, agent labels for the Streamlit app. |
| **`app/`** | Streamlit app (`app.py`) that calls the MAS endpoint with the viewer's OBO token. `app.yaml` declares `user_api_scopes` for Serving, Genie, and SQL. |
| **`mcp-server/`** | Reference Salesforce MCP server (Phase 5, optional). Not deployed in this build. |
| **`docs/`** | Extensibility guide, customization guide, MCP guide. |

---

## Demo data

15 synthetic AI vendor agreements between AT&T business units and major AI vendors:

| | | |
|-|-|-|
| **OpenAI** GPT-4 Turbo Enterprise | **Anthropic** Claude 3 Opus | **Google Cloud** Gemini 1.5 Pro |
| **AWS** Bedrock Model Suite | **NVIDIA** DGX Cloud H100 Reserved | **Databricks** Mosaic AI |
| **Snowflake** Cortex | **Scale AI** GenAI Data Engine | **Cohere** Command R+ |
| **Labelbox** Model Foundry | **DataRobot** AutoML | **Palantir** Foundry AIP |
| **Hugging Face** Enterprise Hub | + secondary BU contracts | |

Each contract has structured commercial terms (committed spend, overage rates, term, BU, regions) AND clause-level governance flags (data residency, training-data restriction, AI governance addendum, MFN, sublicensing, output ownership) plus realistic compliance certifications. The OpenAI GPT-4 Turbo Enterprise contract (AI-0001) is the **demo anchor** — expiring soon, trending over committed spend, ambiguous training-data language, missing governance addendum.

---

## Two-persona masking sub-demo

`v_ai_access_audit` is a UC view over `system.access.audit` filtered to AI services (Genie, Vector Search, Model Serving, Agent Framework, MLflow Trace, Feature Store), with **inline column masks** on user email, display name, and source IP.

The mask UDFs (`mask_email`, `mask_name`, `mask_ip`) return the real value when `current_user() = 'razi.bayati@databricks.com'` and a redaction otherwise. With OBO enabled in the app, the same question through the same app yields different results for different users — driven entirely by Unity Catalog identity, not application logic.

---

## Build sequence (high-level)

1. Create catalog/schema/volume.
2. Generate bronze CSVs from `demo_manifest.json` (or use the embedded copies in `scripts/load_bronze_data.py`).
3. Generate 15 contract PDFs with `scripts/generate_pdfs_local.py` (or the equivalent notebook).
4. Deploy and run the Lakeflow pipeline. Verifies all 8 tables.
5. Create the Genie Space pointing at the gold + silver tables.
6. Create the Knowledge Assistant indexing the contract PDFs.
7. Create the Multi-Agent Supervisor wrapping both.
8. Deploy the Streamlit App. Set `MAS_ENDPOINT_NAME` env var. Set `user_api_scopes` for OBO. Grant the App SP and end users the right UC + endpoint permissions.

The `CLAUDE.md` file walks through the canonical version of these steps for the original RightsIQ template; this repo is the AT&T-adapted output of that workflow.

---

## Adapting this template

`docs/document_activation_extensibility_guide.pdf` walks through the 3-phase process for adapting the demo to any vertical:

- **Phase 1**: Define 5–6 demo story questions for your customer's pain point.
- **Phase 2**: Classify each question as Structured (Genie) vs Language (KA), and design 3 gold tables.
- **Phase 3**: Map your column names back to the reference schema.

This repo is the result of running that process for AT&T FinOps AI Governance — the output to look at if you want a worked example beyond the original RightsIQ media-licensing reference.

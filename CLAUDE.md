# CMEG Document Activation Demo

## Conventions

This project follows the `databricks-demo` skill conventions:

- **DEMO.md** — Brand guidelines, architecture, component inventory. Read it first for context.
- **TASKS.md** — Living checklist. **Update it as you complete each task.** If re-entering this project, read TASKS.md first to determine where to resume.
- **CLAUDE.md** — This file. Detailed execution instructions for each phase.

**Before starting**: Load relevant skills — `databricks-demo`, `databricks-agent-bricks`, `databricks-genie`, `databricks-unstructured-pdf-generation`, `databricks-synthetic-data-generation`, `databricks-spark-declarative-pipelines`, `databricks-resource-deployment`, `databricks-app-apx` (or `databricks-app-python`). Their instructions contain critical details.

---

## Step 0: Configuration (Ask the User)

Before building anything, **ask the user** for the following and record the answers in TASKS.md:

1. **Workspace Host**: The Databricks workspace URL (e.g., `https://my-workspace.cloud.databricks.com`)
2. **CLI Profile**: The Databricks CLI profile name to use
3. **Catalog**: Which Unity Catalog to use? They can pick an existing one or have you create a new one.
4. **Schema**: Which schema within that catalog? Same — existing or new.
5. **Volume**: Which volume for raw data (PDFs, CSVs)? Default suggestion: `raw_data` within their chosen catalog/schema.

If they want new assets created, use the `databricks-unity-catalog` skill or `execute_sql` to create them:
```sql
CREATE CATALOG IF NOT EXISTS <catalog>;
CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>;
CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.<volume> COMMENT 'Raw data for Document Activation demo';
```

**After getting these values, replace ALL `REPLACE_WITH_` placeholders** across the repo:

| Placeholder | Replace With |
|-------------|-------------|
| `REPLACE_WITH_WORKSPACE_HOST` | The workspace URL |
| `REPLACE_WITH_PROFILE` | The CLI profile name |
| `REPLACE_WITH_CATALOG` | The catalog name |
| `REPLACE_WITH_SCHEMA` | The schema name |
| `REPLACE_WITH_VOLUME` | The volume name |
| `REPLACE_WITH_VOLUME_PATH` | `/Volumes/<catalog>/<schema>/<volume>` |
| `REPLACE_WITH_MAS_ENDPOINT` | Set after Phase 4 |
| `REPLACE_WITH_MCP_SERVER_URL` | Set after Phase 5 (optional) |

Files that contain placeholders:
- `pipeline/databricks.yml`
- `pipeline/resources/pipeline.pipeline.yml`
- `pipeline/src/transformations/pipeline.sql`
- `config/genie_config.json`
- `scripts/generate_pdfs_local.py`
- `scripts/load_bronze_data.py`
- `app/app.py`
- `app/app.yaml`

Throughout this file, these are referenced as `${CATALOG}`, `${SCHEMA}`, `${VOLUME}`, `${PROFILE}`.

---

## Overview

Build a complete **Document Activation** demo for Databricks. The reference implementation is **RightsIQ** (media licensing rights management), but the architecture is extensible to any CMEG vertical. See `docs/extensibility_guide.pdf` for how to customize.

| Phase | Name | Databricks Products | Est. Duration |
|-------|------|---------------------|---------------|
| 1A | **Generate PDFs** | `generate_pdf_documents` MCP tool or local script | 5-10 min |
| 1B | **Upload Bronze Data** | `run_python_file_on_databricks` MCP tool | 2-5 min |
| 1C | **Lakeflow Pipeline** | Spark Declarative Pipelines (Asset Bundles) | 5-15 min |
| 2 | **Genie Space** | Genie Space | ~30 sec |
| 3 | **Knowledge Assistant** | Knowledge Assistant (Agent Brick) | ~30 sec + 2-5 min provisioning |
| 4 | **Supervisor Agent** | Supervisor Agent (Agent Brick) | ~30 sec + 2-5 min provisioning |
| 5 | **MCP Server** (optional) | Custom MCP Server → Salesforce | Manual setup |

**Total estimated build time: 20-40 minutes.**

## Tool & Python Guidelines

- **Always use `uv`** for Python — never `pip` or standalone `python`.
- **Always use serverless** unless told otherwise. Client version 4+ for Python 3.12.
- **Always use Unity Catalog** with 3-layer namespaces. Never Hive Metastore.
- **Use UC Volumes** (preferred) or S3. Never DBFS.
- **Write and test code locally first**, then deploy.

## Required MCP Tools

From `databricks-ai-dev-kit`:
- `generate_pdf_documents` — Phase 1A (alternative: `scripts/generate_pdfs_local.py`)
- `run_python_file_on_databricks`, `execute_databricks_command` — Phase 1B
- `get_table_details` — Phase 2
- `create_or_update_genie`, `ask_genie`, `find_genie_by_name` — Phase 2
- `manage_ka` — Phase 3
- `manage_mas` — Phase 4

---

## Execution Strategy

Phases are mostly sequential, but there is one parallelism opportunity:

```
Phase 1A (PDFs) ──────────────────────────────┐
                                               ├── Phase 3 (KA) ──┐
Phase 1B (Data) → Phase 1C (Lakeflow) ──┐     │                   │
                                         ├── Phase 2 (Genie) ─────├── Phase 4 (MAS) → App
                                         │                         │
                                         └─────────────────────────┘
```

- **Phase 2** needs gold/silver tables (depends on 1C)
- **Phase 3** needs PDFs (depends on 1A only — NOT on 1B/1C)
- **Phase 4** needs both Genie space_id (from Phase 2) AND KA tile_id (from Phase 3)

**Handling long waits**:
- After creating a KA or MAS, **don't block** — move to the next independent task while it provisions.
- Poll provisioning status with `manage_ka(action="get")` or `manage_mas(action="get")` before Phase 4 / App.
- Inform the user of expected wait times at each step.
- **Always update TASKS.md** after completing each step so progress is never lost if context runs long.

---

## Phase 1: Data Generation & Pipeline

### Step 1A: Generate Document PDFs

**Option A — MCP Tool** (if `generate_pdf_documents` is available):

Use `generate_pdf_documents` with:
- `catalog`: "${CATALOG}"
- `schema`: "${SCHEMA}"
- `description`: (from `config/extraction_schema.json` → `pdf_generation.description`)
- `count`: 15
- `volume`: "${VOLUME}"
- `folder`: "rights_contracts"
- `doc_size`: "MEDIUM"
- `overwrite_folder`: true

**Option B — Local Script** (if MCP tool is unavailable):

```bash
DATABRICKS_CONFIG_PROFILE=${PROFILE} python scripts/generate_pdfs_local.py
```

This script reads `config/demo_manifest.json`, calls the Databricks Foundation Models API to generate legal prose for each contract, renders PDFs, and uploads to the UC Volume.

Output: PDFs in `/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/rights_contracts/`

**→ Update TASKS.md Phase 1A when complete.**

### Step 1B: Upload Bronze Data

The script `scripts/load_bronze_data.py` reads the pre-curated CSV files from the manifest and uploads them to the UC Volume as CSVs that the Lakeflow pipeline ingests.

**Before running, update `CATALOG`, `SCHEMA`, and `VOLUME` variables** in the script to match the user's values.

Execute via `run_python_file_on_databricks`:
- `file_path`: "scripts/load_bronze_data.py"

Output: CSVs in `/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/bronze_data/`

**→ Update TASKS.md Phase 1B when complete.**

### Step 1C: Lakeflow Pipeline (Bronze → Silver → Gold)

The pipeline SQL is in `pipeline/src/transformations/pipeline.sql`. **Before deploying, ensure the `REPLACE_WITH_VOLUME_PATH` placeholder has been replaced** with the actual volume path.

Deploy with Asset Bundles:
```bash
cd pipeline
databricks bundle deploy --profile=${PROFILE}
databricks bundle run document_activation_pipeline --profile=${PROFILE}
```

**This step takes 5-15 minutes.** Inform the user.

The pipeline creates:
- **Silver**: `silver_titles`, `silver_rights_agreements`, `silver_royalty_transactions` (dedup, standardize, validate)
- **Gold**: `gold_rights_by_title_territory`, `gold_royalty_summary`, `gold_territory_overview` (business-grain aggregations)

**→ Update TASKS.md Phase 1C when complete.**

---

## Phase 2: Genie Space

### Step 2A: Inspect Gold Tables

Use `get_table_details` for each gold table to confirm schemas.

### Step 2B: Create the Genie Space

Use `create_or_update_genie`. Load `config/genie_config.json` for tables and sample questions. Ensure catalog/schema placeholders have been replaced.

**Save the returned `space_id`** → record in TASKS.md.

**→ Update TASKS.md Phase 2 when complete.**

---

## Phase 3: Knowledge Assistant

Use `manage_ka` with `action="create_or_update"`:
- `name`: Name from `config/app_config.json` (default: "RightsIQ Contract Analyst")
- `volume_path`: "/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/rights_contracts"
- `description`: "Answers detailed questions about document clauses, terms, conditions, and obligations by searching through the actual documents."
- `instructions`: "You are a document analyst. When answering questions: 1) Always cite the specific document and section. 2) If a question involves financial terms, provide exact numbers from the document. 3) If multiple documents are relevant, compare them. 4) If the information isn't in the documents, clearly state that. 5) Use precise terminology."
- `add_examples_from_volume`: true

**KA creation is fast (~30 sec) but the endpoint takes 2-5 minutes to provision.** After creating, immediately move to Phase 2 if it's not done yet.

**IMPORTANT**: The KA volume must contain **only PDFs**. JSON or other files cause the KA to get stuck in KNOWLEDGE_SOURCE_STATE_UPDATING indefinitely.

Poll status: `manage_ka(action="get", tile_id="<tile_id>")`

**Save `tile_id` and endpoint name** (`ka-<tile_id>-endpoint`) → record in TASKS.md.

**→ Update TASKS.md Phase 3 when complete.**

---

## Phase 4: Supervisor Agent

**Pre-check**: Confirm KA endpoint is ONLINE. If still PROVISIONING, wait and poll.

Use `manage_mas` with `action="create_or_update"`:
- `name`: "Document Activation Supervisor" (or from app_config.json)
- `agents`:
  ```json
  [
    {
      "name": "document_analyst",
      "ka_tile_id": "<tile_id from Phase 3>",
      "description": "Answers questions about specific document clauses, terms, conditions, and obligations by searching through the actual documents."
    },
    {
      "name": "data_explorer",
      "genie_space_id": "<space_id from Phase 2>",
      "description": "Runs SQL analytics on structured data: counts, totals, comparisons, trends, and rankings across the portfolio."
    }
  ]
  ```
- `description`: "Unified assistant that can both analyze documents and query structured data."
- `instructions`: |
    Route queries as follows:
    - Questions about specific document language, clauses, terms, obligations, or what a particular document says → document_analyst
    - Questions about data aggregations, counts, totals, comparisons, trends, or analytics → data_explorer
    - Questions about specific financial obligations in a document → document_analyst
    - Questions about portfolio-wide financial metrics → data_explorer

    If a query could benefit from both agents, start with the most relevant one.
    If unclear, ask the user to clarify whether they want document-specific details or portfolio analytics.

**IMPORTANT**: Always explicitly pass ALL agents in every `manage_mas` call — silent drops occur if only a subset is passed during updates.

**MAS takes 2-5 minutes to provision.** While waiting, prep the App.

**Save `tile_id` and endpoint name** (`mas-<tile_id>-endpoint`) → record in TASKS.md.

**→ Update TASKS.md Phase 4 when complete.**

---

## Databricks App (Demo UI)

The app lives in `app/`. Before deploying:

1. **Update `app/app.yaml`** — set `MAS_ENDPOINT_NAME` to the actual endpoint from Phase 4
2. **Update `app/app.py`** — if not relying on the env var, set the endpoint name directly
3. **Update `config/app_config.json`** — if customizing for a different vertical, update title, agents, and demo paths

### Deployment

```bash
databricks apps deploy <app-name> \
  --source-code-path /Workspace/Users/<user>/<app-name> \
  --profile=${PROFILE}
```

Register the MAS endpoint as an app resource:
```bash
databricks apps update <app-name> --json '{
  "resources": [{
    "name": "supervisor_agent",
    "serving_endpoint": {
      "name": "mas-<tile_id>-endpoint",
      "permission": "CAN_QUERY"
    }
  }]
}'
```

**→ Update TASKS.md App section when complete.**

---

## Phase 5: MCP Server — Salesforce Integration (Optional)

> See `docs/phase5_mcp_guide.md` for setup instructions.

This phase adds a custom MCP Server that bridges the Supervisor Agent to Salesforce for write-back actions (flag renewals, log payments, search accounts).

**Note**: The external-mcp-server agent type is accepted in MAS tile config but silently dropped at inference time. The workaround is client-side MCP routing in the Streamlit app — the app detects action intent, calls the MCP server directly, and handles confirmation before executing.

---

## Key Learnings & Gotchas

1. **Data alignment is critical**: PDFs, bronze CSVs, and KA must all reference the same synthetic data. Use `config/demo_manifest.json` as the single source of truth.
2. **OAuth vs. PAT**: Databricks Apps sit behind an OAuth gateway that rejects workspace PATs. Use `Config()` per call, never a module-level cached instance.
3. **SP permissions**: App service principals require CAN_QUERY on the MAS endpoint, CAN_USE on the SQL warehouse, and Unity Catalog grants (USE CATALOG, USE SCHEMA, SELECT on all tables, READ VOLUME).
4. **Diagnostic pattern**: Call the MAS endpoint directly with a personal token first; if that works but the app fails, the issue is always SP permissions on downstream resources.
5. **KA hygiene**: The KA volume must contain only PDFs — JSON or other files cause indefinite indexing.
6. **manage_mas calls**: Always explicitly pass ALL agents in every call — silent drops occur if only a subset is passed during updates.
7. **tile_id changes**: Recreating a KA generates a new tile_id, requiring MAS updates.

---

## Post-Build

1. Update DEMO.md Key IDs table with all recorded IDs
2. Run end-to-end test through the app UI
3. Mark all TASKS.md items as complete
4. Note any workspace-specific issues in TASKS.md Notes section

# Document Activation Demo — Task Tracker

> **Instructions for Claude**: Update this file as you complete each task. Check off completed items, record IDs/outputs, and add new tasks if needed. If re-entering this project after a crash or restart, **read this file first** to determine where to resume.

---

## Step 0: Configuration

- [ ] Ask user for workspace host and CLI profile
- [ ] Ask user for catalog (existing or create new)
- [ ] Ask user for schema (existing or create new)
- [ ] Ask user for volume name (default: `raw_data`, existing or create new)
- [ ] Create catalog/schema/volume if needed via `execute_sql` or UC tools
- [ ] Replace all `REPLACE_WITH_` placeholders in repo files with actual values

| Setting | Value |
|---------|-------|
| Workspace Host | `___` |
| CLI Profile | `___` |
| Catalog | `___` |
| Schema | `___` |
| Volume | `___` |

## Pre-flight

- [ ] Verify Databricks CLI authenticated to workspace
- [ ] Verify ai-dev-kit MCP tools available
- [ ] Load relevant skills

---

## Phase 1A: Generate Contract PDFs (~5-10 min)

- [ ] Run `scripts/generate_pdfs_local.py` (or use `generate_pdf_documents` MCP tool)
- [ ] Verify PDFs in `/Volumes/<catalog>/<schema>/<volume>/rights_contracts/`

**Output**: PDFs at `/Volumes/___/___/___/rights_contracts/`

## Phase 1B: Upload Bronze Data (~2-5 min)

- [ ] Update `scripts/load_bronze_data.py` with actual catalog/schema/volume values
- [ ] Execute via `run_python_file_on_databricks`
- [ ] Verify bronze CSVs uploaded to volume

**Output**: CSVs at `/Volumes/___/___/___/bronze_data/`

## Phase 1C: Lakeflow Pipeline (~5-15 min)

- [ ] Update pipeline SQL with actual volume path
- [ ] Deploy pipeline: `databricks bundle deploy --profile=<profile>`
- [ ] Run pipeline: `databricks bundle run document_activation_pipeline --profile=<profile>`
- [ ] Verify silver tables (3)
- [ ] Verify gold tables (3)

**Output**: Lakeflow pipeline ID = `___`

---

## Phase 2: Genie Space (~30 sec)

- [ ] Inspect gold table schemas with `get_table_details`
- [ ] Create Genie Space via `create_or_update_genie`
- [ ] Record Genie Space ID: `___`

**Output**: Genie Space ID = `___`

## Phase 3: Knowledge Assistant (~30 sec create + 2-5 min provisioning)

- [ ] Create KA via `manage_ka`
- [ ] Record KA Tile ID: `___`
- [ ] Record KA Endpoint Name: `___`
- [ ] Verify KA endpoint is ONLINE

**Output**: KA Tile ID = `___`, Endpoint = `___`

---

## Phase 4: Supervisor Agent (~30 sec create + 2-5 min provisioning)

- [ ] Confirm KA endpoint is ONLINE
- [ ] Create MAS via `manage_mas` with Genie space_id + KA tile_id
- [ ] Record MAS Tile ID: `___`
- [ ] Record MAS Endpoint Name: `___`
- [ ] Verify MAS endpoint is ONLINE

**Output**: MAS Tile ID = `___`, Endpoint = `___`

---

## Databricks App

- [ ] Update `app/app.yaml` with MAS endpoint name
- [ ] Update `app/app.py` with MAS endpoint name (or rely on env var)
- [ ] Deploy app to workspace
- [ ] Register MAS endpoint as app resource
- [ ] Record App URL: `___`

**Output**: App URL = `___`

---

## Phase 5: MCP Server (Optional — Salesforce Integration)

- [ ] Configure Salesforce Connected App (see `docs/phase5_mcp_guide.md`)
- [ ] Deploy MCP server as Databricks App
- [ ] Update app to point to MCP server URL

---

## Post-Build

- [ ] Update DEMO.md Key IDs table with all recorded IDs
- [ ] Run end-to-end test through the app UI
- [ ] Mark all items complete

---

## Notes

_(Record any workspace-specific issues or workarounds here)_

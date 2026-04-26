# Document Activation Demo

## Brand Guidelines

| Element | Value |
|---------|-------|
| Demo Name | _(Set in config/app_config.json)_ |
| Tagline | _(Set in config/app_config.json)_ |
| Primary Color | #1B3A5C (Databricks dark blue) |
| Accent Color | #FF3621 (Databricks red) |
| Font | Inter / system default |
| Tone | Professional, analytics-forward, enterprise |

## Use Case

The reference implementation is **RightsIQ** — media licensing rights management. The architecture is extensible to any CMEG vertical with bilateral commercial agreements (ad sales, gaming IP licensing, telecom wholesale, etc.). See `docs/extensibility_guide.pdf` for how to adapt.

## Architecture

```
                         ┌──────────────────────────────────┐
                         │        Databricks App UI          │
                         │       (Streamlit Chat Interface)  │
                         └──────────────┬───────────────────┘
                                        │
                         ┌──────────────▼───────────────────┐
                         │       Supervisor Agent             │
                         │     (Multi-Agent Supervisor)       │
                         └──┬──────────────────────────┬────┘
                            │                          │
               ┌────────────▼──────────┐  ┌───────────▼────────────┐
               │   Document Analyst     │  │    Data Explorer         │
               │  (Knowledge Assistant) │  │    (Genie Space)        │
               └────────────┬──────────┘  └───────────┬────────────┘
                            │                          │
                   Document PDFs              Lakeflow Pipeline
                   (UC Volume)             (Bronze → Silver → Gold)
```

## Components

| Component | Databricks Product | Phase |
|-----------|-------------------|-------|
| Synthetic document PDFs | `generate_pdf_documents` / local script | Phase 1A |
| Bronze data (CSVs) | `load_bronze_data.py` → UC Volume | Phase 1B |
| Medallion pipeline | Lakeflow Spark Declarative Pipelines | Phase 1C |
| Natural language SQL | Genie Space | Phase 2 |
| Document Q&A | Knowledge Assistant (Agent Brick) | Phase 3 |
| Multi-agent orchestration | Supervisor Agent (Agent Brick) | Phase 4 |
| External system write-back | Custom MCP Server → Salesforce (optional) | Phase 5 |
| Chat UI | Databricks App (Streamlit) | App |

## Data Location (User-Specified)

| Asset | Location |
|-------|----------|
| Catalog | `REPLACE_WITH_CATALOG` |
| Schema | `REPLACE_WITH_SCHEMA` |
| Volume | `REPLACE_WITH_VOLUME` |
| Document PDFs | `/Volumes/<catalog>/<schema>/<volume>/rights_contracts/` |
| Bronze data | `/Volumes/<catalog>/<schema>/<volume>/bronze_data/` |
| Bronze tables | `<catalog>.<schema>.bronze_*` |
| Silver tables | `<catalog>.<schema>.silver_*` |
| Gold tables | `<catalog>.<schema>.gold_*` |

## Key IDs (Updated During Build)

| Asset | ID | Status |
|-------|----|--------|
| Catalog | `___` | |
| Schema | `___` | |
| Volume | `___` | |
| Lakeflow Pipeline ID | `___` | |
| Genie Space ID | `___` | |
| KA Tile ID | `___` | |
| KA Endpoint Name | `___` | |
| MAS Tile ID | `___` | |
| MAS Endpoint Name | `___` | |
| App URL | `___` | |

# Phase 5: Act — External MCP Server (Salesforce Integration)

## Overview

Phase 5 connects the Supervisor Agent to an external system — in this case, Salesforce — via a custom MCP Server deployed as a Databricks App. This enables the Supervisor Agent to not just read data, but take actions: update SAP Title Catalog entries, trigger royalty payment workflows, or flag contract renewals.

This phase is **not automated** because it requires access to an actual SAP sandbox instance and service principal configuration specific to your environment.

## Architecture

```
User Question
    │
    ▼
RightsIQ Supervisor Agent
    │
    ├── contract_analyst (KA)     ← reads contract PDFs
    ├── rights_explorer (Genie)   ← queries structured data
    └── sap_operations (MCP)      ← writes to SAP  ◄── THIS PHASE
            │
            ▼
    Databricks App (MCP Server)
            │
            ▼
    SAP S/4HANA (Title Catalog, AP, etc.)
```

## Step-by-Step Setup

### Step 1: Build the MCP Server

Create a Databricks App that implements a JSON-RPC 2.0 endpoint conforming to the MCP protocol. The server exposes tools that map to SAP operations.

Example tool definitions for RightsIQ:

```python
TOOLS = [
    {
        "name": "update_title_catalog",
        "description": "Update a title's metadata in the SAP Title Catalog",
        "inputSchema": {
            "type": "object",
            "properties": {
                "title_id": {"type": "string", "description": "Internal title ID"},
                "field": {"type": "string", "description": "Field to update (e.g., status, territory_rights)"},
                "value": {"type": "string", "description": "New value"},
            },
            "required": ["title_id", "field", "value"],
        },
    },
    {
        "name": "trigger_royalty_payment",
        "description": "Initiate a royalty payment workflow in SAP AP",
        "inputSchema": {
            "type": "object",
            "properties": {
                "agreement_id": {"type": "string", "description": "Rights agreement ID"},
                "amount_usd": {"type": "number", "description": "Payment amount"},
                "payee": {"type": "string", "description": "Licensor to pay"},
            },
            "required": ["agreement_id", "amount_usd", "payee"],
        },
    },
    {
        "name": "flag_renewal",
        "description": "Flag an agreement for renewal review in SAP workflow",
        "inputSchema": {
            "type": "object",
            "properties": {
                "agreement_id": {"type": "string", "description": "Rights agreement ID"},
                "reason": {"type": "string", "description": "Reason for flagging"},
                "priority": {"type": "string", "enum": ["low", "medium", "high", "urgent"]},
            },
            "required": ["agreement_id", "reason"],
        },
    },
    {
        "name": "search_sap_contracts",
        "description": "Search for contracts in SAP by licensor, title, or territory",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "filters": {
                    "type": "object",
                    "properties": {
                        "licensor": {"type": "string"},
                        "territory": {"type": "string"},
                        "status": {"type": "string"},
                    },
                },
            },
            "required": ["query"],
        },
    },
]
```

The MCP server must implement three JSON-RPC methods:
- `initialize` — handshake and capability negotiation
- `tools/list` — returns the tool definitions above
- `tools/call` — executes a tool and returns results

### Step 2: Deploy as a Databricks App

```bash
# Create the app structure
mkdir rightsiq-mcp-server
cd rightsiq-mcp-server

# app.py should implement FastAPI with the JSON-RPC endpoint at /api/mcp
# See the ai-dev-kit supervisor-agents documentation for MCP server templates

databricks apps deploy rightsiq-mcp-server \
  --source-code-path /Workspace/Users/<user>/rightsiq-mcp-server \
  --profile=<your-profile>
```

Note the deployed app URL: `https://<app-name>.databricksapps.com`

### Step 3: Create a Unity Catalog HTTP Connection

```sql
CREATE CONNECTION rightsiq_sap_mcp TYPE HTTP
OPTIONS (
  host 'https://rightsiq-mcp-server.databricksapps.com',
  port '443',
  base_path '/api/mcp',
  client_id '<service_principal_application_id>',
  client_secret '<service_principal_secret>',
  oauth_scope 'all-apis',
  token_endpoint 'REPLACE_WITH_WORKSPACE_HOST/oidc/v1/token',
  is_mcp_connection 'true'
);
```

### Step 4: Grant Permissions

```sql
-- Grant the agent's service principal access to the connection
GRANT USE CONNECTION ON rightsiq_sap_mcp TO `<supervisor_agent_service_principal>`;
```

### Step 5: Add MCP Agent to the Supervisor

Update the existing Supervisor Agent to include the SAP operations agent:

```python
manage_mas(
    action="create_or_update",
    name="RightsIQ Supervisor",
    tile_id="<existing_mas_tile_id>",
    agents=[
        {
            "name": "contract_analyst",
            "ka_tile_id": "<ka_tile_id>",
            "description": "Answers questions about specific contract clauses, terms, and legal obligations"
        },
        {
            "name": "rights_explorer",
            "genie_space_id": "<genie_space_id>",
            "description": "Runs SQL analytics on rights data: agreements, royalties, territories"
        },
        {
            "name": "sap_operations",
            "connection_name": "rightsiq_sap_mcp",
            "description": "Execute SAP operations: update title catalog entries, trigger royalty payments, flag agreements for renewal, and search SAP contracts. Use for ANY write operation or action."
        }
    ],
    instructions="""
    Route queries as follows:
    - Contract language/clauses/terms/obligations → contract_analyst
    - Data aggregations/counts/totals/comparisons → rights_explorer
    - Update/create/trigger/flag/modify actions → sap_operations
    - Search SAP for specific records → sap_operations

    When a user asks to take an action (update, pay, flag, trigger), ALWAYS use sap_operations.
    For read-only questions, prefer contract_analyst or rights_explorer based on the data source.
    """
)
```

### Step 6: Test the Integration

```sql
-- Verify the connection works
SELECT http_request(
  conn => 'rightsiq_sap_mcp',
  method => 'POST',
  path => '',
  json => '{"jsonrpc":"2.0","method":"tools/list","id":1}'
);
```

Then test through the Supervisor Agent:
- "Flag the Warner Bros agreement for renewal — it expires next month"
- "Trigger a royalty payment of $50,000 for agreement AGR-001"
- "Update the title catalog status for 'The Matrix' to 'Active'"

## Demo Without SAP

If you don't have SAP access, you can still demonstrate Phase 5 conceptually:

1. Build a **mock MCP server** that returns simulated SAP responses
2. Deploy it as a Databricks App
3. The Supervisor Agent routing and tool calling works identically — the audience sees the full flow

The mock server just returns success messages instead of actually calling SAP APIs.

## Resources

- [Model Context Protocol Specification](https://modelcontextprotocol.io)
- [Databricks UC HTTP Connections](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-connection.html)
- [Supervisor Agent MCP documentation](../ai-dev-kit/databricks-skills/databricks-agent-bricks/2-supervisor-agents.md)

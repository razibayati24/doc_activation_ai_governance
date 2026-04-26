"""
RightsIQ MCP Server — Salesforce Edition
Bridges the Databricks Supervisor Agent (MCP protocol) to Salesforce CRM (REST API).

Deploy as a Databricks App. The Supervisor Agent connects via a UC HTTP Connection
with is_mcp_connection: 'true'.

Setup:
  1. Salesforce Developer Edition (free, permanent)
  2. Connected App with Client Credentials flow
  3. Set env vars: SF_INSTANCE_URL, SF_CLIENT_ID, SF_CLIENT_SECRET
"""

import json
import os
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# ============================================================================
# Configuration
# ============================================================================
SF_INSTANCE_URL = os.getenv("SF_INSTANCE_URL", "REPLACE_WITH_SF_INSTANCE_URL")
SF_CLIENT_ID = os.getenv("SF_CLIENT_ID", "")
SF_CLIENT_SECRET = os.getenv("SF_CLIENT_SECRET", "")
SF_API_VERSION = "v60.0"

app = FastAPI(title="RightsIQ MCP Server — Salesforce", version="1.0.0")
logger = logging.getLogger("rightsiq-mcp")
logging.basicConfig(level=logging.INFO)

# In-memory audit log for demo visibility
_audit_log: list[dict] = []


# ============================================================================
# Salesforce OAuth + REST Client
# ============================================================================
class _TokenExpired(Exception):
    """Sentinel raised when Salesforce returns 401."""


class SalesforceClient:
    """Handles OAuth token lifecycle and REST API calls."""

    def __init__(self):
        self._access_token: Optional[str] = None

    def _authenticate(self) -> str:
        """Obtain access token via Client Credentials flow."""
        token_url = f"{SF_INSTANCE_URL}/services/oauth2/token"
        resp = requests.post(token_url, data={
            "grant_type": "client_credentials",
            "client_id": SF_CLIENT_ID,
            "client_secret": SF_CLIENT_SECRET,
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        logger.info("Salesforce OAuth token acquired.")
        return self._access_token

    @property
    def token(self) -> str:
        if not self._access_token:
            return self._authenticate()
        return self._access_token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _handle_response(self, resp: requests.Response) -> Any:
        """Handle response. Raises _TokenExpired on 401 so callers can retry."""
        if resp.status_code == 401:
            self._access_token = None
            raise _TokenExpired()
        resp.raise_for_status()
        if resp.status_code == 204:
            return {"success": True}
        return resp.json()

    def _api_url(self, path: str) -> str:
        return f"{SF_INSTANCE_URL}/services/data/{SF_API_VERSION}{path}"

    def _request_with_retry(self, method: str, url: str, **kwargs) -> Any:
        """Execute an HTTP request; on 401 re-authenticate and retry once."""
        for attempt in range(2):
            resp = requests.request(method, url, headers=self._headers(), timeout=30, **kwargs)
            try:
                return self._handle_response(resp)
            except _TokenExpired:
                if attempt == 0:
                    logger.info("Token expired, re-authenticating and retrying...")
                    self._authenticate()
                else:
                    raise requests.exceptions.HTTPError(
                        "Salesforce returned 401 after token refresh."
                    )

    def query(self, soql: str) -> dict:
        """Run a SOQL query."""
        logger.info(f"SOQL: {soql}")
        return self._request_with_retry("GET", self._api_url("/query/"), params={"q": soql})

    def create(self, sobject: str, data: dict) -> dict:
        """Create a record."""
        logger.info(f"CREATE {sobject}: {json.dumps(data)}")
        return self._request_with_retry("POST", self._api_url(f"/sobjects/{sobject}/"), json=data)

    def update(self, sobject: str, record_id: str, data: dict) -> dict:
        """Update a record."""
        logger.info(f"UPDATE {sobject}/{record_id}: {json.dumps(data)}")
        return self._request_with_retry("PATCH", self._api_url(f"/sobjects/{sobject}/{record_id}"), json=data)


sf = SalesforceClient()


# ============================================================================
# Audit Log Helper
# ============================================================================
def log_action(tool: str, details: dict, result: dict):
    _audit_log.insert(0, {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tool": tool,
        "details": details,
        "result": result,
    })


# ============================================================================
# MCP Tool Definitions
# ============================================================================
MCP_TOOLS = [
    {
        "name": "flag_renewal",
        "description": (
            "Flag a rights agreement for renewal review in Salesforce. "
            "Creates an Opportunity in 'Qualification' stage and a Task assigned to the licensing team. "
            "Use when the user wants to initiate a renewal review for an expiring deal."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "agreement_id": {
                    "type": "string",
                    "description": "The agreement identifier (e.g., AGR-0042)"
                },
                "title_name": {
                    "type": "string",
                    "description": "The title being licensed (e.g., 'The Dark Horizon')"
                },
                "licensee": {
                    "type": "string",
                    "description": "The licensee company (e.g., 'Netflix')"
                },
                "territory": {
                    "type": "string",
                    "description": "Territory of the deal (e.g., 'United States')"
                },
                "expiry_date": {
                    "type": "string",
                    "description": "Current license expiry date (YYYY-MM-DD)"
                },
                "reason": {
                    "type": "string",
                    "description": "Reason for flagging renewal"
                },
                "priority": {
                    "type": "string",
                    "description": "Priority level: High, Normal, Low",
                    "enum": ["High", "Normal", "Low"]
                }
            },
            "required": ["agreement_id", "licensee", "reason"]
        }
    },
    {
        "name": "log_royalty_payment",
        "description": (
            "Log a royalty payment action in Salesforce. "
            "Creates a Task on the relevant Account with payment details. "
            "Use when the user wants to record or initiate a royalty payment."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "agreement_id": {
                    "type": "string",
                    "description": "The agreement identifier"
                },
                "licensee": {
                    "type": "string",
                    "description": "The licensee company"
                },
                "amount": {
                    "type": "number",
                    "description": "Payment amount in USD"
                },
                "payment_type": {
                    "type": "string",
                    "description": "Payment type: Royalty, Minimum Guarantee, Advance",
                    "enum": ["Royalty", "Minimum Guarantee", "Advance"]
                },
                "period": {
                    "type": "string",
                    "description": "Payment period (e.g., 'Q1 2025')"
                },
                "notes": {
                    "type": "string",
                    "description": "Additional payment notes"
                }
            },
            "required": ["agreement_id", "licensee", "amount"]
        }
    },
    {
        "name": "search_accounts",
        "description": (
            "Search Salesforce Accounts (licensors, licensees, studios). "
            "Use when the user asks about a specific company or wants to look up account details."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Company name or keyword to search"
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "search_opportunities",
        "description": (
            "Search Salesforce Opportunities (licensing deals, renewals). "
            "Use when the user asks about deal status, pipeline, or renewal reviews."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search keyword (title name, licensee, agreement ID)"
                },
                "stage": {
                    "type": "string",
                    "description": "Filter by stage: Qualification, Prospecting, Negotiation, Closed Won, Closed Lost"
                }
            }
        }
    },
    {
        "name": "get_audit_log",
        "description": (
            "Retrieve the audit log of all actions taken by the RightsIQ agent this session. "
            "Use when the user asks what actions have been performed."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Number of recent entries to return (default: 20)"
                }
            }
        }
    }
]


# ============================================================================
# Tool Handlers
# ============================================================================
def handle_flag_renewal(args: dict) -> str:
    agreement_id = args["agreement_id"]
    licensee = args["licensee"]
    title_name = args.get("title_name", "")
    territory = args.get("territory", "")
    expiry_date = args.get("expiry_date", "")
    reason = args.get("reason", "Flagged for renewal review")
    priority = args.get("priority", "Normal")

    try:
        # 1. Find or create the Account for the licensee
        account_id = _find_or_create_account(licensee)

        # 2. Create an Opportunity for the renewal review
        opp_name = f"Renewal: {agreement_id}"
        if title_name:
            opp_name = f"Renewal: {title_name} — {licensee} ({territory})" if territory else f"Renewal: {title_name} — {licensee}"

        opp_data = {
            "Name": opp_name,
            "AccountId": account_id,
            "StageName": "Qualification",
            "CloseDate": expiry_date or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "Description": (
                f"Agreement: {agreement_id}\n"
                f"Title: {title_name}\n"
                f"Territory: {territory}\n"
                f"Current Expiry: {expiry_date}\n"
                f"Reason: {reason}\n\n"
                f"Auto-created by RightsIQ Supervisor Agent"
            ),
        }
        opp_result = sf.create("Opportunity", opp_data)
        opp_id = opp_result.get("id", "unknown")

        # 3. Create a Task for the rights team
        task_data = {
            "Subject": f"Review renewal: {agreement_id} — {licensee}",
            "WhatId": opp_id,
            "Priority": priority,
            "Status": "Not Started",
            "ActivityDate": expiry_date or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "Description": (
                f"Action required: Review this agreement for renewal.\n\n"
                f"Agreement: {agreement_id}\n"
                f"Title: {title_name}\n"
                f"Licensee: {licensee}\n"
                f"Territory: {territory}\n"
                f"Expiry: {expiry_date}\n"
                f"Reason: {reason}\n\n"
                f"Created by RightsIQ Supervisor Agent"
            ),
        }
        task_result = sf.create("Task", task_data)
        task_id = task_result.get("id", "unknown")

        summary = {
            "success": True,
            "opportunity_id": opp_id,
            "task_id": task_id,
            "message": (
                f"Renewal flagged in Salesforce for {agreement_id}.\n"
                f"• Opportunity created: '{opp_name}' (Stage: Qualification)\n"
                f"• Task assigned: 'Review renewal: {agreement_id} — {licensee}' (Priority: {priority})\n"
                f"• Account: {licensee}"
            ),
        }
        log_action("flag_renewal", args, summary)
        return json.dumps(summary, indent=2)

    except Exception as e:
        logger.error(f"flag_renewal failed: {e}")
        return json.dumps({"success": False, "error": str(e)})


def handle_log_payment(args: dict) -> str:
    agreement_id = args["agreement_id"]
    licensee = args["licensee"]
    amount = args["amount"]
    payment_type = args.get("payment_type", "Royalty")
    period = args.get("period", "")
    notes = args.get("notes", "")

    try:
        account_id = _find_or_create_account(licensee)
        ref_number = f"PAY-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

        task_data = {
            "Subject": f"Royalty Payment: {agreement_id} — ${amount:,.2f} ({payment_type})",
            "Priority": "Normal",
            "Status": "Completed",
            "ActivityDate": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "Description": (
                f"Payment recorded by RightsIQ Supervisor Agent\n\n"
                f"Agreement: {agreement_id}\n"
                f"Licensee: {licensee}\n"
                f"Amount: ${amount:,.2f}\n"
                f"Type: {payment_type}\n"
                f"Period: {period}\n"
                f"Notes: {notes}\n"
                f"Reference: {ref_number}"
            ),
        }

        # Link to Opportunity for this agreement; create one if none exists
        opp_created = False
        opps = sf.query(
            f"SELECT Id FROM Opportunity WHERE Name LIKE '%{agreement_id}%' LIMIT 1"
        )
        if opps.get("totalSize", 0) > 0:
            task_data["WhatId"] = opps["records"][0]["Id"]
        else:
            opp_name = f"Renewal: {agreement_id}"
            opp_result = sf.create("Opportunity", {
                "Name": opp_name,
                "AccountId": account_id,
                "StageName": "Qualification",
                "CloseDate": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "Description": (
                    f"Agreement: {agreement_id}\n"
                    f"Licensee: {licensee}\n"
                    f"Auto-created by RightsIQ when logging payment"
                ),
            })
            task_data["WhatId"] = opp_result["id"]
            opp_created = True

        task_result = sf.create("Task", task_data)
        task_id = task_result.get("id", "unknown")

        message = (
            f"Payment logged in Salesforce.\n"
            f"• Agreement: {agreement_id}\n"
            f"• Amount: ${amount:,.2f} ({payment_type})\n"
            f"• Period: {period}\n"
            f"• Reference: {ref_number}\n"
            f"• Licensee: {licensee}"
        )
        if opp_created:
            message += f"\n• New Opportunity created: 'Renewal: {agreement_id}' (Qualification stage)"

        summary = {
            "success": True,
            "task_id": task_id,
            "reference_number": ref_number,
            "message": message,
        }
        log_action("log_royalty_payment", args, summary)
        return json.dumps(summary, indent=2)

    except Exception as e:
        logger.error(f"log_payment failed: {e}")
        return json.dumps({"success": False, "error": str(e)})


def handle_search_accounts(args: dict) -> str:
    query = args["query"]
    try:
        result = sf.query(
            f"SELECT Id, Name, Industry, Phone, Website, BillingCity, BillingCountry "
            f"FROM Account WHERE Name LIKE '%{query}%' LIMIT 10"
        )
        records = result.get("records", [])
        cleaned = [{k: v for k, v in r.items() if k != "attributes"} for r in records]
        return f"Found {len(cleaned)} accounts:\n{json.dumps(cleaned, indent=2)}"
    except Exception as e:
        logger.error(f"search_accounts failed: {e}")
        return json.dumps({"error": str(e)})


def handle_search_opportunities(args: dict) -> str:
    query = args.get("query", "")
    stage = args.get("stage", "")
    try:
        conditions = []
        if query:
            conditions.append(f"Name LIKE '%{query}%'")
        if stage:
            conditions.append(f"StageName = '{stage}'")

        where_clause = " AND ".join(conditions) if conditions else "CreatedDate = LAST_N_DAYS:30"

        result = sf.query(
            f"SELECT Id, Name, StageName, CloseDate, Amount, Account.Name, Description "
            f"FROM Opportunity WHERE {where_clause} ORDER BY CreatedDate DESC LIMIT 10"
        )
        records = result.get("records", [])
        cleaned = [{k: v for k, v in r.items() if k != "attributes"} for r in records]
        return f"Found {len(cleaned)} opportunities:\n{json.dumps(cleaned, indent=2)}"
    except Exception as e:
        logger.error(f"search_opportunities failed: {e}")
        return json.dumps({"error": str(e)})


def handle_audit_log(args: dict) -> str:
    limit = args.get("limit", 20)
    entries = _audit_log[:limit]
    if not entries:
        return "No actions have been taken this session."
    return f"Last {len(entries)} actions:\n{json.dumps(entries, indent=2)}"


# ============================================================================
# Helpers
# ============================================================================
def _find_or_create_account(name: str) -> str:
    """Find an Account by name, or create one if it doesn't exist."""
    result = sf.query(f"SELECT Id FROM Account WHERE Name = '{name}' LIMIT 1")
    if result.get("totalSize", 0) > 0:
        return result["records"][0]["Id"]

    create_result = sf.create("Account", {
        "Name": name,
        "Industry": "Media",
        "Description": "Auto-created by RightsIQ Supervisor Agent",
    })
    return create_result["id"]


TOOL_HANDLERS = {
    "flag_renewal": handle_flag_renewal,
    "log_royalty_payment": handle_log_payment,
    "search_accounts": handle_search_accounts,
    "search_opportunities": handle_search_opportunities,
    "get_audit_log": handle_audit_log,
}


# ============================================================================
# MCP JSON-RPC 2.0 Endpoint
# ============================================================================
@app.post("/")
@app.post("/mcp")
async def mcp_endpoint(request: Request):
    """Handle MCP JSON-RPC 2.0 requests from the Databricks Supervisor Agent."""
    body = await request.json()
    method = body.get("method", "")
    req_id = body.get("id")
    params = body.get("params", {})

    logger.info(f"MCP Request: method={method}, id={req_id}")

    # ---- initialize ----
    if method == "initialize":
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": "rightsiq-salesforce-operations",
                    "version": "1.0.0"
                }
            }
        })

    # ---- tools/list ----
    elif method == "tools/list":
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"tools": MCP_TOOLS}
        })

    # ---- tools/call ----
    elif method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})

        handler = TOOL_HANDLERS.get(tool_name)
        if not handler:
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"}
            })

        try:
            result_text = handler(arguments)
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": result_text}]
                }
            })
        except Exception as e:
            logger.error(f"Tool execution error: {e}")
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32603, "message": f"Tool execution failed: {str(e)}"}
            })

    # ---- notifications ----
    elif method.startswith("notifications/"):
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": {}})

    # ---- unknown ----
    else:
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"}
        })


# ---- Health check ----
@app.get("/")
@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "rightsiq-mcp-server-salesforce",
        "salesforce_instance": SF_INSTANCE_URL,
        "tools": [t["name"] for t in MCP_TOOLS],
        "actions_taken": len(_audit_log),
    }


# ============================================================================
# Run
# ============================================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", "8000")))
    uvicorn.run(app, host="0.0.0.0", port=port)

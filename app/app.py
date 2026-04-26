"""
AT&T AI Governance — FinOps & Responsible-AI Intelligence
Calls a Multi-Agent Supervisor that orchestrates Genie (finops_explorer) + KA (contract_analyst).
"""

import json
import os
import re
from pathlib import Path

import requests
import streamlit as st
from databricks.sdk.core import Config

st.set_page_config(
    page_title="AT&T AI Governance",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Config ───────────────────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent / "app_config.json"
DEFAULT_CONFIG = {
    "app_name": "AT&T AI Governance",
    "app_subtitle": "FinOps & Responsible-AI Intelligence Across AT&T",
    "agents": {
        "contract_analyst": {"label": "Contract Analyst", "icon": "◆", "color": "#6B9BD2"},
        "finops_explorer": {"label": "FinOps Explorer", "icon": "◆", "color": "#7BC88F"},
    },
    "demo_paths": [],
    "placeholder": "Ask about AI spend, vendor contracts, or governance clauses…",
}


def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH) as f:
                return {**DEFAULT_CONFIG, **json.load(f)}
        except Exception:
            pass
    return DEFAULT_CONFIG


config = load_config()

cfg = Config()
MAS_ENDPOINT_NAME = os.getenv("MAS_ENDPOINT_NAME", "mas-c39464e9-endpoint")
SERVING_URL = f"{cfg.host.rstrip('/')}/serving-endpoints/{MAS_ENDPOINT_NAME}/invocations"


# ── User-on-Behalf-Of (OBO) auth ─────────────────────────────────────────────
# Databricks Apps inject the viewer's OAuth token in X-Forwarded-Access-Token
# when `user_api_scopes` is declared in app.yaml. We use that token (not the
# app SP) to call the MAS endpoint, so Genie queries run as the actual user
# and column-level masks resolve per-user.
def _get_request_headers() -> dict:
    """Best-effort fetch of the current Streamlit request's HTTP headers."""
    try:
        # Streamlit ≥ 1.42
        return dict(st.context.headers or {})
    except Exception:
        pass
    try:
        from streamlit.web.server.websocket_headers import _get_websocket_headers
        h = _get_websocket_headers()
        return dict(h) if h else {}
    except Exception:
        return {}


def get_user_token() -> str | None:
    h = _get_request_headers()
    # Header names are case-insensitive; check common variants
    for k in ("X-Forwarded-Access-Token", "x-forwarded-access-token"):
        if k in h:
            return h[k]
    # Some Streamlit versions normalize header names
    for k, v in h.items():
        if k.lower() == "x-forwarded-access-token":
            return v
    return None


def get_user_email() -> str | None:
    h = _get_request_headers()
    for k in ("X-Forwarded-Email", "x-forwarded-email", "X-Forwarded-User", "x-forwarded-user"):
        if k in h:
            return h[k]
    for k, v in h.items():
        if k.lower() in ("x-forwarded-email", "x-forwarded-user"):
            return v
    return None


# ── Theme ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
    --bg-primary: #0B0F19;
    --bg-secondary: #111827;
    --bg-elevated: #1A2234;
    --bg-hover: #1E2A3E;
    --border: rgba(255,255,255,0.06);
    --border-hover: rgba(255,255,255,0.12);
    --text-primary: rgba(255,255,255,0.92);
    --text-secondary: rgba(255,255,255,0.55);
    --text-tertiary: rgba(255,255,255,0.30);
    --accent: #00A8E0;
    --agent-ka: #6B9BD2;
    --agent-genie: #7BC88F;
    --agent-mcp: #E8945A;
}

.stApp { background: var(--bg-primary) !important; font-family: 'DM Sans', -apple-system, sans-serif !important; }
#MainMenu, footer, header { visibility: hidden; }

section[data-testid="stSidebar"] { background: var(--bg-secondary) !important; border-right: 1px solid var(--border) !important; }
section[data-testid="stSidebar"] .stMarkdown p, section[data-testid="stSidebar"] .stMarkdown li {
    font-family: 'DM Sans', sans-serif !important; color: var(--text-secondary) !important; font-size: 0.82rem !important;
}
section[data-testid="stSidebar"] .stButton > button {
    font-family: 'DM Sans', sans-serif !important; font-size: 0.78rem !important; font-weight: 400 !important;
    text-align: left !important; padding: 10px 14px !important; border-radius: 8px !important;
    border: 1px solid var(--border) !important; background: transparent !important; color: var(--text-secondary) !important;
    transition: all 0.2s ease !important; line-height: 1.45 !important; white-space: normal !important; height: auto !important;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: var(--bg-hover) !important; border-color: var(--border-hover) !important; color: var(--text-primary) !important;
}

[data-testid="stChatMessage"] { background: transparent !important; border: none !important; padding: 20px 0 !important;
    border-bottom: 1px solid var(--border) !important; border-radius: 0 !important; font-family: 'DM Sans', sans-serif !important;
}
[data-testid="stChatMessage"] p { font-family: 'DM Sans', sans-serif !important; font-size: 0.9rem !important; line-height: 1.7 !important; color: var(--text-primary) !important; }
[data-testid="stChatMessage"] li { font-family: 'DM Sans', sans-serif !important; font-size: 0.88rem !important; line-height: 1.65 !important; color: var(--text-primary) !important; }
[data-testid="stChatMessage"] strong { font-weight: 600 !important; color: white !important; }
[data-testid="stChatMessage"] table { width: 100% !important; border-collapse: collapse !important; margin: 8px 0 !important; }
[data-testid="stChatMessage"] th, [data-testid="stChatMessage"] td { border: 1px solid var(--border) !important; padding: 6px 10px !important; font-size: 0.82rem !important; text-align: left !important; }

[data-testid="stChatInput"] { border-top: 1px solid var(--border) !important; background: var(--bg-secondary) !important; }
[data-testid="stChatInput"] textarea { font-family: 'DM Sans', sans-serif !important; font-size: 0.88rem !important;
    color: var(--text-primary) !important; background: var(--bg-elevated) !important; border: 1px solid var(--border) !important; border-radius: 10px !important;
}
[data-testid="stChatInput"] textarea::placeholder { color: var(--text-tertiary) !important; }

.stSpinner > div { border-top-color: var(--accent) !important; }

section[data-testid="stSidebar"] .streamlit-expanderHeader {
    font-family: 'DM Sans', sans-serif !important; font-size: 0.82rem !important; font-weight: 500 !important;
    color: var(--text-primary) !important; background: transparent !important; border: 1px solid var(--border) !important; border-radius: 8px !important; padding: 10px 14px !important;
}

.brand-header { padding: 32px 0 24px; border-bottom: 1px solid var(--border); margin-bottom: 24px; }
.brand-header h1 { font-family: 'DM Sans', sans-serif; font-size: 1.6rem; font-weight: 700; color: white; margin: 0 0 4px; letter-spacing: -0.03em; }
.brand-header p { font-family: 'DM Sans', sans-serif; font-size: 0.82rem; color: var(--text-tertiary); margin: 0; letter-spacing: 0.02em; }

.agent-tag { display: inline-flex; align-items: center; gap: 6px; padding: 3px 10px 3px 8px; border-radius: 4px;
    font-family: 'JetBrains Mono', monospace; font-size: 0.68rem; font-weight: 500; letter-spacing: 0.04em;
    text-transform: uppercase; margin-bottom: 10px; background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.06);
}
.agent-tag .dot { width: 6px; height: 6px; border-radius: 2px; }

.sidebar-label { font-family: 'JetBrains Mono', monospace; font-size: 0.62rem; font-weight: 500; text-transform: uppercase;
    letter-spacing: 0.12em; color: var(--text-tertiary); margin: 20px 0 10px;
}
.agent-row { display: flex; align-items: flex-start; gap: 10px; padding: 8px 0; }
.agent-row .indicator { width: 7px; height: 7px; border-radius: 2px; margin-top: 5px; flex-shrink: 0; }
.agent-row .name { font-size: 0.8rem; font-weight: 500; color: var(--text-primary); }
.agent-row .desc { font-size: 0.72rem; color: var(--text-tertiary); line-height: 1.45; margin-top: 1px; }

.thinking-bar { display: flex; align-items: center; gap: 10px; padding: 10px 14px; background: var(--bg-elevated);
    border: 1px solid var(--border); border-radius: 8px; margin-bottom: 12px; font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem; color: var(--text-secondary);
}
.thinking-bar .pulse { width: 6px; height: 6px; border-radius: 2px; background: var(--accent); animation: pulse-anim 1.4s ease-in-out infinite; }
@keyframes pulse-anim { 0%, 100% { opacity: 0.3; } 50% { opacity: 1; } }
</style>
""", unsafe_allow_html=True)


# ── Supervisor call (single endpoint, MAS does the routing) ──────────────────
def call_supervisor(messages: list) -> tuple[str, str | None]:
    """Invoke the Multi-Agent Supervisor. Returns (answer_text, agent_name_called).

    Uses the viewer's OBO token if present (so Genie queries run as the actual
    user and column-level masks resolve per-identity). Falls back to the app
    service principal token if OBO isn't available.
    """
    user_token = get_user_token()
    if user_token:
        hdrs = {"Authorization": f"Bearer {user_token}", "Content-Type": "application/json"}
    else:
        hdrs = Config().authenticate()
        hdrs["Content-Type"] = "application/json"
    try:
        resp = requests.post(SERVING_URL, headers=hdrs, json={"input": messages}, timeout=180)
        resp.raise_for_status()
    except requests.HTTPError as e:
        return f"Error {e.response.status_code}: {e.response.text[:300]}", None
    except Exception as e:
        return f"{type(e).__name__}: {e}", None

    data = resp.json()
    agent_called = None
    last_text = ""
    for item in data.get("output", []):
        if item.get("type") == "function_call":
            agent_called = item.get("name")
        elif item.get("type") == "message":
            for c in item.get("content", []):
                if c.get("type") == "output_text" and c.get("text"):
                    last_text = c["text"]
    return last_text or "No response received.", agent_called


def normalize_agent_name(raw: str | None) -> str | None:
    """Map the function_call name from MAS to a config.agents key."""
    if not raw:
        return None
    raw_lower = raw.lower()
    # MAS will return the agent name we registered (finops_explorer / contract_analyst)
    if "finops" in raw_lower or "data_explorer" in raw_lower or "genie" in raw_lower:
        return "finops_explorer"
    if "contract" in raw_lower or "analyst" in raw_lower or "ka" in raw_lower:
        return "contract_analyst"
    # Direct match against config keys
    if raw in config.get("agents", {}):
        return raw
    return None


def agent_tag_html(agent_name: str | None) -> str:
    if not agent_name or agent_name not in config.get("agents", {}):
        return ""
    a = config["agents"][agent_name]
    return (
        f'<div class="agent-tag">'
        f'<div class="dot" style="background:{a.get("color","#888")};"></div>'
        f'{a.get("label", agent_name)}'
        f'</div>'
    )


# ── State ────────────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []
if "agents" not in st.session_state:
    st.session_state.agents = []


# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        f'<div style="padding:8px 0 4px;">'
        f'<span style="font-family:DM Sans,sans-serif;font-size:1.15rem;font-weight:700;color:white;letter-spacing:-0.02em;">'
        f'⬡ {config.get("app_name", "AT&T AI Governance")}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div style="font-size:0.78rem;color:var(--text-tertiary);margin-bottom:16px;font-family:DM Sans,sans-serif;">'
        f'{config.get("app_subtitle", "")}</div>',
        unsafe_allow_html=True,
    )

    # Signed-in-as badge — shows OBO is active and which identity is calling
    viewer_email = get_user_email()
    obo_active = bool(get_user_token())
    if viewer_email or obo_active:
        badge_color = "#7BC88F" if obo_active else "#E8945A"
        badge_text = f"Signed in as {viewer_email}" if viewer_email else "Signed in"
        sub_text = "OBO active — queries run as you" if obo_active else "Service principal mode"
        st.markdown(
            f'<div style="margin-top:12px; padding:10px 12px; background:var(--bg-elevated); '
            f'border:1px solid var(--border); border-radius:8px; font-family:DM Sans,sans-serif;">'
            f'<div style="display:flex; align-items:center; gap:8px;">'
            f'<div style="width:6px; height:6px; border-radius:50%; background:{badge_color};"></div>'
            f'<div style="font-size:0.78rem; color:var(--text-primary); font-weight:500;">{badge_text}</div>'
            f'</div>'
            f'<div style="margin-top:4px; font-size:0.68rem; color:var(--text-tertiary); '
            f'font-family:JetBrains Mono,monospace;">{sub_text}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown('<div class="sidebar-label">Agents</div>', unsafe_allow_html=True)
    for name, a in config.get("agents", {}).items():
        st.markdown(
            f'<div class="agent-row">'
            f'<div class="indicator" style="background:{a.get("color","#888")};"></div>'
            f'<div><div class="name">{a.get("label", name)}</div>'
            f'<div class="desc">{a.get("description", "")}</div></div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown('<div class="sidebar-label">Demo Paths</div>', unsafe_allow_html=True)
    example_prompt = None
    for path_cfg in config.get("demo_paths", []):
        with st.expander(f'{path_cfg.get("icon", "▸")} {path_cfg["title"]}', expanded=False):
            for q in path_cfg.get("questions", []):
                if st.button(q, use_container_width=True, key=f"dp_{hash(q)}"):
                    example_prompt = q

    st.markdown("---")
    if st.button("Clear conversation", use_container_width=True, key="clear"):
        st.session_state.messages = []
        st.session_state.agents = []
        st.rerun()


# ── Main ─────────────────────────────────────────────────────────────────────
st.markdown(
    f'<div class="brand-header">'
    f'<h1>⬡ {config.get("app_name", "AT&T AI Governance")}</h1>'
    f'<p>{config.get("app_subtitle", "")} · powered by {config.get("powered_by", "Databricks Agent Bricks")}</p>'
    f'</div>',
    unsafe_allow_html=True,
)

# Chat history
for idx, msg in enumerate(st.session_state.messages):
    with st.chat_message(msg["role"]):
        if msg["role"] == "assistant":
            a_idx = idx // 2
            if a_idx < len(st.session_state.agents):
                tag = agent_tag_html(st.session_state.agents[a_idx])
                if tag:
                    st.markdown(tag, unsafe_allow_html=True)
        st.markdown(msg["content"])

prompt = st.chat_input(config.get("placeholder", "Ask a question…"))
if example_prompt:
    prompt = example_prompt

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        thinking = st.empty()
        thinking.markdown(
            '<div class="thinking-bar"><div class="pulse"></div>Routing through AT&T AI Governance Supervisor…</div>',
            unsafe_allow_html=True,
        )
        api_messages = [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages]
        answer, raw_agent = call_supervisor(api_messages)
        agent = normalize_agent_name(raw_agent)
        thinking.markdown(agent_tag_html(agent), unsafe_allow_html=True)
        st.markdown(answer)

    st.session_state.agents.append(agent)
    st.session_state.messages.append({"role": "assistant", "content": answer})
    st.rerun()

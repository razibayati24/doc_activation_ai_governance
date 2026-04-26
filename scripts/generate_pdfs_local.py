#!/usr/bin/env python3
"""
Manifest-driven PDF generation for Telco Bricks FinOps AI Governance demo.
Reads contract specs from demo_manifest.json (deterministic fields),
uses Databricks Foundation Models to generate AI vendor contract prose,
converts to PDF, and uploads to UC Volume for Knowledge Assistant indexing.

Run with: DATABRICKS_CONFIG_PROFILE=fevm-cmegdemos python scripts/generate_pdfs_local.py

Dependencies: pip install pymupdf databricks-sdk requests
"""

import json
import os
import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

# ============================================================================
CATALOG = "cmegdemos_catalog"
SCHEMA = "ai_governance"
VOLUME = "raw_data"
FOLDER = "ai_contracts"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{FOLDER}"
WORKSPACE_HOST = "https://fevm-cmegdemos.cloud.databricks.com"
FM_MODEL = "databricks-meta-llama-3-3-70b-instruct"
FM_URL = f"{WORKSPACE_HOST}/serving-endpoints/{FM_MODEL}/invocations"

MANIFEST_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "demo_manifest.json")

# ============================================================================
import requests
from databricks.sdk import WorkspaceClient

_WS_CLIENT = None


def get_ws_client():
    global _WS_CLIENT
    if _WS_CLIENT is None:
        _WS_CLIENT = WorkspaceClient(
            profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "fevm-cmegdemos")
        )
    return _WS_CLIENT


def call_llm(messages: list, max_tokens: int = 4000) -> str:
    """Call Databricks Foundation Model API with OAuth auth and 429 retry."""
    import time
    w = get_ws_client()
    auth_headers = w.config.authenticate()
    headers = dict(auth_headers)
    headers["Content-Type"] = "application/json"
    payload = {"messages": messages, "max_tokens": max_tokens}
    for attempt in range(5):
        r = requests.post(FM_URL, headers=headers, json=payload, timeout=180)
        if r.status_code == 429:
            wait = 10 * (attempt + 1)
            print(f"    Rate limited (429), retrying in {wait}s...")
            time.sleep(wait)
            auth_headers = w.config.authenticate()
            headers = dict(auth_headers)
            headers["Content-Type"] = "application/json"
            continue
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    r.raise_for_status()


# ============================================================================
def load_manifest() -> dict:
    manifest_path = MANIFEST_PATH
    if not os.path.exists(manifest_path):
        for alt in [
            os.path.join(os.path.dirname(__file__), "demo_manifest.json"),
            "config/demo_manifest.json",
            "demo_manifest.json",
        ]:
            if os.path.exists(alt):
                manifest_path = alt
                break
        else:
            raise FileNotFoundError(f"Cannot find demo_manifest.json. Tried: {MANIFEST_PATH}")
    with open(manifest_path) as f:
        return json.load(f)


def manifest_contract_to_spec(contract: dict) -> dict:
    """Convert a manifest contract entry to the spec format used by the HTML generator."""
    start = datetime.strptime(contract["contract_start"], "%Y-%m-%d")
    end = datetime.strptime(contract["contract_end"], "%Y-%m-%d")
    date_range = f"{start.strftime('%B %d, %Y')} through {end.strftime('%B %d, %Y')}"

    overage_pct = contract.get("overage_rate_pct")
    if overage_pct is not None:
        overage_str = f"{overage_pct * 100:.0f}% premium on committed rate for usage above committed allotment"
    else:
        overage_str = "Not applicable — fixed commitment, overages negotiated separately"

    committed = contract["committed_spend_usd"]
    committed_str = f"${committed:,.0f} USD"

    category = contract["service_category"]
    commitment_type = contract.get("commitment_type", "Standard")

    category_label_map = {
        "LLM API": "Large Language Model API Access",
        "GPU Compute": "Reserved GPU Compute Capacity",
        "ML Platform": "Managed Machine Learning Platform",
        "AI Platform": "Enterprise AI Platform and Ontology Services",
        "Data Labeling": "Data Annotation and Labeling Services",
        "Model Hosting": "Private Model Hub and Inference Hosting",
    }
    category_label = category_label_map.get(category, category)

    clause_instructions = []

    if contract.get("has_auto_renewal"):
        clause_instructions.append(
            f"SECTION 9 — TERM, TERMINATION AND RENEWAL: This Agreement MUST include an automatic renewal clause. "
            f"Specific terms: {contract['auto_renewal_terms']}. "
            f"Termination notice: {contract['termination_notice_days']} days written notice."
        )
    else:
        clause_instructions.append(
            f"SECTION 9 — TERM, TERMINATION AND RENEWAL: This Agreement has a FIXED TERM with NO automatic renewal. "
            f"It expires on {end.strftime('%B %d, %Y')} and must be actively renegotiated. "
            f"Termination notice: {contract['termination_notice_days']} days written notice."
        )

    if contract.get("has_data_residency_clause"):
        clause_instructions.append(
            f"SECTION 6 — DATA RESIDENCY AND GEOGRAPHIC PROCESSING: This Agreement MUST include a data residency clause. "
            f"Specific terms: {contract['data_residency_terms']}"
        )
    else:
        clause_instructions.append(
            "SECTION 6 — DATA RESIDENCY AND GEOGRAPHIC PROCESSING: No geographic restrictions on data processing. "
            "Vendor may process Customer data in any region within its global infrastructure."
        )

    if contract.get("has_training_data_restriction"):
        clause_instructions.append(
            f"SECTION 7 — TRAINING DATA USAGE AND DATA RIGHTS: This Agreement MUST include an explicit restriction on training data usage. "
            f"Specific terms: {contract['training_data_terms']}"
        )
    else:
        clause_instructions.append(
            f"SECTION 7 — TRAINING DATA USAGE AND DATA RIGHTS: Include the following training-data language (note: this is intentionally ambiguous, not a strict prohibition). "
            f"Specific terms: {contract.get('training_data_terms', 'Standard Vendor Usage Policies apply.')}"
        )

    if contract.get("has_ai_governance_addendum"):
        clause_instructions.append(
            f"SECTION 8 — AI GOVERNANCE AND RESPONSIBLE AI ADDENDUM: This Agreement MUST include an AI Governance Addendum. "
            f"Specific terms: {contract['ai_governance_terms']}"
        )
    else:
        clause_instructions.append(
            "SECTION 8 — AI GOVERNANCE AND RESPONSIBLE AI ADDENDUM: NO dedicated AI Governance Addendum is included in this Agreement. "
            "Vendor's general Terms of Service apply; Customer is responsible for its own AI governance policies."
        )

    if contract.get("has_output_ownership_clause"):
        clause_instructions.append(
            f"SECTION 10 — OUTPUT OWNERSHIP AND INTELLECTUAL PROPERTY: Include an output ownership clause. "
            f"Specific terms: {contract['output_ownership_terms']}"
        )

    if contract.get("has_mfn_clause"):
        clause_instructions.append(
            f"SECTION 11 — PRICING AND MOST-FAVORED-NATION PROTECTION: This Agreement MUST include a Most-Favored-Nation (MFN) pricing clause. "
            f"Specific terms: {contract['mfn_terms']}"
        )

    if contract.get("has_sublicensing_restriction"):
        clause_instructions.append(
            f"SECTION 12 — PERMITTED USE AND RESTRICTIONS: Include restrictions on downstream use / resale of Outputs. "
            f"Specific terms: {contract['sublicensing_terms']}"
        )

    certs = contract.get("compliance_certifications") or []
    if certs:
        clause_instructions.append(
            f"COMPLIANCE CERTIFICATIONS: Vendor maintains the following certifications which Customer may rely upon: {', '.join(certs)}."
        )

    for sc in contract.get("special_clauses") or []:
        clause_instructions.append(f"ADDITIONAL PROVISION: Include this clause: {sc}")

    return {
        "title": f"AI Services Agreement — {contract['service_name']}",
        "parties": {
            "vendor": contract["vendor"],
            "customer": f"Telco Bricks Inc. ({contract['business_unit']} Business Unit)",
        },
        "service_name": contract["service_name"],
        "service_category": category_label,
        "deployment_regions": contract["deployment_regions"],
        "commitment_type": commitment_type,
        "date_range": date_range,
        "committed_spend": committed_str,
        "overage_rate": overage_str,
        "advance_payment": contract.get("advance_payment", f"${committed * 0.25:,.0f} USD due within 30 days of execution"),
        "termination_notice": f"{contract['termination_notice_days']} days written notice",
        "clause_instructions": clause_instructions,
        "agreement_id": contract["agreement_id"],
        "contract_number": contract["contract_number"],
        "question": f"What are the data-residency, training-data, and AI governance clauses in the Telco Bricks–{contract['vendor']} {contract['service_name']} agreement?",
        "guideline": f"Answer should reference data residency: "
                     f"{'YES — ' + contract.get('data_residency_terms', '') if contract.get('has_data_residency_clause') else 'NOT SPECIFIED'}; "
                     f"training data restriction: "
                     f"{'YES — ' + contract.get('training_data_terms', '') if contract.get('has_training_data_restriction') else 'AMBIGUOUS / NOT STRICT'}; "
                     f"AI governance addendum: "
                     f"{'PRESENT' if contract.get('has_ai_governance_addendum') else 'MISSING'}. "
                     f"Committed spend {committed_str}.",
    }


# ============================================================================
# Contract HTML generation — LLM writes prose, manifest controls facts
# ============================================================================
def generate_contract_html(spec: dict, index: int) -> str:
    clause_block = "\n".join(f"- {ci}" for ci in spec.get("clause_instructions", []))

    prompt = f"""You are an enterprise procurement and technology-sourcing attorney drafting an AI vendor services agreement between Telco Bricks and a major AI vendor. Write a detailed, realistic contract in formal legal prose.

CONTRACT DETAILS — USE THESE EXACT VALUES. Do not change names, amounts, or dates:
- Vendor: {spec['parties']['vendor']}
- Customer: {spec['parties']['customer']}
- Service: {spec['service_name']}
- Service Category: {spec['service_category']}
- Deployment Regions: {', '.join(spec['deployment_regions'])}
- Commitment Type: {spec['commitment_type']}
- Term: {spec['date_range']}
- Committed Spend: {spec['committed_spend']}
- Overage Rate: {spec['overage_rate']}
- Advance Payment: {spec['advance_payment']}
- Termination Notice: {spec['termination_notice']}

MANDATORY CLAUSE INSTRUCTIONS — you MUST follow these exactly:
{clause_block}

Write a complete agreement with these sections:
1. PARTIES AND RECITALS
2. DEFINITIONS (define Customer Data, Inputs, Outputs, Foundation Model, Service)
3. GRANT OF ACCESS (scope of service, deployment regions, named users or API keys)
4. COMMERCIAL TERMS (committed spend, overage pricing, billing cadence, true-up)
5. SERVICE LEVELS (availability SLA, latency commitments, support tiers)
6. DATA RESIDENCY AND GEOGRAPHIC PROCESSING (include the data-residency clause specified above)
7. TRAINING DATA USAGE AND DATA RIGHTS (include the training-data clause specified above — be faithful to the exact language given)
8. AI GOVERNANCE AND RESPONSIBLE AI ADDENDUM (include addendum language if specified, or explicitly state none if not)
9. TERM, TERMINATION AND RENEWAL (include auto-renewal if specified, or explicitly state fixed term if not)
10. OUTPUT OWNERSHIP AND INTELLECTUAL PROPERTY
11. PRICING AND MOST-FAVORED-NATION PROTECTION (only if MFN specified)
12. PERMITTED USE AND RESTRICTIONS (sublicensing, downstream training restrictions)
13. REPRESENTATIONS, WARRANTIES AND INDEMNIFICATION (include foundation-model IP indemnity if relevant)
14. COMPLIANCE AND AUDIT RIGHTS (reference the compliance certifications listed above; include Customer's audit rights)
15. GENERAL PROVISIONS (governing law: State of Texas; dispute resolution; notice addresses; force majeure)

CRITICAL INSTRUCTIONS:
- Use the EXACT dollar amounts, dates, vendor and customer names, regions, and clause language provided above.
- If a data-residency clause is specified, its exact language MUST appear in Section 6.
- If a training-data restriction is specified, its exact language MUST appear in Section 7. If NOT specified, reproduce the ambiguous language exactly as given — do not tighten it.
- If an AI Governance Addendum is specified, its terms MUST appear in Section 8. If NOT specified, Section 8 must state explicitly that no dedicated governance addendum applies.
- If an auto-renewal clause is specified, it MUST appear in Section 9.
- If an MFN clause is specified, it MUST appear in Section 11.
- If sublicensing restrictions are specified, they MUST appear in Section 12.
- Write in formal legal language with numbered sections (1., 1.1, 1.2, etc.).
- Be specific about amounts, dates, notice periods, and certifications.
- Generate approximately 4-5 pages of content.
- Do NOT use markdown formatting (no # headers, no ** bold, no * bullets). Plain text with section numbering only."""

    contract_body = call_llm([{"role": "user", "content": prompt}], max_tokens=4000)

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
  body {{
    font-family: 'Times New Roman', Times, serif;
    font-size: 11pt;
    margin: 60px 70px;
    line-height: 1.6;
    color: #1a1a1a;
  }}
  .header {{
    text-align: center;
    border-bottom: 2px solid #00A8E0;
    padding-bottom: 20px;
    margin-bottom: 30px;
  }}
  .header h1 {{
    font-size: 16pt;
    color: #00A8E0;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 5px;
  }}
  .header .subtitle {{
    font-size: 12pt;
    color: #555;
    margin-bottom: 5px;
  }}
  .parties-box {{
    background: #f8f9fa;
    border: 1px solid #ddd;
    padding: 15px 20px;
    margin: 20px 0;
    border-radius: 4px;
  }}
  .parties-box h3 {{
    color: #00A8E0;
    margin-top: 0;
    font-size: 12pt;
  }}
  .summary-table {{
    width: 100%;
    border-collapse: collapse;
    margin: 20px 0;
  }}
  .summary-table td {{
    padding: 8px 12px;
    border: 1px solid #ddd;
    font-size: 10pt;
  }}
  .summary-table td:first-child {{
    font-weight: bold;
    background: #eef6fb;
    width: 35%;
    color: #00517A;
  }}
  h2 {{
    color: #00A8E0;
    font-size: 12pt;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-top: 25px;
    border-bottom: 1px solid #ddd;
    padding-bottom: 5px;
  }}
  p {{ margin: 8px 0; text-align: justify; }}
  .contract-number {{
    font-size: 9pt;
    color: #888;
    margin-top: 5px;
  }}
  .footer {{
    margin-top: 40px;
    padding-top: 20px;
    border-top: 1px solid #ddd;
    font-size: 9pt;
    color: #888;
    text-align: center;
  }}
  .signature-block {{
    margin-top: 40px;
    display: flex;
    justify-content: space-between;
  }}
  .sig-col {{ width: 45%; }}
  .sig-line {{
    border-top: 1px solid #333;
    margin-top: 40px;
    padding-top: 5px;
    font-size: 10pt;
  }}
  pre {{
    white-space: pre-wrap;
    font-family: 'Times New Roman', Times, serif;
    font-size: 11pt;
    line-height: 1.6;
  }}
</style>
</head>
<body>

<div class="header">
  <h1>Telco Bricks AI Vendor Services Agreement</h1>
  <div class="subtitle">{spec['title']}</div>
  <div class="contract-number">Agreement Reference: {spec['agreement_id']}</div>
</div>

<div class="parties-box">
  <h3>CONTRACT SUMMARY</h3>
  <table class="summary-table">
    <tr><td>Vendor</td><td>{spec['parties']['vendor']}</td></tr>
    <tr><td>Customer</td><td>{spec['parties']['customer']}</td></tr>
    <tr><td>Service</td><td>{spec['service_name']}</td></tr>
    <tr><td>Service Category</td><td>{spec['service_category']}</td></tr>
    <tr><td>Deployment Regions</td><td>{', '.join(spec['deployment_regions'])}</td></tr>
    <tr><td>Commitment Type</td><td>{spec['commitment_type']}</td></tr>
    <tr><td>Term</td><td>{spec['date_range']}</td></tr>
    <tr><td>Committed Spend</td><td>{spec['committed_spend']}</td></tr>
    <tr><td>Overage Rate</td><td>{spec['overage_rate']}</td></tr>
  </table>
</div>

<pre>{contract_body}</pre>

<div class="signature-block">
  <div class="sig-col">
    <div class="sig-line">
      Authorized Signature — {spec['parties']['vendor']}<br>
      Name: ___________________________<br>
      Title: ___________________________<br>
      Date: ___________________________
    </div>
  </div>
  <div class="sig-col">
    <div class="sig-line">
      Authorized Signature — Telco Bricks Inc.<br>
      Name: ___________________________<br>
      Title: ___________________________<br>
      Date: ___________________________
    </div>
  </div>
</div>

<div class="footer">
  CONFIDENTIAL — Telco Bricks Proprietary. This document contains confidential commercial and
  technical terms. Distribution or reproduction without express written consent is prohibited.
</div>

</body>
</html>"""
    return html


# ============================================================================
# PDF conversion — same pymupdf-based flow as original
# ============================================================================
def html_to_pdf(html_content: str, output_path: str) -> bool:
    try:
        import pymupdf

        doc = pymupdf.Document()
        page = doc.new_page(width=595, height=842)

        story = pymupdf.Story(html=html_content)
        mediabox = pymupdf.Rect(0, 0, 595, 842)
        where = mediabox + (36, 36, -36, -36)

        more = True
        while more:
            dev, more = doc.new_page(width=595, height=842).get_displaylist()
            more, _ = story.place(where)
            story.draw(dev)

        if len(doc) > 1:
            doc.delete_page(0)

        doc.save(output_path)
        doc.close()
        return True
    except Exception as e:
        print(f"    PyMuPDF Story failed ({e}), using fallback...")
        return _html_to_pdf_fallback(html_content, output_path)


def _html_to_pdf_fallback(html_content: str, output_path: str) -> bool:
    import pymupdf
    import re

    text = re.sub(r"<style[^>]*>.*?</style>", "", html_content, flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    doc = pymupdf.Document()
    page = doc.new_page(width=595, height=842)
    y = 60
    margin_left = 60
    margin_right = 535
    line_height = 14
    font_size = 10

    for line in lines:
        if y > 780:
            page = doc.new_page(width=595, height=842)
            y = 60

        if line.upper() == line and len(line) > 3 and len(line) < 80:
            page.insert_text((margin_left, y), line, fontsize=11, fontname="helv", color=(0.0, 0.66, 0.88))
            y += line_height + 4
        else:
            words = line.split()
            current_line = ""
            for word in words:
                test_line = f"{current_line} {word}".strip()
                if len(test_line) * 5.5 < (margin_right - margin_left):
                    current_line = test_line
                else:
                    if current_line:
                        page.insert_text((margin_left, y), current_line, fontsize=font_size, fontname="tiro")
                        y += line_height
                        if y > 780:
                            page = doc.new_page(width=595, height=842)
                            y = 60
                    current_line = word
            if current_line:
                page.insert_text((margin_left, y), current_line, fontsize=font_size, fontname="tiro")
                y += line_height

    doc.save(output_path)
    doc.close()
    return True


def upload_file_to_volume(local_path: str, volume_dest: str, w: "WorkspaceClient") -> bool:
    try:
        with open(local_path, "rb") as f:
            w.files.upload(volume_dest, f, overwrite=True)
        return True
    except Exception as e:
        print(f"    Upload failed: {e}")
        return False


# ============================================================================
def main():
    print("=" * 60)
    print("Telco Bricks FinOps AI Governance — Manifest-Driven Contract PDF Generation")
    print("=" * 60)
    print(f"Target: {VOLUME_PATH}")
    print()

    manifest = load_manifest()
    contracts = manifest["contracts"]
    print(f"Manifest loaded: {len(contracts)} contracts")
    print()

    w = WorkspaceClient(profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "fevm-cmegdemos"))
    print(f"Connected to: {w.config.host}")
    print()

    results = []
    with tempfile.TemporaryDirectory() as tmp_dir:
        for i, contract in enumerate(contracts):
            doc_id = f"contract_{str(contract['contract_number']).zfill(3)}"
            print(
                f"[{i+1}/{len(contracts)}] {doc_id}: "
                f"{contract['vendor']} → {contract['business_unit']} "
                f"({contract['service_name']})"
            )

            try:
                spec = manifest_contract_to_spec(contract)
                print(f"  → Spec built from manifest")
                print(f"  → Generating legal prose via LLM...")
                html = generate_contract_html(spec, i)
                print(f"  → HTML generated ({len(html)} chars)")

                pdf_path = os.path.join(tmp_dir, f"{doc_id}.pdf")
                print(f"  → Converting to PDF...")
                success = html_to_pdf(html, pdf_path)
                if not success:
                    print(f"  ✗ PDF conversion failed, skipping")
                    continue

                pdf_size = Path(pdf_path).stat().st_size
                print(f"  → PDF size: {pdf_size / 1024:.1f} KB")

                volume_pdf = f"{VOLUME_PATH}/{doc_id}.pdf"
                print(f"  → Uploading to {volume_pdf}...")
                upload_file_to_volume(pdf_path, volume_pdf, w)

                results.append({
                    "doc_id": doc_id,
                    "agreement_id": contract["agreement_id"],
                    "title": spec["title"],
                    "pdf_path": volume_pdf,
                    "success": True,
                })
                print(f"  ✓ Done!")

            except Exception as e:
                print(f"  ✗ Error: {e}")
                import traceback
                traceback.print_exc()
                results.append({"doc_id": doc_id, "success": False, "error": str(e)})

            print()

    successful = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]
    print("=" * 60)
    print(f"COMPLETE: {len(successful)}/{len(contracts)} contracts generated")
    print(f"Volume path: {VOLUME_PATH}")
    if successful:
        print("\nGenerated contracts:")
        for r in successful:
            print(f"  ✓ {r['doc_id']} ({r['agreement_id']}): {r['title']}")
    if failed:
        print(f"\nFailed ({len(failed)}):")
        for f_item in failed:
            print(f"  ✗ {f_item['doc_id']}: {f_item.get('error', 'unknown')}")
    print("=" * 60)
    print("\nNext step: Create the Knowledge Assistant pointing at this volume.")


if __name__ == "__main__":
    main()

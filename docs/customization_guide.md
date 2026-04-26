# Customization Guide — Adapting for Your Vertical

This demo ships as a RightsIQ (media licensing) use case. Here's how to adapt it for other CMEG verticals.

## What Changes Per Vertical

| Component | Difficulty | What to Modify |
|-----------|------------|----------------|
| PDF descriptions | Easy | Update `config/extraction_schema.json` → `pdf_generation.description` |
| Structured table schemas | Easy | Update `config/extraction_schema.json` → `structured_tables` |
| Territory standardization | Easy | Update `config/extraction_schema.json` → `silver_transformations` |
| Gold aggregation grain | Easy | Update `config/extraction_schema.json` → `gold_aggregations` |
| Genie Space questions | Easy | Update `config/genie_config.json` |
| KA instructions | Easy | Update the KA instructions in CLAUDE.md Phase 3 |
| Supervisor routing logic | Easy | Update the MAS instructions in CLAUDE.md Phase 4 |
| Data generation script | Medium | Rewrite `scripts/generate_rights_data.py` for new schemas |
| Lakeflow transformations | Medium | Rewrite silver/gold SQL transformations |
| App UI branding | Medium | Update `app/app.py` query examples and branding |
| Phase 5 external system | Hard | Different per customer (SAP → Salesforce, Oracle, etc.) |

## Vertical Templates

### Telecom — SLA Contract Analysis

**Use case**: Extract and analyze Service Level Agreement contracts to track SLA compliance, penalties, and service commitments.

**PDF generation description**:
```
Telecommunications Service Level Agreement (SLA) contracts between enterprise
customers and telecom providers. Each contract should include: Parties (provider
and customer with company names), Service descriptions (data circuits, voice,
cloud connectivity, managed services), Uptime guarantees (99.9%, 99.99%, etc.
by service tier), Penalty and credit structures for SLA breaches, Performance
metrics (latency, jitter, packet loss, MTTR, MTBF), Escalation procedures
and support tiers, Contract term and auto-renewal clauses, Bandwidth commitments
and burst capacity, Disaster recovery and redundancy requirements, Billing
terms and rate cards.
```

**Key tables**:
- `customers` — enterprise customer profiles
- `sla_agreements` — SLA terms per service per customer
- `service_metrics` — monthly performance measurements
- `sla_breaches` — penalty/credit events

**Gold grain**: customer × service × month

**Genie questions**:
- "Which customers have had the most SLA breaches this quarter?"
- "What is the average uptime by service tier?"
- "Show total credits issued by customer in the last 12 months"

---

### Gaming — IP Licensing & Royalties

**Use case**: Manage intellectual property licensing agreements for game characters, franchises, and merchandising rights.

**PDF generation description**:
```
Intellectual property licensing agreements for video game franchises and
characters. Each contract should include: Parties (IP owner/developer and
licensee/publisher), Licensed properties (game titles, character names,
franchise universes), Grant of rights (game development, merchandising,
film adaptation, theme parks), Territory and platform restrictions (console,
PC, mobile, specific regions), Royalty structures (per-unit, revenue share,
minimum guarantees), Quality control and approval processes, Marketing
obligations and co-promotion rights, Sequel and derivative work rights,
Term and renewal with performance milestones, Audit rights and reporting
requirements.
```

**Key tables**:
- `properties` — IP assets (games, characters, franchises)
- `license_agreements` — licensing terms per property
- `royalty_reports` — quarterly royalty calculations
- `milestone_tracking` — development and sales milestones

**Gold grain**: property × licensee × quarter

---

### Advertising — Campaign Contract Management

**Use case**: Manage advertising campaign contracts, insertion orders, and media buy agreements across channels.

**PDF generation description**:
```
Advertising campaign contracts and insertion orders between brands/agencies
and media publishers. Each contract should include: Parties (advertiser/agency
and publisher/network), Campaign details (name, objective, target audience),
Media placements (channel, format, position, frequency), Rate cards and
pricing (CPM, CPC, CPA, flat rate), Budget allocations by channel and
flight dates, Performance guarantees (impressions, clicks, conversions),
Makegoods and underdelivery remedies, Creative specifications and approval
workflows, Reporting and attribution requirements, Payment terms and
cancellation policies.
```

**Key tables**:
- `campaigns` — campaign metadata and objectives
- `insertion_orders` — media buy details per publisher
- `delivery_metrics` — daily impression/click/conversion data
- `billing_records` — invoicing and payment tracking

**Gold grain**: campaign × publisher × month

---

## Step-by-Step: Creating a New Vertical

1. **Copy `config/extraction_schema.json`** and update all sections for your vertical
2. **Update CLAUDE.md** Phases 1-4 with your table names, descriptions, and instructions
3. **Update `config/genie_config.json`** with relevant sample questions
4. **Run `vibe agent`** and tell Claude: "Build the demo end to end"

Claude will use the updated configs to generate appropriate PDFs, data, tables, Genie Spaces, Knowledge Assistants, and the Supervisor Agent.

## Interactive Mode (Future)

In a future iteration, you'll be able to tell Claude:

> "I need a Document Activation demo for a telecom customer focused on SLA compliance."

Claude will then walk you through:
1. Confirming the vertical and use case
2. Selecting which products to include (Genie, KA, MAS, or subset)
3. Generating appropriate synthetic data
4. Building and deploying all components

This interactive workflow is planned but not yet implemented. For now, manually edit the config files and CLAUDE.md.

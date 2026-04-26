-- AT&T FinOps AI Governance — Lakeflow Pipeline
--
-- ARCHITECTURE:
--   bronze_extracted_ai_contracts (from PDF extraction) ──→ silver_ai_services
--                                                       └→ silver_ai_contracts
--   bronze_ai_spend_transactions  (from ERP / billing)  ──→ silver_ai_spend_transactions
--                                                              ↓
--                                                    gold_ai_spend_by_vendor_bu
--                                                    gold_contract_summary
--                                                    gold_bu_ai_overview
--
-- Volume path injected at deploy time: /Volumes/cmegdemos_catalog/ai_governance/raw_data/bronze_data


-- ============================================================================
-- BRONZE
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_extracted_ai_contracts
COMMENT "Raw ai_extract output from AI vendor contract PDFs. One row per contract with all extracted fields. deployment_regions pipe-delimited."
AS SELECT *
FROM STREAM read_files(
  '/Volumes/cmegdemos_catalog/ai_governance_doc_intelligence/raw_data/bronze_data/extracted_contracts/',
  format => 'csv',
  header => true,
  inferSchema => true
);

CREATE OR REFRESH STREAMING TABLE bronze_ai_spend_transactions
COMMENT "Raw AI spend records from the billing / ERP system."
AS SELECT *
FROM STREAM read_files(
  '/Volumes/cmegdemos_catalog/ai_governance_doc_intelligence/raw_data/bronze_data/ai_spend_transactions/',
  format => 'csv',
  header => true,
  inferSchema => true
);


-- ============================================================================
-- SILVER
-- ============================================================================

-- Silver AI Services: distinct catalog of AI services/products referenced by contracts
CREATE OR REFRESH MATERIALIZED VIEW silver_ai_services (
  CONSTRAINT valid_service_id EXPECT (service_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_service_name EXPECT (service_name IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Deduplicated AI service catalog derived from contract extractions. One row per unique service_id."
AS SELECT service_id, service_name, service_category, vendor, commitment_type
FROM (
  SELECT
    service_id,
    service_name,
    service_category,
    vendor,
    commitment_type,
    ROW_NUMBER() OVER (PARTITION BY service_id ORDER BY source_file) AS _rn
  FROM bronze_extracted_ai_contracts
)
WHERE _rn = 1;


-- Silver AI Contracts: explode deployment_regions, standardize BU names, dedup, validate
CREATE OR REFRESH MATERIALIZED VIEW silver_ai_contracts (
  CONSTRAINT valid_agreement_id EXPECT (agreement_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_vendor EXPECT (vendor IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_bu EXPECT (business_unit IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_service_ref EXPECT (service_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date_range EXPECT (contract_end >= contract_start) ON VIOLATION DROP ROW,
  CONSTRAINT valid_committed EXPECT (committed_spend_usd >= 0 OR committed_spend_usd IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_region EXPECT (deployment_region IS NOT NULL AND deployment_region != '') ON VIOLATION DROP ROW
)
COMMENT "Normalized AI vendor contracts: one row per agreement + deployment_region. BU codes standardized to full names; governance addendum flags preserved."
AS SELECT
  agreement_id,
  service_id,
  service_name,
  vendor,
  CASE TRIM(bu_raw)
    WHEN 'ATT-BUS' THEN 'AT&T Business'
    WHEN 'ATTBUS'  THEN 'AT&T Business'
    WHEN 'ENG'     THEN 'Engineering R&D'
    WHEN 'R&D'     THEN 'Engineering R&D'
    WHEN 'CW'      THEN 'Consumer Wireless'
    WHEN 'CONSUMER' THEN 'Consumer Wireless'
    WHEN 'MOB'     THEN 'Mobility'
    WHEN 'MOBILITY' THEN 'Mobility'
    WHEN 'CX'      THEN 'Customer Experience'
    WHEN 'NETOPS'  THEN 'Network Operations'
    WHEN 'NET-OPS' THEN 'Network Operations'
    WHEN 'FIN'     THEN 'Finance'
    WHEN 'CYBER'   THEN 'Cybersecurity'
    WHEN 'SEC'     THEN 'Cybersecurity'
    ELSE TRIM(bu_raw)
  END AS business_unit,
  TRIM(region_exploded) AS deployment_region,
  service_category,
  commitment_type,
  contract_start,
  contract_end,
  committed_spend_usd,
  overage_rate_pct,
  contract_status,
  termination_notice_days,
  has_auto_renewal,
  has_mfn_clause,
  has_data_residency_clause,
  has_training_data_restriction,
  has_ai_governance_addendum,
  has_output_ownership_clause,
  has_sublicensing_restriction,
  compliance_certifications
FROM (
  SELECT
    ec.source_file,
    ec.agreement_id,
    ec.service_id,
    ec.service_name,
    ec.vendor,
    ec.business_unit AS bu_raw,
    region_exploded,
    ec.service_category,
    ec.commitment_type,
    ec.contract_start,
    ec.contract_end,
    ec.committed_spend_usd,
    ec.overage_rate_pct,
    ec.contract_status,
    ec.termination_notice_days,
    ec.has_auto_renewal,
    ec.has_mfn_clause,
    ec.has_data_residency_clause,
    ec.has_training_data_restriction,
    ec.has_ai_governance_addendum,
    ec.has_output_ownership_clause,
    ec.has_sublicensing_restriction,
    ec.compliance_certifications,
    ROW_NUMBER() OVER (
      PARTITION BY ec.agreement_id, TRIM(region_exploded)
      ORDER BY ec.source_file DESC
    ) AS _rn
  FROM bronze_extracted_ai_contracts ec
  LATERAL VIEW explode(split(ec.deployment_regions, '\\|')) r AS region_exploded
)
WHERE _rn = 1;


-- Silver Spend Transactions: dedup, validate agreement refs
CREATE OR REFRESH MATERIALIZED VIEW silver_ai_spend_transactions (
  CONSTRAINT valid_transaction_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_agreement_ref EXPECT (agreement_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_spend EXPECT (actual_spend_usd > 0) ON VIOLATION DROP ROW
)
COMMENT "Deduplicated AI spend transactions. Orphaned records and invalid amounts dropped."
AS SELECT * FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY report_date DESC) AS _rn
  FROM bronze_ai_spend_transactions
)
WHERE _rn = 1;


-- ============================================================================
-- GOLD
-- ============================================================================

-- gold_ai_spend_by_vendor_bu: one row per vendor × BU × service_category
CREATE OR REFRESH MATERIALIZED VIEW gold_ai_spend_by_vendor_bu
COMMENT "AI spend aggregated by vendor, business unit, and service category. Primary view for portfolio and BU-level spend queries."
AS
WITH contract_to_bu AS (
  -- Flatten contract → BU (one contract may appear across multiple regions but BU is constant)
  SELECT DISTINCT
    agreement_id,
    vendor,
    business_unit,
    service_category,
    service_name,
    commitment_type,
    committed_spend_usd,
    contract_status,
    contract_start,
    contract_end,
    has_ai_governance_addendum,
    has_training_data_restriction,
    has_data_residency_clause
  FROM silver_ai_contracts
),
spend_agg AS (
  SELECT
    agreement_id,
    ROUND(SUM(COALESCE(actual_spend_usd, 0)), 2) AS total_actual_spend_usd,
    ROUND(SUM(COALESCE(committed_allotment_usd, 0)), 2) AS total_committed_allotment_usd,
    ROUND(SUM(COALESCE(overage_spend_usd, 0)), 2) AS total_overage_usd,
    SUM(CASE WHEN payment_status IN ('Pending', 'Overdue') THEN COALESCE(actual_spend_usd, 0) ELSE 0 END) AS outstanding_amount_usd,
    COUNT(*) AS transaction_count
  FROM silver_ai_spend_transactions
  GROUP BY agreement_id
)
SELECT
  c.vendor,
  c.business_unit,
  c.service_category,
  COUNT(DISTINCT c.agreement_id) AS contract_count,
  SUM(CASE WHEN c.contract_status = 'Active' THEN 1 ELSE 0 END) AS active_contract_count,
  ROUND(SUM(COALESCE(c.committed_spend_usd, 0)), 2) AS total_committed_spend_usd,
  ROUND(SUM(COALESCE(s.total_actual_spend_usd, 0)), 2) AS total_actual_spend_usd,
  ROUND(SUM(COALESCE(s.total_overage_usd, 0)), 2) AS total_overage_usd,
  ROUND(SUM(COALESCE(s.outstanding_amount_usd, 0)), 2) AS outstanding_amount_usd,
  SUM(CASE WHEN c.has_ai_governance_addendum = true THEN 1 ELSE 0 END) AS contracts_with_gov_addendum,
  SUM(CASE WHEN c.has_training_data_restriction = true THEN 1 ELSE 0 END) AS contracts_with_training_restriction,
  SUM(CASE WHEN c.has_data_residency_clause = true THEN 1 ELSE 0 END) AS contracts_with_data_residency,
  COLLECT_SET(c.commitment_type) AS commitment_types,
  COLLECT_SET(c.service_name) AS services
FROM contract_to_bu c
LEFT JOIN spend_agg s ON c.agreement_id = s.agreement_id
GROUP BY c.vendor, c.business_unit, c.service_category;


-- gold_contract_summary: one row per AI contract with spend + governance flags
CREATE OR REFRESH MATERIALIZED VIEW gold_contract_summary
COMMENT "One row per AI vendor contract with utilization, overage, and governance flags. Deduplicated via first region row to prevent cross-product overcounting."
AS
WITH contract_ref AS (
  SELECT * FROM (
    SELECT
      agreement_id,
      service_id,
      service_name,
      vendor,
      business_unit,
      service_category,
      commitment_type,
      contract_status,
      contract_start,
      contract_end,
      committed_spend_usd,
      overage_rate_pct,
      termination_notice_days,
      has_auto_renewal,
      has_mfn_clause,
      has_data_residency_clause,
      has_training_data_restriction,
      has_ai_governance_addendum,
      has_output_ownership_clause,
      has_sublicensing_restriction,
      compliance_certifications,
      ROW_NUMBER() OVER (PARTITION BY agreement_id ORDER BY deployment_region) AS _rn
    FROM silver_ai_contracts
  )
  WHERE _rn = 1
),
contract_regions AS (
  SELECT
    agreement_id,
    COLLECT_SET(deployment_region) AS deployment_regions,
    COUNT(DISTINCT deployment_region) AS region_count
  FROM silver_ai_contracts
  GROUP BY agreement_id
)
SELECT
  cr.agreement_id,
  cr.service_id,
  cr.service_name,
  cr.vendor,
  cr.business_unit,
  cr.service_category,
  cr.commitment_type,
  cregs.deployment_regions,
  cregs.region_count,
  cr.contract_status,
  cr.contract_start,
  cr.contract_end,
  cr.committed_spend_usd,
  cr.overage_rate_pct,
  cr.termination_notice_days,
  cr.has_auto_renewal,
  cr.has_mfn_clause,
  cr.has_data_residency_clause,
  cr.has_training_data_restriction,
  cr.has_ai_governance_addendum,
  cr.has_output_ownership_clause,
  cr.has_sublicensing_restriction,
  cr.compliance_certifications,
  -- Days remaining until contract_end (negative = already expired)
  DATEDIFF(cr.contract_end, CURRENT_DATE()) AS days_until_expiry,
  -- Flag contracts expiring within 90 days
  CASE WHEN DATEDIFF(cr.contract_end, CURRENT_DATE()) BETWEEN 0 AND 90 THEN true ELSE false END AS expiring_within_90_days,
  -- Financial roll-up from transactions
  COALESCE(s.transaction_count, 0) AS transaction_count,
  ROUND(COALESCE(s.total_actual_spend_usd, 0), 2) AS total_actual_spend_usd,
  ROUND(COALESCE(s.total_committed_allotment_usd, 0), 2) AS total_committed_allotment_usd,
  ROUND(COALESCE(s.total_overage_usd, 0), 2) AS total_overage_usd,
  -- Utilization = actual / committed (as a share). Over 1.0 means trending over.
  CASE
    WHEN cr.committed_spend_usd IS NULL OR cr.committed_spend_usd = 0 THEN NULL
    ELSE ROUND(COALESCE(s.total_actual_spend_usd, 0) / cr.committed_spend_usd, 4)
  END AS spend_utilization_ratio,
  -- Over-budget flag = actual > prorated committed as of today
  CASE
    WHEN cr.committed_spend_usd IS NULL OR cr.committed_spend_usd = 0 THEN false
    WHEN DATEDIFF(cr.contract_end, cr.contract_start) = 0 THEN false
    WHEN COALESCE(s.total_actual_spend_usd, 0) > (
      cr.committed_spend_usd *
      LEAST(1.0, DATEDIFF(LEAST(CURRENT_DATE(), cr.contract_end), cr.contract_start) / CAST(DATEDIFF(cr.contract_end, cr.contract_start) AS DOUBLE))
    ) THEN true
    ELSE false
  END AS is_over_budget,
  ROUND(COALESCE(s.outstanding_amount_usd, 0), 2) AS outstanding_amount_usd,
  s.paid_count,
  s.pending_count,
  s.overdue_count
FROM contract_ref cr
LEFT JOIN contract_regions cregs ON cr.agreement_id = cregs.agreement_id
LEFT JOIN (
  SELECT
    agreement_id,
    COUNT(*) AS transaction_count,
    SUM(COALESCE(actual_spend_usd, 0)) AS total_actual_spend_usd,
    SUM(COALESCE(committed_allotment_usd, 0)) AS total_committed_allotment_usd,
    SUM(COALESCE(overage_spend_usd, 0)) AS total_overage_usd,
    SUM(CASE WHEN payment_status IN ('Pending', 'Overdue') THEN COALESCE(actual_spend_usd, 0) ELSE 0 END) AS outstanding_amount_usd,
    SUM(CASE WHEN payment_status = 'Paid' THEN 1 ELSE 0 END) AS paid_count,
    SUM(CASE WHEN payment_status = 'Pending' THEN 1 ELSE 0 END) AS pending_count,
    SUM(CASE WHEN payment_status = 'Overdue' THEN 1 ELSE 0 END) AS overdue_count
  FROM silver_ai_spend_transactions
  GROUP BY agreement_id
) s ON cr.agreement_id = s.agreement_id;


-- gold_bu_ai_overview: one row per business unit
CREATE OR REFRESH MATERIALIZED VIEW gold_bu_ai_overview
COMMENT "One row per AT&T business unit with portfolio-level AI spend and governance metrics."
AS
WITH contract_per_bu AS (
  SELECT DISTINCT
    business_unit,
    agreement_id,
    vendor,
    service_category,
    commitment_type,
    contract_status,
    contract_start,
    contract_end,
    committed_spend_usd,
    has_ai_governance_addendum,
    has_training_data_restriction,
    has_data_residency_clause
  FROM silver_ai_contracts
),
spend_per_bu AS (
  SELECT
    c.business_unit,
    SUM(COALESCE(s.actual_spend_usd, 0)) AS total_actual_spend_usd
  FROM silver_ai_spend_transactions s
  JOIN (SELECT DISTINCT agreement_id, business_unit FROM silver_ai_contracts) c
    ON s.agreement_id = c.agreement_id
  GROUP BY c.business_unit
)
SELECT
  cp.business_unit,
  COUNT(DISTINCT cp.agreement_id) AS contract_count,
  COUNT(DISTINCT cp.vendor) AS vendor_count,
  SUM(CASE WHEN cp.contract_status = 'Active' THEN 1 ELSE 0 END) AS active_contracts,
  SUM(CASE WHEN cp.contract_status = 'Expired' THEN 1 ELSE 0 END) AS expired_contracts,
  SUM(CASE WHEN cp.contract_status = 'Active' AND DATEDIFF(cp.contract_end, CURRENT_DATE()) BETWEEN 0 AND 90 THEN 1 ELSE 0 END) AS expiring_within_90_days,
  SUM(CASE WHEN cp.contract_status = 'Active' AND cp.has_ai_governance_addendum = false THEN 1 ELSE 0 END) AS active_missing_gov_addendum,
  SUM(CASE WHEN cp.contract_status = 'Active' AND cp.has_training_data_restriction = false THEN 1 ELSE 0 END) AS active_missing_training_restriction,
  SUM(CASE WHEN cp.contract_status = 'Active' AND cp.has_data_residency_clause = false THEN 1 ELSE 0 END) AS active_missing_data_residency,
  ROUND(SUM(COALESCE(cp.committed_spend_usd, 0)), 2) AS total_committed_spend_usd,
  ROUND(AVG(COALESCE(cp.committed_spend_usd, 0)), 2) AS avg_committed_spend_usd,
  ROUND(COALESCE(sp.total_actual_spend_usd, 0), 2) AS total_actual_spend_usd,
  COLLECT_SET(cp.service_category) AS service_categories,
  COLLECT_SET(cp.vendor) AS vendors
FROM contract_per_bu cp
LEFT JOIN spend_per_bu sp ON cp.business_unit = sp.business_unit
GROUP BY cp.business_unit, sp.total_actual_spend_usd;

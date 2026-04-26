# scripts/load_bronze_data.py
# Uploads pre-curated bronze data as CSV files to a Unity Catalog Volume.
# The Lakeflow pipeline then ingests these CSVs as streaming tables (bronze layer).
#
# Execute on Databricks via: run_python_file_on_databricks(file_path="scripts/load_bronze_data.py")
#
# ============================================================================
# Target: Telco Bricks FinOps AI Governance demo
# ============================================================================

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ============================================================================
CATALOG = "cmegdemos_catalog"
SCHEMA = "ai_governance"
VOLUME = "raw_data"
# ============================================================================

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/bronze_data"

# Ensure catalog, schema, and volume exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} COMMENT 'Raw data for Telco Bricks FinOps AI Governance demo'")

# Also create the ai_contracts folder used by PDFs + Knowledge Assistant
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# ============================================================================
# Static bronze data — derived from demo_manifest.json
# Intentional data quality issues included for the silver layer to clean.
# ============================================================================

AI_SERVICES_CSV = """service_id,service_name,category,vendor,vendor_sector,launched_year,pricing_model
SVC-0001,GPT-4 Turbo Enterprise,LLM API,OpenAI,Frontier Model Lab,2023,Token-based + committed spend
SVC-0002,Claude 3 Opus Enterprise,LLM API,Anthropic,Frontier Model Lab,2024,Token-based + committed spend
SVC-0003,Gemini 1.5 Pro (Vertex AI),LLM API,Google Cloud,Hyperscaler AI,2024,Token-based + committed spend
SVC-0004,Amazon Bedrock Model Suite,LLM API,Amazon Web Services,Hyperscaler AI,2023,Token-based
SVC-0005,NVIDIA DGX Cloud H100 Reserved,GPU Compute,NVIDIA,AI Infrastructure,2023,Reserved capacity
SVC-0006,Databricks Mosaic AI Platform,ML Platform,Databricks,ML Platform,2022,Consumption + committed
SVC-0007,Snowflake Cortex AI Services,ML Platform,Snowflake,ML Platform,2024,Credit-based
SVC-0008,Scale AI Generative AI Data Engine,Data Labeling,Scale AI,Data Services,2022,Per-task + platform fee
SVC-0009,Labelbox Model Foundry,Data Labeling,Labelbox,Data Services,2023,SaaS subscription + usage
SVC-0010,Cohere Command R+ Enterprise,LLM API,Cohere,Frontier Model Lab,2024,Token-based
SVC-0011,Hugging Face Enterprise Hub,Model Hosting,Hugging Face,Model Hub,2021,SaaS subscription
SVC-0012,DataRobot AutoML Enterprise,ML Platform,DataRobot,ML Platform,2020,SaaS subscription + compute
SVC-0013,Palantir Foundry AIP,AI Platform,Palantir,Enterprise AI,2023,Enterprise license
SVC-0014,H2O.ai Generative AI Cloud,AI Platform,H2O.ai,ML Platform,2023,SaaS subscription
SVC-0015,Dataiku DSS Enterprise,ML Platform,Dataiku,ML Platform,2019,SaaS subscription
SVC-0001,GPT-4 Turbo Enterprise,LLM API,OpenAI,Frontier Model Lab,2023,Token-based + committed spend
SVC-0002,Claude 3 Opus Enterprise,LLM API,Anthropic,Frontier Model Lab,2024,Token-based + committed spend"""


AI_CONTRACTS_CSV = """agreement_id,service_id,vendor,business_unit,deployment_region,service_category,commitment_type,contract_start,contract_end,committed_spend_usd,overage_rate_pct,contract_status,created_at,updated_at
AI-0001,SVC-0001,OpenAI,TB-BUS,US-East,LLM API,Enterprise,2024-07-01,2026-06-30,8000000.0,0.15,Active,2024-04-10T00:00:00,2025-12-15T00:00:00
AI-0001,SVC-0001,OpenAI,Telco Bricks Business,US-West,LLM API,Enterprise,2024-07-01,2026-06-30,8000000.0,0.15,Active,2024-04-10T00:00:00,2025-12-15T00:00:00
AI-0002,SVC-0002,Anthropic,Customer Experience,US-East,LLM API,Enterprise,2024-10-01,2027-09-30,5000000.0,0.1,Active,2024-07-20T00:00:00,2025-10-01T00:00:00
AI-0003,SVC-0003,Google Cloud,Mobility,US-Central,LLM API,Enterprise,2024-01-15,2027-12-31,12000000.0,,Active,2023-10-15T00:00:00,2025-08-20T00:00:00
AI-0003,SVC-0003,Google Cloud,Mobility,US-East,LLM API,Enterprise,2024-01-15,2027-12-31,12000000.0,,Active,2023-10-15T00:00:00,2025-08-20T00:00:00
AI-0004,SVC-0004,Amazon Web Services,Network Operations,US-East,LLM API,Standard,2023-06-01,2026-06-30,3500000.0,0.12,Active,2023-03-15T00:00:00,2024-05-20T00:00:00
AI-0004,SVC-0004,Amazon Web Services,Network Operations,US-West,LLM API,Standard,2023-06-01,2026-06-30,3500000.0,0.12,Active,2023-03-15T00:00:00,2024-05-20T00:00:00
AI-0004,SVC-0004,Amazon Web Services,Network Operations,EU-West,LLM API,Standard,2023-06-01,2026-06-30,3500000.0,0.12,Active,2023-03-15T00:00:00,2024-05-20T00:00:00
AI-0005,SVC-0005,NVIDIA,Engineering R&D,US-East,GPU Compute,Reserved,2024-03-01,2027-02-28,9500000.0,0.2,Active,2023-12-10T00:00:00,2025-03-15T00:00:00
AI-0005,SVC-0005,NVIDIA,ENG,US-East,GPU Compute,Reserved,2024-03-01,2027-02-28,9500000.0,0.2,Active,2023-12-10T00:00:00,2025-03-15T00:00:00
AI-0006,SVC-0006,Databricks,Customer Experience,US-East,ML Platform,Enterprise,2024-04-01,2029-03-31,7200000.0,,Active,2024-01-10T00:00:00,2024-11-20T00:00:00
AI-0006,SVC-0006,Databricks,Customer Experience,US-West,ML Platform,Enterprise,2024-04-01,2029-03-31,7200000.0,,Active,2024-01-10T00:00:00,2024-11-20T00:00:00
AI-0007,SVC-0007,Snowflake,Finance,US-East,ML Platform,Enterprise,2024-08-01,2026-07-31,2800000.0,0.18,Active,2024-05-10T00:00:00,2025-06-15T00:00:00
AI-0008,SVC-0008,Scale AI,Engineering R&D,US-East,Data Labeling,Enterprise,2024-11-01,2026-10-31,4200000.0,0.25,Active,2024-08-01T00:00:00,2025-02-20T00:00:00
AI-0009,SVC-0001,OpenAI,CW,US-East,LLM API,Standard,2023-03-01,2026-02-28,2400000.0,0.2,Expired,2022-12-15T00:00:00,2023-10-01T00:00:00
AI-0010,SVC-0010,Cohere,HR,US-East,LLM API,Standard,2024-02-01,2026-07-31,650000.0,0.15,Active,2023-11-10T00:00:00,2024-08-15T00:00:00
AI-0011,SVC-0009,Labelbox,Network Operations,US-East,Data Labeling,Standard,2025-01-01,2028-12-31,1100000.0,0.3,Active,2024-10-01T00:00:00,2025-03-01T00:00:00
AI-0012,SVC-0012,DataRobot,Finance,US-East,ML Platform,Enterprise,2022-06-01,2026-05-31,3800000.0,0.15,Expired,2022-03-15T00:00:00,2023-05-01T00:00:00
AI-0012,SVC-0012,DataRobot,Finance,US-West,ML Platform,Enterprise,2022-06-01,2026-05-31,3800000.0,0.15,Expired,2022-03-15T00:00:00,2023-05-01T00:00:00
AI-0013,SVC-0013,Palantir,Cybersecurity,US-East,AI Platform,Enterprise,2024-05-01,2029-04-30,6800000.0,,Active,2024-02-10T00:00:00,2025-01-20T00:00:00
AI-0014,SVC-0002,Anthropic,MOB,US-East,LLM API,Enterprise,2024-06-01,2027-05-31,3200000.0,0.1,Active,2024-03-20T00:00:00,2025-01-15T00:00:00
AI-0014,SVC-0002,Anthropic,Mobility,US-West,LLM API,Enterprise,2024-06-01,2027-05-31,3200000.0,0.1,Active,2024-03-20T00:00:00,2025-01-15T00:00:00
AI-0015,SVC-0011,Hugging Face,Engineering R&D,US-East,Model Hosting,Standard,2023-09-01,2026-08-31,480000.0,0.2,Active,2023-06-10T00:00:00,2024-06-20T00:00:00
AI-0015,SVC-0011,Hugging Face,Engineering R&D,EU-West,Model Hosting,Standard,2023-09-01,2026-08-31,480000.0,0.2,Active,2023-06-10T00:00:00,2024-06-20T00:00:00
AI-0003,SVC-0003,Google Cloud,Mobility,US-Central,LLM API,Enterprise,2024-01-15,2023-12-01,12000000.0,,Active,2023-10-15T00:00:00,2025-08-20T00:00:00
AI-0007,SVC-0007,Snowflake,Finance,US-East,ML Platform,Enterprise,2024-08-01,2026-07-31,2800000.0,0.18,Active,2024-05-10T00:00:00,2025-06-15T00:00:00
AI-0011,SVC-0009,Labelbox,Network Operations,US-East,Data Labeling,Standard,2025-01-01,2028-12-31,,0.3,Active,2024-10-01T00:00:00,2025-03-01T00:00:00"""


# Quarterly spend transactions.
# For AI-0001 (OpenAI anchor): escalating overage — cumulative actual exceeds prorated commit.
# For other contracts: realistic mix of at-commit, under-commit, pending, overdue.
AI_SPEND_TRANSACTIONS_CSV = """transaction_id,agreement_id,period_start,period_end,committed_allotment_usd,actual_spend_usd,overage_spend_usd,payment_status,payment_date,report_date
TXN-00001,AI-0001,2024-07-01,2024-09-30,1000000.0,812456.78,0.0,Paid,2024-11-15,2024-10-20T00:00:00
TXN-00002,AI-0001,2024-10-01,2024-12-31,1000000.0,923178.45,0.0,Paid,2025-02-10,2025-01-15T00:00:00
TXN-00003,AI-0001,2025-01-01,2025-03-31,1000000.0,856234.12,0.0,Paid,2025-05-20,2025-04-18T00:00:00
TXN-00004,AI-0001,2025-04-01,2025-06-30,1000000.0,1012456.89,12456.89,Paid,2025-08-25,2025-07-15T00:00:00
TXN-00005,AI-0001,2025-07-01,2025-09-30,1000000.0,1187923.56,187923.56,Paid,2025-11-12,2025-10-10T00:00:00
TXN-00006,AI-0001,2025-10-01,2025-12-31,1000000.0,1328945.23,328945.23,Paid,2026-02-18,2026-01-20T00:00:00
TXN-00007,AI-0001,2026-01-01,2026-03-31,1000000.0,1412678.9,412678.9,Pending,,2026-04-12T00:00:00
TXN-00008,AI-0001,2026-04-01,2026-06-30,1000000.0,1089456.34,89456.34,Pending,,2026-04-15T00:00:00
TXN-00009,AI-0002,2024-10-01,2024-12-31,416666.67,398456.12,0.0,Paid,2025-02-20,2025-01-18T00:00:00
TXN-00010,AI-0002,2025-01-01,2025-03-31,416666.67,412345.67,0.0,Paid,2025-05-15,2025-04-12T00:00:00
TXN-00011,AI-0002,2025-04-01,2025-06-30,416666.67,389234.45,0.0,Paid,2025-08-22,2025-07-15T00:00:00
TXN-00012,AI-0002,2025-07-01,2025-09-30,416666.67,421567.89,4901.22,Paid,2025-11-28,2025-10-15T00:00:00
TXN-00013,AI-0002,2025-10-01,2025-12-31,416666.67,398123.45,0.0,Paid,2026-01-18,2026-01-10T00:00:00
TXN-00014,AI-0002,2026-01-01,2026-03-31,416666.67,407890.12,0.0,Pending,,2026-04-12T00:00:00
TXN-00015,AI-0003,2024-01-15,2024-03-31,750000.0,712456.78,0.0,Paid,2024-05-20,2024-04-18T00:00:00
TXN-00016,AI-0003,2024-04-01,2024-06-30,750000.0,734567.89,0.0,Paid,2024-09-08,2024-08-01T00:00:00
TXN-00017,AI-0003,2024-07-01,2024-09-30,750000.0,689012.34,0.0,Paid,2024-12-05,2024-11-01T00:00:00
TXN-00018,AI-0003,2024-10-01,2024-12-31,750000.0,756123.45,6123.45,Paid,2025-03-10,2025-02-05T00:00:00
TXN-00019,AI-0003,2025-01-01,2025-03-31,750000.0,723890.12,0.0,Paid,2025-06-12,2025-05-01T00:00:00
TXN-00020,AI-0003,2025-04-01,2025-06-30,750000.0,778234.56,28234.56,Paid,2025-09-05,2025-08-01T00:00:00
TXN-00021,AI-0003,2025-07-01,2025-09-30,750000.0,745678.9,0.0,Paid,2025-12-10,2025-11-15T00:00:00
TXN-00022,AI-0003,2025-10-01,2025-12-31,750000.0,769012.34,19012.34,Paid,2026-03-12,2026-02-15T00:00:00
TXN-00023,AI-0003,2026-01-01,2026-03-31,750000.0,732456.78,0.0,Pending,,2026-04-10T00:00:00
TXN-00024,AI-0004,2023-06-01,2023-09-30,291666.67,234567.89,0.0,Paid,2023-11-15,2023-10-20T00:00:00
TXN-00025,AI-0004,2023-10-01,2023-12-31,291666.67,278912.34,0.0,Paid,2024-02-18,2024-01-15T00:00:00
TXN-00026,AI-0004,2024-01-01,2024-03-31,291666.67,267845.67,0.0,Paid,2024-05-22,2024-04-18T00:00:00
TXN-00027,AI-0004,2024-04-01,2024-06-30,291666.67,289123.45,0.0,Paid,2024-08-25,2024-07-20T00:00:00
TXN-00028,AI-0004,2024-07-01,2024-09-30,291666.67,245678.9,0.0,Paid,2024-11-28,2024-10-22T00:00:00
TXN-00029,AI-0004,2024-10-01,2024-12-31,291666.67,312456.78,20790.11,Paid,2025-01-25,2024-12-20T00:00:00
TXN-00030,AI-0004,2025-01-01,2025-03-31,291666.67,278901.23,0.0,Paid,2025-05-18,2025-04-15T00:00:00
TXN-00031,AI-0004,2025-04-01,2025-06-30,291666.67,295634.56,3967.89,Paid,2025-08-22,2025-07-18T00:00:00
TXN-00032,AI-0004,2025-07-01,2025-09-30,291666.67,267890.12,0.0,Paid,2025-11-20,2025-10-15T00:00:00
TXN-00033,AI-0004,2025-10-01,2025-12-31,291666.67,285123.45,0.0,Paid,2026-02-18,2026-01-15T00:00:00
TXN-00034,AI-0004,2026-01-01,2026-03-31,291666.67,301456.78,9790.11,Pending,,2026-04-12T00:00:00
TXN-00035,AI-0005,2024-03-01,2024-05-30,791666.67,745678.9,0.0,Paid,2024-07-20,2024-06-15T00:00:00
TXN-00036,AI-0005,2024-06-01,2024-08-30,791666.67,789234.56,0.0,Paid,2024-10-22,2024-09-15T00:00:00
TXN-00037,AI-0005,2024-09-01,2024-11-30,791666.67,812456.78,20790.11,Paid,2025-01-18,2024-12-10T00:00:00
TXN-00038,AI-0005,2024-12-01,2025-02-28,791666.67,823456.78,31790.11,Paid,2025-04-22,2025-03-15T00:00:00
TXN-00039,AI-0005,2025-03-01,2025-05-30,791666.67,798123.45,6456.78,Paid,2025-07-20,2025-06-15T00:00:00
TXN-00040,AI-0005,2025-06-01,2025-08-30,791666.67,834567.89,42901.22,Paid,2025-10-18,2025-09-15T00:00:00
TXN-00041,AI-0005,2025-09-01,2025-11-30,791666.67,845678.9,54012.23,Paid,2026-01-22,2025-12-15T00:00:00
TXN-00042,AI-0005,2025-12-01,2026-02-28,791666.67,867890.12,76223.45,Paid,2026-04-20,2026-03-15T00:00:00
TXN-00043,AI-0005,2026-03-01,2026-05-30,791666.67,612345.67,0.0,Pending,,2026-04-15T00:00:00
TXN-00044,AI-0006,2024-04-01,2024-06-30,360000.0,312456.78,0.0,Paid,2024-08-22,2024-07-15T00:00:00
TXN-00045,AI-0006,2024-07-01,2024-09-30,360000.0,345678.9,0.0,Paid,2024-11-20,2024-10-15T00:00:00
TXN-00046,AI-0006,2024-10-01,2024-12-31,360000.0,378234.56,18234.56,Paid,2025-02-18,2025-01-15T00:00:00
TXN-00047,AI-0006,2025-01-01,2025-03-31,360000.0,356789.12,0.0,Paid,2025-05-20,2025-04-15T00:00:00
TXN-00048,AI-0006,2025-04-01,2025-06-30,360000.0,367234.56,7234.56,Paid,2025-08-22,2025-07-15T00:00:00
TXN-00049,AI-0006,2025-07-01,2025-09-30,360000.0,389456.78,29456.78,Paid,2025-11-18,2025-10-15T00:00:00
TXN-00050,AI-0006,2025-10-01,2025-12-31,360000.0,401234.56,41234.56,Paid,2026-02-20,2026-01-15T00:00:00
TXN-00051,AI-0006,2026-01-01,2026-03-31,360000.0,412678.9,52678.9,Pending,,2026-04-12T00:00:00
TXN-00052,AI-0007,2024-08-01,2024-10-31,350000.0,298456.78,0.0,Paid,2025-01-15,2024-12-10T00:00:00
TXN-00053,AI-0007,2024-11-01,2025-01-31,350000.0,324567.89,0.0,Paid,2025-04-20,2025-03-15T00:00:00
TXN-00054,AI-0007,2025-02-01,2025-04-30,350000.0,312890.12,0.0,Paid,2025-07-18,2025-06-15T00:00:00
TXN-00055,AI-0007,2025-05-01,2025-07-31,350000.0,345234.56,0.0,Paid,2025-10-22,2025-09-15T00:00:00
TXN-00056,AI-0007,2025-08-01,2025-10-31,350000.0,367890.12,17890.12,Paid,2026-01-18,2025-12-15T00:00:00
TXN-00057,AI-0007,2025-11-01,2026-01-31,350000.0,356789.01,6789.01,Overdue,,2026-02-20T00:00:00
TXN-00058,AI-0007,2026-02-01,2026-04-30,350000.0,289345.67,0.0,Pending,,2026-04-15T00:00:00
TXN-00059,AI-0008,2024-11-01,2025-01-31,525000.0,478234.56,0.0,Paid,2025-04-18,2025-03-15T00:00:00
TXN-00060,AI-0008,2025-02-01,2025-04-30,525000.0,512678.9,0.0,Paid,2025-07-22,2025-06-15T00:00:00
TXN-00061,AI-0008,2025-05-01,2025-07-31,525000.0,489345.67,0.0,Paid,2025-10-18,2025-09-15T00:00:00
TXN-00062,AI-0008,2025-08-01,2025-10-31,525000.0,534567.89,9567.89,Paid,2026-01-20,2025-12-15T00:00:00
TXN-00063,AI-0008,2025-11-01,2026-01-31,525000.0,501234.56,0.0,Paid,2026-04-18,2026-02-15T00:00:00
TXN-00064,AI-0008,2026-02-01,2026-04-30,525000.0,467890.12,0.0,Pending,,2026-04-15T00:00:00
TXN-00065,AI-0009,2023-03-01,2023-05-31,200000.0,178456.78,0.0,Paid,2023-07-15,2023-06-20T00:00:00
TXN-00066,AI-0009,2023-06-01,2023-08-31,200000.0,195678.9,0.0,Paid,2023-10-20,2023-09-15T00:00:00
TXN-00067,AI-0009,2023-09-01,2023-11-30,200000.0,212345.67,12345.67,Paid,2024-01-22,2023-12-15T00:00:00
TXN-00068,AI-0009,2023-12-01,2024-02-29,200000.0,189012.34,0.0,Paid,2024-04-18,2024-03-15T00:00:00
TXN-00069,AI-0009,2024-03-01,2024-05-31,200000.0,198456.78,0.0,Paid,2024-07-22,2024-06-15T00:00:00
TXN-00070,AI-0009,2024-06-01,2024-08-31,200000.0,223456.78,23456.78,Paid,2024-10-18,2024-09-15T00:00:00
TXN-00071,AI-0009,2024-09-01,2024-11-30,200000.0,201234.56,1234.56,Paid,2025-01-20,2024-12-15T00:00:00
TXN-00072,AI-0009,2024-12-01,2025-02-28,200000.0,187890.12,0.0,Paid,2025-04-18,2025-03-15T00:00:00
TXN-00073,AI-0009,2025-03-01,2025-05-31,200000.0,212456.78,12456.78,Paid,2025-07-22,2025-06-15T00:00:00
TXN-00074,AI-0009,2025-06-01,2025-08-31,200000.0,198234.56,0.0,Paid,2025-10-18,2025-09-15T00:00:00
TXN-00075,AI-0009,2025-09-01,2025-11-30,200000.0,223456.78,23456.78,Paid,2026-01-20,2025-12-15T00:00:00
TXN-00076,AI-0009,2025-12-01,2026-02-28,200000.0,187345.67,0.0,Paid,2026-04-18,2026-03-15T00:00:00
TXN-00077,AI-0010,2024-02-01,2024-04-30,65000.0,58234.56,0.0,Paid,2024-06-20,2024-05-15T00:00:00
TXN-00078,AI-0010,2024-05-01,2024-07-31,65000.0,62345.67,0.0,Paid,2024-09-22,2024-08-15T00:00:00
TXN-00079,AI-0010,2024-08-01,2024-10-31,65000.0,67890.12,2890.12,Paid,2024-12-18,2024-11-15T00:00:00
TXN-00080,AI-0010,2024-11-01,2025-01-31,65000.0,59456.78,0.0,Paid,2025-03-20,2025-02-15T00:00:00
TXN-00081,AI-0010,2025-02-01,2025-04-30,65000.0,63456.78,0.0,Paid,2025-06-18,2025-05-15T00:00:00
TXN-00082,AI-0010,2025-05-01,2025-07-31,65000.0,71234.56,6234.56,Paid,2025-09-22,2025-08-15T00:00:00
TXN-00083,AI-0010,2025-08-01,2025-10-31,65000.0,68890.12,3890.12,Paid,2025-12-18,2025-11-15T00:00:00
TXN-00084,AI-0010,2025-11-01,2026-01-31,65000.0,61234.56,0.0,Paid,2026-03-20,2026-02-15T00:00:00
TXN-00085,AI-0010,2026-02-01,2026-04-30,65000.0,57890.12,0.0,Pending,,2026-04-15T00:00:00
TXN-00086,AI-0011,2025-01-01,2025-03-31,68750.0,58234.56,0.0,Paid,2025-05-18,2025-04-15T00:00:00
TXN-00087,AI-0011,2025-04-01,2025-06-30,68750.0,62456.78,0.0,Paid,2025-08-22,2025-07-15T00:00:00
TXN-00088,AI-0011,2025-07-01,2025-09-30,68750.0,71345.67,2595.67,Paid,2025-11-20,2025-10-15T00:00:00
TXN-00089,AI-0011,2025-10-01,2025-12-31,68750.0,65789.01,0.0,Overdue,,2026-02-10T00:00:00
TXN-00090,AI-0011,2026-01-01,2026-03-31,68750.0,67234.56,0.0,Pending,,2026-04-10T00:00:00
TXN-00091,AI-0012,2022-06-01,2022-08-31,237500.0,212456.78,0.0,Paid,2022-10-22,2022-09-15T00:00:00
TXN-00092,AI-0012,2022-09-01,2022-11-30,237500.0,223890.12,0.0,Paid,2023-01-18,2022-12-15T00:00:00
TXN-00093,AI-0012,2022-12-01,2023-02-28,237500.0,198234.56,0.0,Paid,2023-04-20,2023-03-15T00:00:00
TXN-00094,AI-0012,2023-03-01,2023-05-31,237500.0,234567.89,0.0,Paid,2023-07-22,2023-06-15T00:00:00
TXN-00095,AI-0012,2023-06-01,2023-08-31,237500.0,245678.9,8178.9,Paid,2023-10-18,2023-09-15T00:00:00
TXN-00096,AI-0012,2023-09-01,2023-11-30,237500.0,212345.67,0.0,Paid,2024-01-20,2023-12-15T00:00:00
TXN-00097,AI-0012,2023-12-01,2024-02-29,237500.0,228901.23,0.0,Paid,2024-04-22,2024-03-15T00:00:00
TXN-00098,AI-0012,2024-03-01,2024-05-31,237500.0,241234.56,3734.56,Paid,2024-07-18,2024-06-15T00:00:00
TXN-00099,AI-0012,2024-06-01,2024-08-31,237500.0,219876.54,0.0,Paid,2024-10-20,2024-09-15T00:00:00
TXN-00100,AI-0012,2024-09-01,2024-11-30,237500.0,234567.89,0.0,Paid,2025-01-22,2024-12-15T00:00:00
TXN-00101,AI-0012,2024-12-01,2025-02-28,237500.0,245678.9,8178.9,Paid,2025-04-18,2025-03-15T00:00:00
TXN-00102,AI-0012,2025-03-01,2025-05-31,237500.0,223456.78,0.0,Paid,2025-07-20,2025-06-15T00:00:00
TXN-00103,AI-0012,2025-06-01,2025-08-31,237500.0,251234.56,13734.56,Paid,2025-10-18,2025-09-15T00:00:00
TXN-00104,AI-0012,2025-09-01,2025-11-30,237500.0,234567.89,0.0,Paid,2026-01-20,2025-12-15T00:00:00
TXN-00105,AI-0012,2025-12-01,2026-02-28,237500.0,245678.9,8178.9,Paid,2026-04-18,2026-03-15T00:00:00
TXN-00106,AI-0012,2026-03-01,2026-05-31,237500.0,178234.56,0.0,Pending,,2026-04-15T00:00:00
TXN-00107,AI-0013,2024-05-01,2024-07-31,340000.0,312456.78,0.0,Paid,2024-09-18,2024-08-15T00:00:00
TXN-00108,AI-0013,2024-08-01,2024-10-31,340000.0,334567.89,0.0,Paid,2024-12-20,2024-11-15T00:00:00
TXN-00109,AI-0013,2024-11-01,2025-01-31,340000.0,345678.9,5678.9,Paid,2025-03-22,2025-02-15T00:00:00
TXN-00110,AI-0013,2025-02-01,2025-04-30,340000.0,323456.78,0.0,Paid,2025-06-18,2025-05-15T00:00:00
TXN-00111,AI-0013,2025-05-01,2025-07-31,340000.0,356789.01,16789.01,Paid,2025-09-20,2025-08-15T00:00:00
TXN-00112,AI-0013,2025-08-01,2025-10-31,340000.0,334567.89,0.0,Paid,2025-12-18,2025-11-15T00:00:00
TXN-00113,AI-0013,2025-11-01,2026-01-31,340000.0,367890.12,27890.12,Paid,2026-03-20,2026-02-15T00:00:00
TXN-00114,AI-0013,2026-02-01,2026-04-30,340000.0,312345.67,0.0,Pending,,2026-04-15T00:00:00
TXN-00115,AI-0014,2024-06-01,2024-08-31,266666.67,234567.89,0.0,Paid,2024-10-22,2024-09-15T00:00:00
TXN-00116,AI-0014,2024-09-01,2024-11-30,266666.67,256789.01,0.0,Paid,2025-01-18,2024-12-15T00:00:00
TXN-00117,AI-0014,2024-12-01,2025-02-28,266666.67,243456.78,0.0,Paid,2025-04-20,2025-03-15T00:00:00
TXN-00118,AI-0014,2025-03-01,2025-05-31,266666.67,278123.45,11456.78,Paid,2025-07-22,2025-06-15T00:00:00
TXN-00119,AI-0014,2025-06-01,2025-08-31,266666.67,256789.01,0.0,Paid,2025-10-18,2025-09-15T00:00:00
TXN-00120,AI-0014,2025-09-01,2025-11-30,266666.67,267890.12,1223.45,Paid,2026-01-20,2025-12-15T00:00:00
TXN-00121,AI-0014,2025-12-01,2026-02-28,266666.67,245678.9,0.0,Paid,2026-04-20,2026-03-15T00:00:00
TXN-00122,AI-0014,2026-03-01,2026-05-31,266666.67,189234.56,0.0,Pending,,2026-04-15T00:00:00
TXN-00123,AI-0015,2023-09-01,2023-11-30,40000.0,34567.89,0.0,Paid,2024-01-18,2023-12-15T00:00:00
TXN-00124,AI-0015,2023-12-01,2024-02-29,40000.0,38901.23,0.0,Paid,2024-04-20,2024-03-15T00:00:00
TXN-00125,AI-0015,2024-03-01,2024-05-31,40000.0,42345.67,2345.67,Paid,2024-07-22,2024-06-15T00:00:00
TXN-00126,AI-0015,2024-06-01,2024-08-31,40000.0,36789.01,0.0,Paid,2024-10-18,2024-09-15T00:00:00
TXN-00127,AI-0015,2024-09-01,2024-11-30,40000.0,39012.34,0.0,Paid,2025-01-20,2024-12-15T00:00:00
TXN-00128,AI-0015,2024-12-01,2025-02-28,40000.0,43456.78,3456.78,Paid,2025-04-18,2025-03-15T00:00:00
TXN-00129,AI-0015,2025-03-01,2025-05-31,40000.0,37890.12,0.0,Paid,2025-07-20,2025-06-15T00:00:00
TXN-00130,AI-0015,2025-06-01,2025-08-31,40000.0,41234.56,1234.56,Paid,2025-10-22,2025-09-15T00:00:00
TXN-00131,AI-0015,2025-09-01,2025-11-30,40000.0,38567.89,0.0,Paid,2026-01-18,2025-12-15T00:00:00
TXN-00132,AI-0015,2025-12-01,2026-02-28,40000.0,42345.67,2345.67,Paid,2026-04-20,2026-03-15T00:00:00
TXN-00133,AI-0015,2026-03-01,2026-05-31,40000.0,31234.56,0.0,Pending,,2026-04-15T00:00:00
TXN-00134,,2025-06-15,2025-09-13,125000.0,118456.78,0.0,Paid,2025-11-10,2025-10-01T00:00:00
TXN-00135,,2025-10-01,2025-12-31,88000.0,92345.67,4345.67,Pending,,2026-01-15T00:00:00
TXN-00136,,2026-01-01,2026-03-31,150000.0,178234.56,28234.56,Overdue,,2026-04-15T00:00:00
TXN-00137,AI-0001,2024-10-01,2024-12-31,1000000.0,923178.45,0.0,Paid,2025-02-10,2025-01-15T00:00:00
TXN-00138,AI-0003,2024-04-01,2024-06-30,750000.0,734567.89,0.0,Paid,2024-09-08,2024-08-01T00:00:00
TXN-00139,AI-0005,2024-09-01,2024-11-30,791666.67,812456.78,20790.11,Paid,2025-01-18,2024-12-10T00:00:00
TXN-00140,AI-0009,2024-03-01,2024-05-31,200000.0,198456.78,0.0,Paid,2024-07-22,2024-06-15T00:00:00
TXN-00141,AI-0012,2023-06-01,2023-08-31,237500.0,245678.9,8178.9,Paid,2023-10-18,2023-09-15T00:00:00
TXN-00142,AI-0013,2024-08-01,2024-10-31,340000.0,334567.89,0.0,Paid,2024-12-20,2024-11-15T00:00:00
TXN-00143,AI-0015,2024-03-01,2024-05-31,40000.0,42345.67,2345.67,Paid,2024-07-22,2024-06-15T00:00:00
TXN-00144,AI-0002,2025-04-01,2025-06-30,416666.67,389234.45,0.0,Paid,2025-08-22,2025-07-15T00:00:00"""


# ============================================================================
# Also write the extracted_contracts.csv that the pipeline reads
# (This is the "extracted from PDFs via ai_extract" bronze source —
#  in this demo we pre-compute it from the manifest for determinism.)
# ============================================================================
import json as _json
import os as _os

_manifest_path = None
for _candidate in [
    "/Workspace/Users/razi.bayati@databricks.com/ai-governance-demo/config/demo_manifest.json",
    "./config/demo_manifest.json",
    "demo_manifest.json",
]:
    if _os.path.exists(_candidate):
        _manifest_path = _candidate
        break

EXTRACTED_HEADER = (
    "source_file,agreement_id,service_id,service_name,vendor,business_unit,"
    "deployment_regions,service_category,commitment_type,contract_start,contract_end,"
    "committed_spend_usd,overage_rate_pct,contract_status,termination_notice_days,"
    "has_auto_renewal,has_mfn_clause,has_data_residency_clause,has_training_data_restriction,"
    "has_ai_governance_addendum,has_output_ownership_clause,has_sublicensing_restriction,"
    "compliance_certifications"
)


def _build_extracted_csv_from_manifest() -> str:
    if _manifest_path is None:
        # Fallback minimal hand-curated extracted CSV mirrors the manifest content.
        return _FALLBACK_EXTRACTED_CSV
    with open(_manifest_path) as f:
        mani = _json.load(f)
    svc_lookup = {s["service_id"]: s for s in mani["ai_services"]}
    rows = [EXTRACTED_HEADER]
    for c in mani["contracts"]:
        svc = svc_lookup.get(c["service_id"], {})
        regions = "|".join(c["deployment_regions"])
        certs = "|".join(c.get("compliance_certifications", []))
        row = (
            f'{c["contract_file"]},{c["agreement_id"]},{c["service_id"]},'
            f'"{c["service_name"]}","{c["vendor"]}","{c["business_unit"]}",'
            f'{regions},{c["service_category"]},{c["commitment_type"]},'
            f'{c["contract_start"]},{c["contract_end"]},{c["committed_spend_usd"]},'
            f'{c.get("overage_rate_pct") if c.get("overage_rate_pct") is not None else ""},'
            f'{c["contract_status"]},{c["termination_notice_days"]},'
            f'{str(c.get("has_auto_renewal", False)).lower()},'
            f'{str(c.get("has_mfn_clause", False)).lower()},'
            f'{str(c.get("has_data_residency_clause", False)).lower()},'
            f'{str(c.get("has_training_data_restriction", False)).lower()},'
            f'{str(c.get("has_ai_governance_addendum", False)).lower()},'
            f'{str(c.get("has_output_ownership_clause", False)).lower()},'
            f'{str(c.get("has_sublicensing_restriction", False)).lower()},'
            f'"{certs}"'
        )
        rows.append(row)
    return "\n".join(rows)


_FALLBACK_EXTRACTED_CSV = EXTRACTED_HEADER + """
contract_001.pdf,AI-0001,SVC-0001,GPT-4 Turbo Enterprise,OpenAI,Telco Bricks Business,US-East|US-West,LLM API,Enterprise,2024-07-01,2026-06-30,8000000.0,0.15,Active,90,true,false,true,false,false,true,true,SOC 2 Type II
contract_002.pdf,AI-0002,SVC-0002,Claude 3 Opus Enterprise,Anthropic,Customer Experience,US-East,LLM API,Enterprise,2024-10-01,2027-09-30,5000000.0,0.10,Active,120,false,false,true,true,true,true,true,SOC 2 Type II|ISO 27001|HIPAA BAA available
contract_003.pdf,AI-0003,SVC-0003,Gemini 1.5 Pro (Vertex AI),Google Cloud,Mobility,US-Central|US-East,LLM API,Enterprise,2024-01-15,2027-12-31,12000000.0,,Active,180,true,false,true,true,true,true,false,SOC 2 Type II|ISO 27001|ISO 27017|ISO 27018|HIPAA BAA|FedRAMP Moderate
contract_004.pdf,AI-0004,SVC-0004,Amazon Bedrock Model Suite,Amazon Web Services,Network Operations,US-East|US-West|EU-West,LLM API,Standard,2023-06-01,2026-06-30,3500000.0,0.12,Active,60,false,false,true,true,false,true,true,SOC 2 Type II|ISO 27001|FedRAMP Moderate|HIPAA BAA
contract_005.pdf,AI-0005,SVC-0005,NVIDIA DGX Cloud H100 Reserved,NVIDIA,Engineering R&D,US-East,GPU Compute,Reserved,2024-03-01,2027-02-28,9500000.0,0.20,Active,90,true,true,false,false,false,true,false,SOC 2 Type II
contract_006.pdf,AI-0006,SVC-0006,Databricks Mosaic AI Platform,Databricks,Customer Experience,US-East|US-West,ML Platform,Enterprise,2024-04-01,2029-03-31,7200000.0,,Active,180,true,false,true,true,true,true,false,SOC 2 Type II|ISO 27001|HIPAA BAA|FedRAMP Moderate|PCI DSS
contract_007.pdf,AI-0007,SVC-0007,Snowflake Cortex AI Services,Snowflake,Finance,US-East,ML Platform,Enterprise,2024-08-01,2026-07-31,2800000.0,0.18,Active,90,true,false,true,true,false,true,true,SOC 2 Type II|ISO 27001|HIPAA BAA|PCI DSS
contract_008.pdf,AI-0008,SVC-0008,Scale AI Generative AI Data Engine,Scale AI,Engineering R&D,US-East,Data Labeling,Enterprise,2024-11-01,2026-10-31,4200000.0,0.25,Active,60,false,false,true,true,true,true,true,SOC 2 Type II|ISO 27001
contract_009.pdf,AI-0009,SVC-0001,GPT-4 Turbo Enterprise,OpenAI,Consumer Wireless,US-East,LLM API,Standard,2023-03-01,2026-02-28,2400000.0,0.20,Expired,60,false,false,false,false,false,true,true,SOC 2 Type II
contract_010.pdf,AI-0010,SVC-0010,Cohere Command R+ Enterprise,Cohere,HR,US-East,LLM API,Standard,2024-02-01,2026-07-31,650000.0,0.15,Active,60,true,false,true,true,false,true,false,SOC 2 Type II
contract_011.pdf,AI-0011,SVC-0009,Labelbox Model Foundry,Labelbox,Network Operations,US-East,Data Labeling,Standard,2025-01-01,2028-12-31,1100000.0,0.30,Active,30,false,false,true,true,false,true,false,SOC 2 Type II
contract_012.pdf,AI-0012,SVC-0012,DataRobot AutoML Enterprise,DataRobot,Finance,US-East|US-West,ML Platform,Enterprise,2022-06-01,2026-05-31,3800000.0,0.15,Expired,90,false,true,true,true,true,true,true,SOC 2 Type II|ISO 27001|HIPAA BAA
contract_013.pdf,AI-0013,SVC-0013,Palantir Foundry AIP,Palantir,Cybersecurity,US-East,AI Platform,Enterprise,2024-05-01,2029-04-30,6800000.0,,Active,180,true,false,true,true,true,true,true,SOC 2 Type II|ISO 27001|FedRAMP High|CJIS-ready|IL5
contract_014.pdf,AI-0014,SVC-0002,Claude 3 Opus Enterprise,Anthropic,Mobility,US-East|US-West,LLM API,Enterprise,2024-06-01,2027-05-31,3200000.0,0.10,Active,90,false,true,true,true,true,true,true,SOC 2 Type II|ISO 27001|HIPAA BAA
contract_015.pdf,AI-0015,SVC-0011,Hugging Face Enterprise Hub,Hugging Face,Engineering R&D,US-East|EU-West,Model Hosting,Standard,2023-09-01,2026-08-31,480000.0,0.20,Active,60,true,false,false,true,false,true,false,SOC 2 Type II"""


EXTRACTED_CONTRACTS_CSV = _build_extracted_csv_from_manifest()


# ============================================================================
# Upload CSVs to Volume
# ============================================================================
print(f"Uploading bronze CSV files to {VOLUME_PATH}/")

for filename, content in [
    ("ai_services.csv", AI_SERVICES_CSV),
    ("ai_contracts.csv", AI_CONTRACTS_CSV),
    ("ai_spend_transactions.csv", AI_SPEND_TRANSACTIONS_CSV),
    ("extracted_contracts.csv", EXTRACTED_CONTRACTS_CSV),
]:
    filepath = f"{VOLUME_PATH}/{filename}"
    dbutils.fs.put(filepath, content.strip(), overwrite=True)
    print(f"  ✓ {filepath}")

print(f"\n✅ Bronze CSV files uploaded to {VOLUME_PATH}/")
print(f"\n📊 Data quality issues present (silver layer cleans these):")
print(f"   - BU code variants: TB-BUS, ENG, CW, MOB")
print(f"   - Duplicate service rows: SVC-0001, SVC-0002")
print(f"   - Duplicate contract rows: AI-0003, AI-0007, AI-0011")
print(f"   - Date anomaly: AI-0003 duplicate with contract_end < contract_start")
print(f"   - Null committed_spend_usd: AI-0011 duplicate row")
print(f"   - Null agreement_id: 3 transaction rows (TXN-00134/135/136)")
print(f"   - Duplicate transactions: 8 rows (TXN-00137…00144)")
print(f"\n🔄 Next: Run the Lakeflow pipeline to ingest these CSVs into bronze → silver → gold tables.")

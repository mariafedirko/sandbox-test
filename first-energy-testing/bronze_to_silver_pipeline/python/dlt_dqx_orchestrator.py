# Databricks notebook source
import dlt
import os
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk import WorkspaceClient

# ─────────────────────────────────────────────────────────────────────
# Setup — runs once at pipeline registration
# ─────────────────────────────────────────────────────────────────────

ws = WorkspaceClient()

# BUNDLE COMPATIBLE: Dynamically find where this file is executing from
# __file__ points to: .../bronze_to_silver_pipeline/python/dlt_dqx_orchestrator.py
CURRENT_DIR = os.path.dirname(
    os.path.abspath(__file__)
)  # comment if running in workspace folder not git

# Step up one level out of 'python/' to get to the root pipeline folder
PIPELINE_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
# PIPELINE_ROOT = "/Workspace/Users/maria.fedirko@lovelytics.com/sandbox-test/first-energy-testing/bronze_to_silver_pipeline"

CONTRACT_PATH = os.path.join(PIPELINE_ROOT, "contracts", "silver_contracts.yml")

# Aggregate functions are incompatible with Lakeflow's append state mechanisms
_AGGREGATE_FUNCTIONS = {"is_aggr_not_less_than", "is_aggr_not_greater_than"}

# Initialize the generative metadata contractor
generator = DQGenerator(workspace_client=ws, spark=spark)

print(f"[DQX] Reading data contract from: {CONTRACT_PATH}")
all_rules = generator.generate_rules_from_contract(
    contract_file=CONTRACT_PATH,
    generate_predefined_rules=True,
    generate_schema_validation=False,
    process_text_rules=False,
    default_criticality="error",
)

# Filter out incompatible aggregate constraints
rules = [
    r
    for r in all_rules
    if r.get("check", {}).get("function") not in _AGGREGATE_FUNCTIONS
]

print(f"[DQX] Successfully parsed {len(rules)} rules out of the contract layout:")
for r in rules:
    print(f"  - {r['name']} ({r['criticality']})")

dq_engine = DQEngine(ws)


# ─────────────────────────────────────────────────────────────────────
# Compute the split ONCE — shared between the two output tables
# ─────────────────────────────────────────────────────────────────────

# Target your SQL file location explicitly from the root folder
SQL_PATH = os.path.join(PIPELINE_ROOT, "sql", "silver_clean_users.sql")

print(f"[DQX] Reading clean SQL transformation asset from: {SQL_PATH}")
with open(SQL_PATH, "r") as f:
    SILVER_SQL = f.read().strip()  # Directly reads the pure SELECT query text


def _split_staging():
    """
    Executes the clean SQL query text and processes compliance checks
    via the metadata engine to split valid and quarantined dataframe states.
    """
    transformed_df = spark.sql(SILVER_SQL)
    good_df, bad_df = dq_engine.apply_checks_by_metadata_and_split(
        transformed_df, rules
    )
    return good_df, bad_df


# ─────────────────────────────────────────────────────────────────────
# Target Delta Lakeflow Outputs (Declarative Graph Registrations)
# ─────────────────────────────────────────────────────────────────────


@dlt.table(
    name="users_cleaned",
    comment="Clean silver table containing user records passing all contract constraints.",
    table_properties={"quality": "silver", "contract_source": "silver_contracts.yml"},
)
def silver_users():
    good_df, _ = _split_staging()
    return good_df


@dlt.table(
    name="users_quarantine",
    comment="Audit quarantine table storing records violating data contract constraints.",
    table_properties={
        "quality": "quarantine",
        "contract_source": "silver_contracts.yml",
    },
)
def quarantine_users():
    _, bad_df = _split_staging()
    return bad_df

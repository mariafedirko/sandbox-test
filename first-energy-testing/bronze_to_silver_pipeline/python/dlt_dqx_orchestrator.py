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

# Grab the workspace path of this running notebook
notebook_path = (
    dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)

# Go up two levels (out of 'python', then out of 'bronze_to_silver_pipeline') to reach the project root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(notebook_path)))

# Attach the mandatory physical filesystem prefix
BASE_DIR = os.path.join("/Workspace", project_root.lstrip("/"))

# Target your asset files cleanly (FIXED: Unified to consistent UPPERCASE constants)
CONTRACT_PATH = os.path.join(
    BASE_DIR, "bronze_to_silver_pipeline", "contracts", "silver_contracts.yml"
)
SQL_PATH = os.path.join(
    BASE_DIR, "bronze_to_silver_pipeline", "sql", "silver_clean_users.sql"
)

# # Aggregate functions are incompatible with Lakeflow's append state mechanisms
# _AGGREGATE_FUNCTIONS = {"is_aggr_not_less_than", "is_aggr_not_greater_than"}

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

# # Filter out incompatible aggregate constraints
# rules = [
#     r
#     for r in all_rules
#     if r.get("check", {}).get("function") not in _AGGREGATE_FUNCTIONS
# ]
rules = all_rules

print(f"[DQX] Successfully parsed {len(rules)} rules out of the contract layout:")
for r in rules:
    print(f"  - {r['name']} ({r['criticality']})")

dq_engine = DQEngine(ws)


# ─────────────────────────────────────────────────────────────────────
# Compute the split ONCE — shared between the two output tables
# ─────────────────────────────────────────────────────────────────────

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

# Databricks notebook source
import dlt
import os
import uuid
from datetime import datetime  # FIXED: Added missing import to prevent NameError
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk import WorkspaceClient

# ─────────────────────────────────────────────────────────────────────
# Setup — runs once at pipeline registration
# ─────────────────────────────────────────────────────────────────────

ws = WorkspaceClient()

notebook_path = (
    dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(notebook_path)))
BASE_DIR = os.path.join("/Workspace", project_root.lstrip("/"))

CONTRACT_PATH = os.path.join(
    BASE_DIR, "bronze_to_silver_pipeline", "contracts", "silver_contracts.yml"
)
SQL_PATH = os.path.join(
    BASE_DIR, "bronze_to_silver_pipeline", "sql", "silver_clean_users.sql"
)

generator = DQGenerator(workspace_client=ws, spark=spark)

print(f"[DQX] Reading data contract from: {CONTRACT_PATH}")
all_rules = generator.generate_rules_from_contract(
    contract_file=CONTRACT_PATH,
    generate_predefined_rules=True,
    generate_schema_validation=False,
    process_text_rules=False,
    default_criticality="error",
)

# ─────────────────────────────────────────────────────────────────────
# PERFORMANCE FIX: Compute Core Transformation View ONCE
# ─────────────────────────────────────────────────────────────────────

print(f"[DQX] Reading clean SQL transformation asset from: {SQL_PATH}")
with open(SQL_PATH, "r") as f:
    SILVER_SQL = f.read().strip()


@dlt.view(name="stg_transformed_users")
def stg_transformed_users():
    """
    Executes the base business logic query exactly once.
    Downstream targets read from this shared view node.
    """
    return spark.sql(SILVER_SQL)


# ─────────────────────────────────────────────────────────────────────
# Target Delta Live Tables Outputs
# ─────────────────────────────────────────────────────────────────────


@dlt.table(
    name="users_cleaned",
    comment="Clean silver table containing user records passing all contract constraints.",
)
def silver_users():
    dq_engine = DQEngine(ws)
    transformed_df = dlt.read("stg_transformed_users")
    good_df, _ = dq_engine.apply_checks_by_metadata_and_split(transformed_df, all_rules)
    return good_df


@dlt.table(
    name="users_quarantine",
    comment="Audit quarantine table storing records violating data contract constraints.",
)
def quarantine_users():
    dq_engine = DQEngine(ws)
    transformed_df = dlt.read("stg_transformed_users")
    _, bad_df = dq_engine.apply_checks_by_metadata_and_split(transformed_df, all_rules)
    return bad_df


# # ─── OFFICIAL PROGRAMMATIC APPROACH (ADAPTED FOR DLT) ───
# @dlt.table(
#     name="summary_metrics",
#     comment="Centralized summary metrics tracking for the AI/BI Quality Dashboard.",
# )
# def summary_metrics():
#     from databricks.labs.dqx.metrics_observer import DQMetricsObserver
#     import uuid  # Ensure uuid is accessible inside the block if needed

#     # 1. Initialize engine WITH the observer to gather metrics statefully
#     obs = DQMetricsObserver()
#     engine_with_obs = DQEngine(ws, observer=obs)

#     # 2. Pull the optimized staging data view
#     transformed_df = dlt.read("stg_transformed_users")

#     # FIXED: Swapped to 'apply_checks_by_metadata_and_split' to support dictionary objects
#     valid_df, invalid_df = engine_with_obs.apply_checks_by_metadata_and_split(
#         transformed_df, all_rules
#     )

#     # 3. Trigger an action to compile lazy execution state metrics
#     invalid_df.count()

#     # FIXED: Pull directly from the stateful observer instance object mapping
#     observed_data = obs.get

#     run_id = observed_data.get("run_id", str(uuid.uuid4()))
#     run_time = datetime.now()
#     output_loc = "sandbox.fed_silver.users_cleaned"
#     quarantine_loc = "sandbox.fed_silver.users_quarantine"

#     metric_records = [
#         (
#             output_loc,
#             quarantine_loc,
#             run_id,
#             run_time,
#             "input_row_count",
#             float(observed_data.get("input_row_count", 0)),
#         ),
#         (
#             output_loc,
#             quarantine_loc,
#             run_id,
#             run_time,
#             "valid_row_count",
#             float(observed_data.get("valid_row_count", 0)),
#         ),
#         (
#             output_loc,
#             quarantine_loc,
#             run_id,
#             run_time,
#             "error_row_count",
#             float(observed_data.get("error_row_count", 0)),
#         ),
#         (
#             output_loc,
#             quarantine_loc,
#             run_id,
#             run_time,
#             "warning_row_count",
#             float(observed_data.get("warning_row_count", 0)),
#         ),
#     ]

#     schema = "output_location string, quarantine_location string, run_id string, run_time timestamp, metric_name string, metric_value double"
#     return spark.createDataFrame(metric_records, schema)

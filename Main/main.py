from pyspark.sql import SparkSession

from utils import load_config, audit_record
from ingestion import ingest_data
from dq import dq_job
from scdtype1 import scdtype1
from scdtype2 import scdtype2
from transformer import silver_data
from writer import (
    write_data,
    write_dq_results,
    write_audit,
    write_doctor_revenue,
    write_disease_revenue,
    write_monthly_revenue,
    write_doctor_visits,
    write_running_revenue
)

# ---------------------------------------
# Create Spark Session
# ---------------------------------------

spark = SparkSession.builder.appName(
    "Hospital_ETL_Pipeline"
).getOrCreate()

# ---------------------------------------
# Load Config
# ---------------------------------------

config = load_config(
    "/Workspace/Users/your_email/config/dev.yaml"
)

# ---------------------------------------
# Ingestion
# ---------------------------------------

source_df = ingest_data(spark, config)

rows_read = source_df.count()

# ---------------------------------------
# DQ Validation
# ---------------------------------------

valid_df, invalid_df, dq_result_df = dq_job(
    source_df,
    config
)

rows_valid = valid_df.count()

# ---------------------------------------
# Read Existing Target
# ---------------------------------------

try:

    target_df = (
        spark.read
             .format(config["sink"]["format"])
             .option("header", True)
             .load(config["sink"]["file_path"])
    )

except Exception:

    print("Initial Load")

    target_df = valid_df.limit(0)

# ---------------------------------------
# SCD TYPE 1
# ---------------------------------------

scd1_df, scd1_updates, scd1_inserts = scdtype1(
    valid_df,
    target_df,
    config
)

# ---------------------------------------
# SCD TYPE 2
# ---------------------------------------

final_df, scd2_updates, scd2_inserts = scdtype2(
    scd1_df,
    target_df,
    config
)

# ---------------------------------------
# Transformations
# ---------------------------------------

reports = silver_data(
    final_df,
    config
)

# ---------------------------------------
# Write Final Sink
# ---------------------------------------

write_data(
    final_df,
    config
)

# ---------------------------------------
# Write DQ Results
# ---------------------------------------

write_dq_results(
    dq_result_df,
    config
)

# ---------------------------------------
# Create Audit Record
# ---------------------------------------

audit_dict = audit_record(
    pipeline_name=config["pipeline"]["name"],
    rows_read=rows_read,
    rows_valid=rows_valid,
    rows_inserted=scd2_inserts,
    rows_updated=scd2_updates,
    status="PASS"
)

audit_df = spark.createDataFrame([audit_dict])

write_audit(
    audit_df,
    config
)

# ---------------------------------------
# Write Reports
# ---------------------------------------

write_doctor_revenue(
    reports["doctor_revenue"]
)

write_disease_revenue(
    reports["disease_revenue"]
)

write_monthly_revenue(
    reports["monthly_revenue"]
)

write_doctor_visits(
    reports["doctor_visits"]
)

write_running_revenue(
    reports["running_revenue"]
)

print("--------------------------------------")
print("HOSPITAL ETL PIPELINE COMPLETED")
print("--------------------------------------")

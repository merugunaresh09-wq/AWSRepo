import yaml
from pyspark.sql import SparkSession


# ---------------------------------------------
# CREATE SPARK SESSION
# ---------------------------------------------

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("Hospital ETL Pipeline")
        .getOrCreate()
    )

    return spark


# ---------------------------------------------
# LOAD YAML CONFIG
# ---------------------------------------------

def load_config(path):

    try:

        with open(path, "r") as file:

            config = yaml.safe_load(file)

        print("Configuration Loaded Successfully.")

        return config

    except Exception as e:

        print("Error Loading Configuration File")
        print(e)

        raise


# ---------------------------------------------
# CREATE AUDIT RECORD
# ---------------------------------------------

def audit_record(
        pipeline_name,
        rows_read,
        rows_valid,
        rows_inserted,
        rows_updated,
        status
):

    return {

        "pipeline_name": pipeline_name,
        "rows_read": rows_read,
        "rows_valid": rows_valid,
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "status": status

    }


# ---------------------------------------------
# PRINT PIPELINE DETAILS
# ---------------------------------------------

def print_pipeline_info(config):

    print("\n-----------------------------------")
    print("PIPELINE CONFIGURATION")
    print("-----------------------------------")

    print(f"Pipeline Name : {config['pipeline']['name']}")
    print(f"Source Path   : {config['source']['file_path']}")
    print(f"Sink Path     : {config['sink']['file_path']}")

    print("-----------------------------------\n")


# ---------------------------------------------
# GET BUSINESS KEYS
# ---------------------------------------------

def get_business_keys(config):

    return config["business_keys"]


# ---------------------------------------------
# GET SCD TYPE 1 COLUMNS
# ---------------------------------------------

def get_scd1_columns(config):

    return config["scdtype1_columns"]


# ---------------------------------------------
# GET SCD TYPE 2 COLUMNS
# ---------------------------------------------

def get_scd2_columns(config):

    return config["scdtype2_columns"]


# ---------------------------------------------
# GET VALID ORDER STATUS VALUES
# ---------------------------------------------

def get_valid_status(config):

    return config["order_status_validation"]["valid_values"]


# ---------------------------------------------
# GET TRANSFORMATION CONFIG
# ---------------------------------------------

def get_transformation_config(config):

    return config["transformations"]

from pyspark.sql import DataFrame


def write_data(df: DataFrame, config):

    try:

        (
            df.write
              .format(config["sink"]["format"])
              .option("header", config["sink"]["header"])
              .mode(config["sink"]["mode"])
              .save(config["sink"]["file_path"])
        )

        print("=======================================")
        print("Data successfully written to Sink.")
        print("=======================================")

    except Exception as e:

        print("Error while writing data to sink.")
        print(e)
        raise


def write_dq_results(df, config):

    try:

        (
            df.write
              .format("csv")
              .option("header", True)
              .mode("overwrite")
              .save(config["dq_results"]["file_path"])
        )

        print("DQ Results successfully written.")

    except Exception as e:

        print("Error writing DQ Results.")
        print(e)
        raise


def write_audit(df, config):

    try:

        (
            df.write
              .format("csv")
              .option("header", True)
              .mode("append")
              .save(config["audit"]["file_path"])
        )

        print("Audit written successfully.")

    except Exception as e:

        print("Error writing Audit.")
        print(e)
        raise


def write_doctor_revenue(df):

    df.write \
        .format("csv") \
        .option("header", True) \
        .mode("overwrite") \
        .save("/Volumes/etl_pipeline_outputs/etl_schema/etl_volume/REPORTS/DOCTOR_REVENUE/")


def write_disease_revenue(df):

    df.write \
        .format("csv") \
        .option("header", True) \
        .mode("overwrite") \
        .save("/Volumes/etl_pipeline_outputs/etl_schema/etl_volume/REPORTS/DISEASE_REVENUE/")


def write_monthly_revenue(df):

    df.write \
        .format("csv") \
        .option("header", True) \
        .mode("overwrite") \
        .save("/Volumes/etl_pipeline_outputs/etl_schema/etl_volume/REPORTS/MONTHLY_REVENUE/")


def write_doctor_visits(df):

    df.write \
        .format("csv") \
        .option("header", True) \
        .mode("overwrite") \
        .save("/Volumes/etl_pipeline_outputs/etl_schema/etl_volume/REPORTS/DOCTOR_VISITS/")


def write_running_revenue(df):

    df.write \
        .format("csv") \
        .option("header", True) \
        .mode("overwrite") \
        .save("/Volumes/etl_pipeline_outputs/etl_schema/etl_volume/REPORTS/RUNNING_REVENUE/")

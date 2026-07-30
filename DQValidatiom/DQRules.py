from pyspark.sql.functions import *


def dq_job(df, config):

    try:

        dq_results = []

        # ---------------------------------------------
        # Read Config
        # ---------------------------------------------

        valid_status = config["order_status_validation"]["valid_values"]

        # ---------------------------------------------
        # 1. Duplicate Patient ID Test
        # ---------------------------------------------

        duplicate_count = (
            df.groupBy("patient_id")
              .count()
              .filter(col("count") > 1)
              .count()
        )

        status = "PASS"

        if duplicate_count > 0:
            status = "FAIL"

        dq_results.append({

            "TEST_NAME": "PATIENT_ID_UNIQUE_TEST",
            "FIELD_NAME": "patient_id",
            "STATUS": status,
            "FAILED_COUNT": duplicate_count

        })

        # ---------------------------------------------
        # 2. Patient Name Null Test
        # ---------------------------------------------

        patient_null_count = (

            df.filter(
                col("patient_name").isNull()
            ).count()

        )

        status = "PASS"

        if patient_null_count > 0:
            status = "FAIL"

        dq_results.append({

            "TEST_NAME": "PATIENT_NAME_NULL_TEST",
            "FIELD_NAME": "patient_name",
            "STATUS": status,
            "FAILED_COUNT": patient_null_count

        })

        # ---------------------------------------------
        # 3. Order Status Validation
        # ---------------------------------------------

        status_invalid_count = (

            df.filter(

                ~col("order_status").isin(valid_status)

            ).count()

        )

        status = "PASS"

        if status_invalid_count > 0:
            status = "FAIL"

        dq_results.append({

            "TEST_NAME": "ORDER_STATUS_VALIDATION",
            "FIELD_NAME": "order_status",
            "STATUS": status,
            "FAILED_COUNT": status_invalid_count

        })

        # ---------------------------------------------
        # 4. Treatment Cost Positive Test
        # ---------------------------------------------

        cost_negative_count = (

            df.filter(

                col("treatment_cost") < 0

            ).count()

        )

        status = "PASS"

        if cost_negative_count > 0:
            status = "FAIL"

        dq_results.append({

            "TEST_NAME": "TREATMENT_COST_POSITIVE_TEST",
            "FIELD_NAME": "treatment_cost",
            "STATUS": status,
            "FAILED_COUNT": cost_negative_count

        })

        # ---------------------------------------------
        # Valid Records
        # ---------------------------------------------

        valid_df = (

            df.filter(col("patient_name").isNotNull())

              .filter(col("treatment_cost") >= 0)

              .filter(col("order_status").isin(valid_status))

        )

        # ---------------------------------------------
        # Invalid Records
        # ---------------------------------------------

        invalid_df = (

            df.subtract(valid_df)

        )

        # ---------------------------------------------
        # DQ Result DataFrame
        # ---------------------------------------------

        spark = df.sparkSession

        dq_result_df = spark.createDataFrame(dq_results)

        print("-----------------------------------")
        print("DQ RESULTS")
        print("-----------------------------------")

        dq_result_df.show(truncate=False)

        print("-----------------------------------")
        print("VALID RECORDS")
        print("-----------------------------------")

        print(valid_df.count())

        print("-----------------------------------")
        print("INVALID RECORDS")
        print("-----------------------------------")

        print(invalid_df.count())

        return valid_df, invalid_df, dq_result_df

    except Exception as e:

        print("Error in dq.py")

        print(e)

        raise

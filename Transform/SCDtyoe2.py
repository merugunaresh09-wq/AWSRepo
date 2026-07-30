from pyspark.sql.functions import *


def scdtype2(source_df, target_df, config):

    try:

        # ---------------------------------------------------
        # Read Config
        # ---------------------------------------------------

        business_keys = config["business_keys"]

        scd2_columns = config["scdtype2_columns"]

        # ---------------------------------------------------
        # Current Records Only
        # ---------------------------------------------------

        current_target = (

            target_df

            .filter(col("is_current") == "Y")

        )

        # ---------------------------------------------------
        # Join Source and Target
        # ---------------------------------------------------

        joined_df = (

            source_df.alias("s")

            .join(

                current_target.alias("t"),

                business_keys,

                "inner"

            )

        )

        # ---------------------------------------------------
        # Build Change Condition
        # ---------------------------------------------------

        condition = None

        for column in scd2_columns:

            current_condition = (

                col(f"s.{column}") != col(f"t.{column}")

            )

            if condition is None:

                condition = current_condition

            else:

                condition = condition | current_condition

        # ---------------------------------------------------
        # Changed Records
        # ---------------------------------------------------

        changed_records = (

            joined_df

            .filter(condition)

        )

        # ---------------------------------------------------
        # Expire Old Records
        # ---------------------------------------------------

        expired_records = (

            changed_records

            .select("t.*")

            .withColumn(

                "effective_end_date",

                current_date()

            )

            .withColumn(

                "is_current",

                lit("N")

            )

        )

        # ---------------------------------------------------
        # Insert New Version
        # ---------------------------------------------------

        new_version = (

            changed_records

            .select("s.*")

            .withColumn(

                "effective_start_date",

                current_date()

            )

            .withColumn(

                "effective_end_date",

                lit(None).cast("date")

            )

            .withColumn(

                "is_current",

                lit("Y")

            )

        )

        # ---------------------------------------------------
        # Brand New Customers
        # ---------------------------------------------------

        new_records = (

            source_df.alias("s")

            .join(

                current_target.alias("t"),

                business_keys,

                "left_anti"

            )

            .withColumn(

                "effective_start_date",

                current_date()

            )

            .withColumn(

                "effective_end_date",

                lit(None).cast("date")

            )

            .withColumn(

                "is_current",

                lit("Y")

            )

        )

        # ---------------------------------------------------
        # Keep Existing Unchanged Records
        # ---------------------------------------------------

        changed_keys = (

            changed_records

            .select(*business_keys)

            .distinct()

        )

        unchanged_records = (

            target_df

            .join(

                changed_keys,

                business_keys,

                "left_anti"

            )

        )

        # ---------------------------------------------------
        # Final Output
        # ---------------------------------------------------

        final_df = (

            unchanged_records

            .unionByName(

                expired_records,

                allowMissingColumns=True

            )

            .unionByName(

                new_version,

                allowMissingColumns=True

            )

            .unionByName(

                new_records,

                allowMissingColumns=True

            )

        )

        updated_count = changed_records.count()

        inserted_count = new_records.count()

        print("--------------------------------------")
        print("SCD TYPE 2 SUMMARY")
        print("--------------------------------------")
        print(f"Updated Records : {updated_count}")
        print(f"Inserted Records : {inserted_count}")
        print("--------------------------------------")

        return final_df, updated_count, inserted_count

    except Exception as e:

        print("Error in scdtype2.py")
        print(e)
        raise

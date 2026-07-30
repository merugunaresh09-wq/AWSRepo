from pyspark.sql.functions import *


def scdtype1(source_df, target_df, config):

    try:

        # ------------------------------------------
        # Read Configuration
        # ------------------------------------------

        business_keys = config["business_keys"]

        scd1_columns = config["scdtype1_columns"]

        # ------------------------------------------
        # Join Source & Target
        # ------------------------------------------

        joined_df = (

            source_df.alias("s")

            .join(

                target_df.alias("t"),

                business_keys,

                "inner"

            )

        )

        # ------------------------------------------
        # Build Change Condition
        # ------------------------------------------

        condition = None

        for column in scd1_columns:

            current_condition = (

                col(f"s.{column}") != col(f"t.{column}")

            )

            if condition is None:

                condition = current_condition

            else:

                condition = condition | current_condition

        # ------------------------------------------
        # Updated Records
        # ------------------------------------------

        updated_records = (

            joined_df

            .filter(condition)

            .select("s.*")

        )

        # ------------------------------------------
        # New Records
        # ------------------------------------------

        new_records = (

            source_df.alias("s")

            .join(

                target_df.alias("t"),

                business_keys,

                "left_anti"

            )

        )

        # ------------------------------------------
        # Keys to Replace
        # ------------------------------------------

        changed_keys = (

            updated_records

            .select(*business_keys)

            .distinct()

        )

        # ------------------------------------------
        # Old Records to Keep
        # ------------------------------------------

        unchanged_records = (

            target_df

            .join(

                changed_keys,

                business_keys,

                "left_anti"

            )

        )

        # ------------------------------------------
        # Final Output
        # ------------------------------------------

        final_df = (

            unchanged_records

            .unionByName(

                updated_records,

                allowMissingColumns=True

            )

            .unionByName(

                new_records,

                allowMissingColumns=True

            )

        )

        updated_count = updated_records.count()

        inserted_count = new_records.count()

        print("------------------------------------")
        print("SCD TYPE 1 SUMMARY")
        print("------------------------------------")
        print(f"Updated Records : {updated_count}")
        print(f"Inserted Records : {inserted_count}")
        print("------------------------------------")

        return final_df, updated_count, inserted_count

    except Exception as e:

        print("Error in scdtype1.py")
        print(e)
        raise

from pyspark.sql.functions import *
from pyspark.sql.window import Window


def silver_data(df, config):

    try:

        # -------------------------------------------------
        # Doctor Wise Revenue
        # -------------------------------------------------

        doctor_revenue_df = (

            df.groupBy("doctor_name")

              .agg(

                  sum("treatment_cost").alias("doctor_revenue")

              )

              .orderBy(

                  col("doctor_revenue").desc()

              )

        )

        # -------------------------------------------------
        # Disease Wise Revenue
        # -------------------------------------------------

        disease_revenue_df = (

            df.groupBy("disease")

              .agg(

                  sum("treatment_cost").alias("disease_revenue")

              )

              .orderBy(

                  col("disease_revenue").desc()

              )

        )

        # -------------------------------------------------
        # Monthly Revenue
        # -------------------------------------------------

        monthly_revenue_df = (

            df.withColumn(

                "month",

                date_format(col("visit_date"), "yyyy-MM")

            )

            .groupBy("month")

            .agg(

                sum("treatment_cost").alias("monthly_revenue")

            )

            .orderBy("month")

        )

        # -------------------------------------------------
        # Most Visited Doctor
        # -------------------------------------------------

        doctor_visit_df = (

            df.groupBy("doctor_name")

              .agg(

                  count("*").alias("total_visits")

              )

              .orderBy(

                  col("total_visits").desc()

              )

        )

        # -------------------------------------------------
        # Running Revenue
        # -------------------------------------------------

        window_spec = (

            Window

            .orderBy("visit_date")

        )

        running_revenue_df = (

            df.withColumn(

                "running_revenue",

                sum("treatment_cost").over(window_spec)

            )

        )

        print("------------------------------------")
        print("Doctor Wise Revenue")
        print("------------------------------------")
        doctor_revenue_df.show()

        print("------------------------------------")
        print("Disease Wise Revenue")
        print("------------------------------------")
        disease_revenue_df.show()

        print("------------------------------------")
        print("Monthly Revenue")
        print("------------------------------------")
        monthly_revenue_df.show()

        print("------------------------------------")
        print("Most Visited Doctor")
        print("------------------------------------")
        doctor_visit_df.show()

        print("------------------------------------")
        print("Running Revenue")
        print("------------------------------------")
        running_revenue_df.show()

        return {

            "doctor_revenue": doctor_revenue_df,

            "disease_revenue": disease_revenue_df,

            "monthly_revenue": monthly_revenue_df,

            "doctor_visits": doctor_visit_df,

            "running_revenue": running_revenue_df

        }

    except Exception as e:

        print("Error in transformer.py")

        print(e)

        raise

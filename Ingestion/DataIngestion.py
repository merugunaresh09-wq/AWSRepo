from pyspark.sql.functions import *
from pyspark.sql import DataFrame


def ingest_data(spark, config):

    try:

        # =====================================================
        # Read Configuration
        # =====================================================

        source_path = config["source"]["file_path"]
        processed_path = config["processed_files"]["file_path"]

        source_format = config["source"]["format"]
        header = config["source"]["header"]
        mode = config["source"]["mode"]

        print("=" * 70)
        print("             HOSPITAL ETL - INGESTION STARTED")
        print("=" * 70)

        # =====================================================
        # Step 1 : Get Already Processed Files
        # =====================================================

        try:

            processed_files = {

                file.name

                for file in dbutils.fs.ls(processed_path)

                if file.name.endswith(".csv")

            }

        except Exception:

            processed_files = set()

        print("\nProcessed Files")

        for file in processed_files:
            print(file)

        # =====================================================
        # Step 2 : Get Landing Zone Files
        # =====================================================

        landing_files = {

            file.name

            for file in dbutils.fs.ls(source_path)

            if file.name.endswith(".csv")

        }

        print("\nLanding Zone Files")

        for file in landing_files:
            print(file)

        # =====================================================
        # Step 3 : Find Incremental Files
        # =====================================================

        new_files = list(

            landing_files - processed_files

        )

        print("\nIncremental Files")

        for file in new_files:
            print(file)

        if len(new_files) == 0:

            print("\nNo New Files Available.")

            return None

        # =====================================================
        # Step 4 : Read Incremental Files
        # =====================================================

        file_paths = [

            f"{source_path}/{file}"

            for file in new_files

        ]

        source_df = (

            spark.read

            .format(source_format)

            .option("header", header)

            .option("mode", mode)

            .load(file_paths)

            .withColumn(

                "load_timestamp",

                current_timestamp()

            )

            .withColumn(

                "source_file",

                input_file_name()

            )

        )

        print("\nSource Data")

        source_df.show(5, False)

        print("\nRows Read :", source_df.count())

        # =====================================================
        # Step 5 : Move Files to Processed Folder
        # =====================================================

        print("\nMoving Files to Processed Folder")

        for file in new_files:

            dbutils.fs.mv(

                f"{source_path}/{file}",

                f"{processed_path}/{file}"

            )

            print(file, "Moved Successfully")

        print("\nIncremental Load Completed Successfully")

        return source_df

    except Exception as e:

        print("=" * 60)
        print("ERROR IN ingestion.py")
        print("=" * 60)
        print(e)
        raise

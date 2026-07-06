"""
ETL Orchestration Job

This module coordinates the complete pipeline lifecycle for processing daily sales data:
1. Validates input CSVs in local storage and downloads new files from S3 if none are present.
2. Performs schema verification against mandatory columns.
3. Segregates invalid CSVs, moving them to local/S3 error locations.
4. Triggers MySQL staging database updates to keep execution state consistent.
5. Employs Spark to ingest, clean, and enrich datasets with dimension tables.
6. Publishes aggregated Customer and Sales Team marts back to Local/S3 storage.
7. Computes business calculations and inserts results back into MySQL data mart tables.
8. Cleans up temporary resources.
"""

import os
import sys
import shutil
import datetime
import time
from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.audit_logger import AuditLogger
from src.main.read.aws_read import S3Reader
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter
from pyspark.sql.functions import col, lit, date_format, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
from src.main.utility.file_audit_logger import FileAuditLogger
# ---------------------------------------------------------------------------
# Audit Bootstrap
# ---------------------------------------------------------------------------
# AuditLogger is instantiated at module level so it is accessible from both
# the main body and the top-level except block below.
# Metric accumulators use descriptive per-mart names — not a single aggregated
# 'rows_written' — so each mart's output can be monitored independently.
audit_logger = AuditLogger(
    job_name="etl_pipeline",
    pipeline_version=config.pipeline_version,
    environment=config.environment,
    trigger_type=config.trigger_type,
)


job_id = audit_logger.start_job(...)

file_audit_logger = FileAuditLogger(job_id)

# Audit metrics accumulators — initialised to 0 so fail_job() always has
# valid values regardless of how early an exception is thrown.
_audit_files_processed          = 0
_audit_rows_read                = 0
_audit_customer_mart_rows       = 0
_audit_sales_mart_rows          = 0
_audit_files_rejected           = 0

# Tracks the active pipeline stage name for failure diagnostics.
# Updated at the start of each named stage inside the try block.
_failure_stage = "INITIALIZING"

# ---------------------------------------------------------------------------
# Audit: Record job start BEFORE contacting any external system (S3 or Spark).
# This guarantees that even S3 credential failures or Spark startup OOMs
# produce an audit record. start_job() returns None if the audit DB itself
# is unreachable — all downstream audit calls handle None gracefully.
# ---------------------------------------------------------------------------
_pipeline_start = time.monotonic()
job_id = audit_logger.start_job()

try:
    # -----------------------------------------------------------------------
    # Stage: S3 Initialization
    # -----------------------------------------------------------------------
    _failure_stage = "S3_INITIALIZATION"
    logger.info("Initializing S3 Client Provider...")

    s3_client_provider = S3ClientProvider(
        decrypt(config.aws_access_key),
        decrypt(config.aws_secret_key)
    )
    s3_client = s3_client_provider.get_client()

    # Check S3 connectivity and print bucket list
    response = s3_client.list_buckets()
    logger.info("S3 Buckets connection verified. List of Buckets: %s", response['Buckets'])

    # -----------------------------------------------------------------------
    # Stage: Spark Initialization
    # -----------------------------------------------------------------------
    _failure_stage = "SPARK_INITIALIZATION"
    logger.info("Initializing Spark Session...")
    spark = spark_session()
    logger.info("Spark Session successfully initialized.")

    # Update the RUNNING audit record with the Spark Application ID now that
    # the session is live. This is a separate UPDATE (not part of start_job)
    # because applicationId is only available after getOrCreate() returns.
    audit_logger.update_spark_app_id(job_id, spark.sparkContext.applicationId)

    _failure_stage = "PRE_EXECUTION_CHECK"

    # Check if the local directory contains CSV files left over from a previous execution failure.
    # We resolve absolute paths immediately to avoid working directory mismatches downstream.
    os.makedirs(config.local_directory, exist_ok=True)
    csv_files = [
        os.path.abspath(os.path.join(config.local_directory, file))
        for file in os.listdir(config.local_directory)
        if file.endswith(".csv")
    ]

    # Database Pre-Execution Verification
    connection = None
    cursor = None

    if csv_files:
        try:
            connection = get_mysql_connection()
            cursor = connection.cursor()
            total_csv_files = [os.path.basename(f) for f in csv_files]

            # Query if any of the existing local files are still registered with active status ('A').
            placeholders = ",".join(["%s"] * len(total_csv_files))
            statement = (
                f"SELECT DISTINCT file_name "
                f"FROM {config.database_name}.{config.product_staging_table} "
                f"WHERE file_name IN ({placeholders}) AND status='A'"
            )
            logger.info("Pre-execution check query: %s | params: %s", statement, total_csv_files)
            cursor.execute(statement, total_csv_files)
            data = cursor.fetchall()

            if data:
                logger.warning("Your last run failed. Re-processing current local files: %s", csv_files)
            else:
                logger.error("Local files are present but not registered as Active ('A') in staging. Halting to avoid corrupting data.")
                raise Exception("Pre-execution check failed: local files present but not active.")
        except Exception as e:
            logger.error("Failed executing pre-run check: %s", e)
            raise e
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    else:
        logger.info("Last run was successful. Initiating file list and download from S3 source directory.")

        # List files available in S3 source path
        try:
            s3_reader = S3Reader()
            folder_path = config.s3_source_directory

            s3_absolute_file_path = s3_reader.list_files(
                s3_client,
                config.bucket_name,
                folder_path=folder_path
            )

            logger.info("Absolute paths on S3 bucket: %s", s3_absolute_file_path)

            if not s3_absolute_file_path:
                logger.info("No files available at S3 prefix %s", folder_path)
                raise Exception("No data available to process")

        except Exception as e:
            logger.error("Error reading S3 source directory: %s", e)
            raise e

        # Extract target relative keys for download by removing the bucket prefix
        prefix = f"s3://{config.bucket_name}/"
        file_paths = [url[len(prefix):] for url in s3_absolute_file_path]

        logger.info("Identified relative S3 keys for download: %s", file_paths)

        # Download source files from S3 to local directory
        try:
            downloader = S3FileDownloader(
                s3_client,
                config.bucket_name,
                config.local_directory
            )
            downloader.download_files(file_paths)
        except Exception as e:
            logger.error("File download error: %s", e)
            # sys.exit() raises SystemExit which does NOT inherit from Exception,
            # causing the outer except block to be bypassed and fail_job() to
            # never be called. Replace with RuntimeError so the outer handler
            # captures the failure correctly.
            raise RuntimeError(f"File download failed: {e}") from e

        # Re-evaluate the local files present in the download directory
        all_files = os.listdir(config.local_directory)
        logger.info("List of files present at local directory after S3 download: %s", all_files)

        csv_files = []
        error_files = []

        for file in all_files:
            abs_path = os.path.abspath(os.path.join(config.local_directory, file))
            if file.endswith(".csv"):
                csv_files.append(abs_path)
            else:
                error_files.append(abs_path)

        if not csv_files:
            logger.error("No CSV data available to process")
            raise Exception("No CSV data available to process")

        if error_files:
            logger.warning("Non-CSV files found locally: %s", error_files)

        logger.info("List of CSV files prepared for processing: %s", csv_files)


    # Schema Validation and Cleaning Phase
    _failure_stage = "SCHEMA_VALIDATION"
    logger.info("************ Checking Schema for data loaded in S3 ************")

    correct_files = []
    error_files = []

    for data in csv_files:
        try:
            # Load CSV columns (using string types implicitly to fetch headers efficiently)
            data_schema = (
                spark.read.format("csv")
                .option("header", "true")
                .load(data)
                .columns
            )
            logger.info("Schema for the file %s is %s", data, data_schema)
            logger.info("Mandatory columns expected: %s", config.mandatory_columns)

            missing_columns = set(config.mandatory_columns) - set(data_schema)
            logger.info("Missing columns for file %s: %s", data, missing_columns)

            if missing_columns:
                error_files.append(data)
            else:
                logger.info("No missing columns for file %s", data)
                correct_files.append(data)
        except Exception as e:
            logger.error("Error reading file schema for %s: %s", data, e)
            error_files.append(data)

    logger.info("************ List of correct files ************ %s", correct_files)
    logger.info("************ List of error files ************ %s", error_files)

    # Audit: capture count of files rejected during schema validation.
    # The column is named files_rejected (not rows_rejected) because files that
    # fail header validation cannot yield a row count without a full re-read.
    # Storing a file count here is accurate and avoids misleading metric names.
    _audit_files_rejected = len(error_files)


    # Handle invalid files by moving them locally and on S3
    logger.info("************ Moving Error data to error directory if any ************")
    error_folder_local_path = config.error_folder_path_local

    if error_files:
        for file_path in error_files:
            if os.path.exists(file_path):
                file_name = os.path.basename(file_path)
                destination_path = os.path.join(error_folder_local_path, file_name)

                # Ensure destination folder exists before moving
                os.makedirs(error_folder_local_path, exist_ok=True)
                shutil.move(file_path, destination_path)
                logger.info("Moved local error file '%s' to '%s'.", file_name, destination_path)

                source_prefix = config.s3_source_directory
                destination_prefix = config.s3_error_directory

                # Fixed the bug where move_files module was instantiated incorrectly.
                # We call the imported move_s3_to_s3 function directly.
                try:
                    message = move_s3_to_s3(
                        s3_client,
                        config.bucket_name,
                        source_prefix,
                        destination_prefix,
                        file_name
                    )
                    logger.info("S3 error move message: %s", message)
                except Exception as s3_err:
                    logger.error("Failed to move S3 object %s to error prefix: %s", file_name, s3_err)
            else:
                logger.error("File '%s' does not exist.", file_path)
    else:
        logger.info("************ There are no error files available in our dataset ************")


    # Register correct files to Staging table with active ('A') status
    _failure_stage = "STAGING_REGISTRATION"
    logger.info("************ Updating the product_staging_table that we have started ************")
    db_name = config.database_name
    current_date = datetime.datetime.now()
    formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

    if correct_files:
        insert_sql = (
            f"INSERT INTO {db_name}.{config.product_staging_table} "
            f"(file_name, file_location, created_date, status) VALUES (%s, %s, %s, 'A')"
        )
        insert_rows = [(os.path.basename(f), f, formatted_date) for f in correct_files]

        logger.info("Registering %d file(s) into staging table.", len(correct_files))

        connection = None
        cursor = None
        try:
            logger.info("************ Connecting with MySQL server ************")
            connection = get_mysql_connection()
            cursor = connection.cursor()
            logger.info("************ MySQL server connected successfully ************")

            cursor.executemany(insert_sql, insert_rows)
            connection.commit()
            logger.info("************ Staging table updated successfully ************")
        except Exception as e:
            logger.error("Database connection/write error during staging registration: %s. Rolling back transaction.", e)
            if connection:
                connection.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    else:
        logger.error("************ There are no files to process ************")
        raise Exception("************ No Data available with correct files ************")


    # Core Spark ingestion and schema harmonization
    _failure_stage = "SPARK_INGESTION"
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("sales_date", DateType(), True),
        StructField("sales_person_id", IntegerType(), True),
        StructField("price", FloatType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_cost", FloatType(), True),
        StructField("additional_column", StringType(), True)
    ])

   # Replaced database JDBC dataframe loading for empty_df_create_table
    # with local empty RDD creation to eliminate JDBC dependency.
    final_df_to_process = spark.createDataFrame(
        spark.sparkContext.emptyRDD(),
        schema
    )

    # Stores only successfully processed files.
    processed_files = []

    for data in correct_files:

        file_name = os.path.basename(data)

        # ------------------------------------------------------------------
        # Start File Audit
        # ------------------------------------------------------------------
        file_audit_id, file_start_time = file_audit_logger.start_file(
            file_name=file_name,
            file_path=data
        )

        try:

            # Read CSV file
            data_df = (
                spark.read.format("csv")
                .option("header", "true")
                .load(data)
            )

            # Identify additional columns present in source
            data_schema = data_df.columns

            extra_columns = list(
                set(data_schema) -
                set(config.mandatory_columns)
            )

            logger.info(
                "Extra columns present in '%s': %s",
                file_name,
                extra_columns
            )

            if extra_columns:

                data_df = data_df.withColumn(
                    "additional_column",
                    lit(",".join(extra_columns))
                )

            else:

                data_df = data_df.withColumn(
                    "additional_column",
                    lit(None).cast(StringType())
                )

            # Match dataframe schema with target schema
            data_df = data_df.select(
                col("customer_id").cast(IntegerType()),
                col("store_id").cast(IntegerType()),
                col("product_name").cast(StringType()),
                to_date(
                    col("sales_date"),
                    "yyyy-MM-dd"
                ).alias("sales_date"),
                col("sales_person_id").cast(IntegerType()),
                col("price").cast(FloatType()),
                col("quantity").cast(IntegerType()),
                col("total_cost").cast(FloatType()),
                col("additional_column").cast(StringType())
            )

            # Merge into final dataframe
            final_df_to_process = final_df_to_process.unionByName(data_df)

            # File level metrics
            row_count = data_df.count()

            file_audit_logger.finish_file(
                file_audit_id=file_audit_id,
                start_time=file_start_time,
                rows_read=row_count,
                rows_written=row_count,
                rows_rejected=0
            )

            processed_files.append(data)

            logger.info(
                "Successfully processed file '%s'.",
                file_name
            )

        except Exception as e:

            logger.exception(
                "Error processing file '%s'. Skipping file.",
                file_name
            )

            file_audit_logger.fail_file(
                file_audit_id=file_audit_id,
                start_time=file_start_time,
                error_message=str(e),
                failure_stage="FILE_PROCESSING"
            )

            # Prevent duplicate entries
            if data not in error_files:
                error_files.append(data)

            continue


    logger.info(
        "Source ingestion completed successfully. "
        "Processed %d of %d files.",
        len(processed_files),
        len(correct_files)
    )

    # ------------------------------------------------------------------
    # Job Audit Metrics
    # ------------------------------------------------------------------
    _audit_files_processed = len(processed_files)

    # Count rows only if data exists.
    if processed_files:
        _audit_rows_read = final_df_to_process.count()
    else:
        _audit_rows_read = 0


    # ------------------------------------------------------------
    # Create dataframe for all dimension tables
    # ------------------------------------------------------------
    _failure_stage = "DIMENSION_JOIN"
    database_client = DatabaseReader(config.url, config.properties)

    logger.info("**************** Loading customer table into customer_table_df ****************")
    customer_table_df = database_client.create_dataframe(
        spark,
        config.customer_table_name
    )



    logger.info("**************** Loading sales team table into sales_team_table_df ****************")
    sales_team_table_df = database_client.create_dataframe(
        spark,
        config.sales_team_table
    )

    logger.info("**************** Loading store table into store_table_df ****************")
    store_table_df = database_client.create_dataframe(
        spark,
        config.store_table
    )

    # ------------------------------------------------------------
    # Join with dimension tables
    # ------------------------------------------------------------
    _t0 = time.monotonic()
    s3_customer_store_sales_df_join = dimesions_table_join(
        final_df_to_process,
        customer_table_df,
        store_table_df,
        sales_team_table_df
    ).cache()

    _enriched_rows = s3_customer_store_sales_df_join.count()
    logger.info("Dimension join complete in %.2fs: %d enriched row(s).", time.monotonic() - _t0, _enriched_rows)

    # Audit: rows_read = total rows in the enriched DataFrame after all joins.
    _audit_rows_read = _enriched_rows


    # Customer Data Mart Creation
    _failure_stage = "CUSTOMER_MART_WRITE"
    logger.info("**************** Write the data into Customer Data Mart ****************")

    # Modified selection strings to use col("ct.*") function bindings to resolve join column ambiguity.
    final_customer_data_mart_df = (
        s3_customer_store_sales_df_join
            .select(
                col("ct.customer_id"),
                col("ct.first_name"),
                col("ct.last_name"),
                col("ct.address"),
                col("ct.pincode"),
                col("phone_number"),
                col("sales_date"),
                col("total_cost")
            )
    )

    # Audit: capture customer mart row count (reuses the already-triggered action).
    # Stored in its own accumulator — NOT added to a shared total — so customer
    # mart output can be monitored independently of sales mart output.
    _audit_customer_mart_rows = final_customer_data_mart_df.count()
    logger.info("Customer data mart prepared: %d row(s).", _audit_customer_mart_rows)

    # Write customer data mart to disk
    parquet_writer = ParquetWriter("overwrite", "parquet")
    parquet_writer.dataframe_writer(
        final_customer_data_mart_df,
        config.customer_data_mart_local_file
    )

    logger.info(
        "**************** customer data written to local disk at %s",
        config.customer_data_mart_local_file
    )

    # Upload Customer Data Mart to S3
    logger.info("**************** Data Movement from local to S3 for customer data mart ****************")
    s3_uploader = UploadToS3(s3_client)
    s3_directory = config.s3_customer_datamart_directory

    message = s3_uploader.upload_to_s3(
        s3_directory,
        config.bucket_name,
        config.customer_data_mart_local_file
    )
    logger.info("%s", message)


    # Sales Team Data Mart Creation
    _failure_stage = "SALES_MART_WRITE"
    logger.info("**************** Write the data into sales team Data Mart ****************")

    final_sales_team_data_mart_df = (
        s3_customer_store_sales_df_join
            .select(
                col("store_id"),
                col("sales_person_id"),
                col("sales_person_first_name"),
                col("sales_person_last_name"),
                col("store_manager_name"),
                col("manager_id"),
                col("is_manager"),
                col("sales_person_address"),
                col("sales_person_pincode"),
                col("sales_date"),
                col("total_cost"),
                # date_format is Catalyst-native for DateType; avoids implicit string cast
                date_format(col("sales_date"), "yyyy-MM").alias("sales_month")
            )
    )

    # Audit: capture sales mart row count independently.
    # Keeping this separate from customer mart gives accurate per-mart visibility.
    _audit_sales_mart_rows = final_sales_team_data_mart_df.count()
    logger.info("Sales team data mart prepared: %d row(s).", _audit_sales_mart_rows)

    # Write sales data mart to local disk
    parquet_writer.dataframe_writer(
        final_sales_team_data_mart_df,
        config.sales_team_data_mart_local_file
    )

    logger.info(
        "**************** sales team data written to local disk at %s",
        config.sales_team_data_mart_local_file
    )

    # Upload Sales Team Data Mart to S3
    s3_directory = config.s3_sales_datamart_directory
    message = s3_uploader.upload_to_s3(
        s3_directory,
        config.bucket_name,
        config.sales_team_data_mart_local_file
    )
    logger.info("%s", message)

    # Write partitioned Sales Team Data Mart locally
    final_sales_team_data_mart_df.write \
        .format("parquet") \
        .option("header", "true") \
        .mode("overwrite") \
        .partitionBy("sales_month", "store_id") \
        .option(
            "path",
            config.sales_team_data_mart_partitioned_local_file
        ) \
        .save()


    # Upload Partitioned Files to S3
    s3_prefix = "sales_partitioned_data_mart"
    current_epoch = int(datetime.datetime.now().timestamp()) * 1000

    for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
        for file in files:
            # Avoid uploading Spark metadata or hidden files
            if file.startswith(".") or file.startswith("_"):
                continue

            local_file_path = os.path.join(root, file)
            relative_file_path = os.path.relpath(
                local_file_path,
                config.sales_team_data_mart_partitioned_local_file
            )
            s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path.replace(os.sep, '/')}"

            logger.info("Uploading partitioned local file %s to key %s", local_file_path, s3_key)
            s3_client.upload_file(
                local_file_path,
                config.bucket_name,
                s3_key
            )


    # Calculations and MySQL Mart Persistence
    # Calculation for customer mart: total purchase amount by customer per month
    _failure_stage = "MYSQL_MART_CALCULATION"
    logger.info("*********** Calculating customer every month purchased amount ***********")
    _t0 = time.monotonic()
    customer_mart_calculation_table_write(final_customer_data_mart_df)
    logger.info("*********** Customer mart write complete in %.2fs. ***********", time.monotonic() - _t0)

    # Calculation for sales team mart: total sales and performer incentives
    logger.info("*********** Calculating sales every month billed amount ***********")
    # Fixed bug: Passed final_sales_team_data_mart_df instead of final_customer_data_mart_df
    _t0 = time.monotonic()
    sales_mart_calculation_table_write(final_sales_team_data_mart_df)
    logger.info("*********** Sales team mart write complete in %.2fs. **********", time.monotonic() - _t0)


    # Move ONLY successfully processed files to the S3 processed prefix.
    # Targeting each file individually prevents archiving files that arrived
    # during this run's execution window and were never processed.
    _failure_stage = "FILE_ARCHIVAL"
    source_prefix = config.s3_source_directory
    destination_prefix = config.s3_processed_directory

    for file in correct_files:
        filename = os.path.basename(file)
        logger.info("Moving processed S3 file '%s' to processed prefix.", filename)
        message = move_s3_to_s3(
            s3_client,
            config.bucket_name,
            source_prefix,
            destination_prefix,
            filename
        )
        logger.info("%s", message)


    # Cleanup local temporary files generated during this ETL execution runs
    _failure_stage = "LOCAL_CLEANUP"
    logger.info("******** Deleting local source files ********")
    delete_local_file(config.local_directory)
    logger.info("******** Local source files deleted successfully ********")

    logger.info("******** Deleting local Customer Data Mart files ********")
    delete_local_file(config.customer_data_mart_local_file)
    logger.info("******** Customer Data Mart files deleted successfully ********")

    logger.info("******** Deleting local Sales Team Data Mart files ********")
    delete_local_file(config.sales_team_data_mart_local_file)
    logger.info("******** Sales Team Data Mart files deleted successfully ********")

    logger.info("******** Deleting local partitioned Sales Team Data Mart files ********")
    delete_local_file(config.sales_team_data_mart_partitioned_local_file)
    logger.info("******** Partitioned Sales Team Data Mart files deleted successfully ********")

    # All consumers of the enriched DataFrame are done; release Spark cache.
    s3_customer_store_sales_df_join.unpersist()
    logger.info("Cached enriched DataFrame released from memory.")


    # Update staging registry database records status to Inactive ('I') for processed files
    _failure_stage = "STAGING_COMPLETION"
    if correct_files:
        update_sql = (
            f"UPDATE {db_name}.{config.product_staging_table} "
            f"SET status='I', updated_date=%s WHERE file_name=%s"
        )
        update_rows = [(formatted_date, os.path.basename(f)) for f in correct_files]

        logger.info("Marking %d file(s) as Inactive in staging table.", len(correct_files))

        connection = None
        cursor = None
        try:
            logger.info("******** Connecting to MySQL Server ********")
            connection = get_mysql_connection()
            cursor = connection.cursor()
            logger.info("******** MySQL connection established successfully ********")

            cursor.executemany(update_sql, update_rows)
            connection.commit()
            logger.info("******** Product Staging table updated successfully ********")
        except Exception as e:
            logger.error("Failed updating staging table to inactive status: %s. Rolling back transaction.", e)
            if connection:
                connection.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    else:
        logger.error("******** No valid files available to update the staging table ********")
        # Replace sys.exit(1) — SystemExit bypasses except Exception, so
        # fail_job() would never be called. RuntimeError propagates correctly.
        raise RuntimeError("No valid files available to update the staging table.")

    logger.info(
        "ETL pipeline completed successfully in %.2fs. Files processed: %d, Files rejected: %d.",
        time.monotonic() - _pipeline_start,
        len(correct_files),
        len(error_files)
    )

    # ---------------------------------------------------------------------------
    # Audit: SUCCESS — all pipeline steps completed without exception.
    # ---------------------------------------------------------------------------
    audit_logger.finish_job(
        job_id=job_id,
        metrics={
            "files_processed":            _audit_files_processed,
            "rows_read":                  _audit_rows_read,
            "customer_mart_rows_written": _audit_customer_mart_rows,
            "sales_mart_rows_written":    _audit_sales_mart_rows,
            "files_rejected":             _audit_files_rejected,
        }
    )

except Exception as _pipeline_exception:
    # ---------------------------------------------------------------------------
    # Audit: FAILED — an unhandled exception escaped the pipeline.
    # We capture the full traceback so ops teams can diagnose root cause
    # directly from the audit table without needing to tail log files.
    # After recording the failure, we re-raise so the process exits with a
    # non-zero code — critical for schedulers (Airflow, AWS Glue, cron).
    # ---------------------------------------------------------------------------
    import traceback as _tb
    _full_error = _tb.format_exc()
    logger.error("ETL pipeline FAILED at stage '%s'. Updating audit record. Error: %s", _failure_stage, _pipeline_exception)
    audit_logger.fail_job(
        job_id=job_id,
        error_message=_full_error,
        failure_stage=_failure_stage,
        metrics={
            "files_processed":            _audit_files_processed,
            "rows_read":                  _audit_rows_read,
            "customer_mart_rows_written": _audit_customer_mart_rows,
            "sales_mart_rows_written":    _audit_sales_mart_rows,
            "files_rejected":             _audit_files_rejected,
        }
    )
    raise

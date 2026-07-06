"""
audit_logger.py
================

Job audit logger for the ETL pipeline.

Writes a RUNNING record when a job starts (before any external system is
contacted), updates it with the Spark application ID once available, and
finalizes it as SUCCESS or FAILED with metrics. Audit failures are always
logged and swallowed -- they must never interrupt the ETL pipeline.

Usage
-----
    audit_logger = AuditLogger(job_name="etl_pipeline", pipeline_version="1.0.0",
                                environment="PROD", trigger_type="AIRFLOW")
    job_id = audit_logger.start_job()
    try:
        spark = spark_session()
        audit_logger.update_spark_app_id(job_id, spark.sparkContext.applicationId)
        # ... pipeline body ...
        audit_logger.finish_job(job_id, metrics={...})
    except Exception:
        audit_logger.fail_job(job_id, traceback.format_exc(), failure_stage="SPARK_INIT")
        raise
"""

import datetime
import traceback

from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection

_AUDIT_TABLE = "job_audit_log"

_METRIC_KEYS = (
    "files_processed",
    "rows_read",
    "customer_mart_rows_written",
    "sales_mart_rows_written",
    "files_rejected",
)


class AuditLogger:
    """
    Manages INSERT/UPDATE operations on the ``job_audit_log`` table.

    All public methods are fault-tolerant: failures are logged, never raised,
    so audit infrastructure can never bring down the pipeline it observes.

    Parameters
    ----------
    job_name : str
        Human-readable pipeline identifier, e.g. ``'etl_pipeline'``.
    pipeline_version : str
        Semantic version of the pipeline code.
    environment : str
        One of ``'DEV'``, ``'TEST'``, ``'PROD'`` (per DDL ENUM).
    trigger_type : str
        One of ``'MANUAL'``, ``'AIRFLOW'``, ``'CRON'``, ``'API'``, ``'TEST'``.
    """

    def __init__(self, job_name, pipeline_version="1.0.0", environment="DEV",
                 trigger_type="MANUAL"):
        if not job_name or not job_name.strip():
            raise ValueError("AuditLogger: 'job_name' must be a non-empty string.")
        self.job_name = job_name.strip()
        self.pipeline_version = pipeline_version
        self.environment = environment
        self.trigger_type = trigger_type

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start_job(self):
        """
        Insert a RUNNING audit record and return the AUTO_INCREMENT job_id.

        Call this before touching any external system (S3, Spark) so that
        even initialization failures are captured. ``spark_application_id``
        stays NULL until ``update_spark_app_id()`` is called.

        Returns
        -------
        int or None
            job_id on success, None if the audit DB is unreachable.
        """
        start_time = datetime.datetime.now()
        logger.info(
            "[AuditLogger] Starting audit for job '%s' v%s | env=%s | trigger=%s",
            self.job_name, self.pipeline_version, self.environment, self.trigger_type,
        )
        sql = (
            f"INSERT INTO `{_AUDIT_TABLE}` "
            f"(job_name, pipeline_version, environment, trigger_type, "
            f" start_time, status, created_date) "
            f"VALUES (%s, %s, %s, %s, %s, 'RUNNING', %s)"
        )
        params = (
            self.job_name, self.pipeline_version, self.environment,
            self.trigger_type, start_time, start_time,
        )
        return self._run(sql, params, context="start_job", mode="insert")

    def update_spark_app_id(self, job_id, spark_app_id):
        """No-op if job_id is None. Records the Spark application ID."""
        if job_id is None:
            logger.warning("[AuditLogger] update_spark_app_id skipped: job_id is None.")
            return
        logger.info("[AuditLogger] Recording spark_application_id=%s for job_id=%s.",
                     spark_app_id, job_id)
        sql = f"UPDATE `{_AUDIT_TABLE}` SET spark_application_id = %s WHERE job_id = %s"
        self._run(sql, (spark_app_id, job_id), context="update_spark_app_id", mode="update")

    def finish_job(self, job_id, metrics=None):
        """
        Mark the job SUCCESS and record final metrics.

        Recognised ``metrics`` keys (default 0 if absent): files_processed,
        rows_read, customer_mart_rows_written, sales_mart_rows_written,
        files_rejected.
        """
        self._finalize(job_id, status="SUCCESS", metrics=metrics)

    def fail_job(self, job_id, error_message, failure_stage=None, metrics=None):
        """
        Mark the job FAILED with error details and any partial metrics.

        ``error_message`` should be ``traceback.format_exc()``; it is
        truncated to 65,000 chars to stay within the TEXT column limit.
        The caller MUST re-raise the original exception after this call so
        the process exits non-zero (schedulers detect failure via exit code).
        """
        truncated_error = (error_message or "")[:65000]
        self._finalize(job_id, status="FAILED", failure_stage=failure_stage,
                        error_message=truncated_error, metrics=metrics)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _finalize(self, job_id, status, failure_stage=None, error_message=None, metrics=None):
        """Shared SUCCESS/FAILED update logic for finish_job/fail_job."""
        if job_id is None:
            logger.warning("[AuditLogger] finalize(%s) skipped: job_id is None.", status)
            return

        end_time = datetime.datetime.now()
        safe_metrics = self._build_metrics(metrics)
        execution_seconds = self._get_execution_seconds(job_id, end_time)

        log_fn = logger.info if status == "SUCCESS" else logger.error
        log_fn(
            "[AuditLogger] Finalizing job_id=%s | status=%s | stage=%s | "
            "metrics=%s | execution_time=%.3fs",
            job_id, status, failure_stage, safe_metrics, execution_seconds,
        )

        sql = (
            f"UPDATE `{_AUDIT_TABLE}` SET "
            f"  status = %s, end_time = %s, execution_time_seconds = %s, "
            f"  failure_stage = %s, error_message = %s, "
            f"  files_processed = %s, rows_read = %s, "
            f"  customer_mart_rows_written = %s, sales_mart_rows_written = %s, "
            f"  files_rejected = %s "
            f"WHERE job_id = %s"
        )
        params = (
            status, end_time, execution_seconds, failure_stage, error_message,
            safe_metrics["files_processed"], safe_metrics["rows_read"],
            safe_metrics["customer_mart_rows_written"],
            safe_metrics["sales_mart_rows_written"], safe_metrics["files_rejected"],
            job_id,
        )
        self._run(sql, params, context=f"finalize({status})", mode="update")

    @staticmethod
    def _build_metrics(metrics):
        """Return a complete metrics dict; missing/None values default to 0."""
        safe = {key: 0 for key in _METRIC_KEYS}
        if metrics:
            for key in _METRIC_KEYS:
                if metrics.get(key) is not None:
                    safe[key] = int(metrics[key])
        return safe

    def _get_execution_seconds(self, job_id, end_time):
        """
        Compute elapsed seconds using start_time fetched from the DB (not an
        in-memory value), so it's correct even if this object was
        re-instantiated mid-run. Returns 0.0 on any failure.
        """
        sql = f"SELECT start_time FROM `{_AUDIT_TABLE}` WHERE job_id = %s"
        row = self._run(sql, (job_id,), context="_get_execution_seconds", mode="select")
        if row and row[0]:
            return round((end_time - row[0]).total_seconds(), 3)
        return 0.0

    def _run(self, sql, params, context, mode):
        """
        Execute a single parameterized statement and manage the connection
        lifecycle (commit/rollback/close). Never raises -- errors are logged
        and swallowed so the ETL pipeline is unaffected by audit DB issues.

        All SQL text contains only hardcoded column/table names; runtime
        values are bound exclusively through ``params``, eliminating SQL
        injection risk.

        Parameters
        ----------
        mode : {'insert', 'update', 'select'}
            'insert' returns cursor.lastrowid, 'update' returns None,
            'select' returns cursor.fetchone().
        """
        connection = cursor = None
        try:
            connection = get_mysql_connection()
            cursor = connection.cursor()
            cursor.execute(sql, params)

            if mode == "select":
                return cursor.fetchone()

            connection.commit()
            if mode == "insert":
                job_id = cursor.lastrowid
                logger.info("[AuditLogger] %s: audit record created with job_id=%s.",
                            context, job_id)
                return job_id

            logger.info("[AuditLogger] %s: audit record updated successfully.", context)
            return None
        except Exception as exc:
            logger.error(
                "[AuditLogger] %s: operation failed. Pipeline continues. Error: %s",
                context, exc, exc_info=True,
            )
            if connection and mode != "select":
                try:
                    connection.rollback()
                except Exception:
                    pass
            return None
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
"""
file_audit_logger.py
=====================

Per-file audit logger for the ETL pipeline. Tracks individual file
processing outcomes in the ``file_audit_log`` table, optionally linked to a
parent ``job_audit_log`` record via ``job_id``.

Schema (``file_audit_log``)::

    file_audit_id           BIGINT AUTO_INCREMENT PRIMARY KEY
    job_id                  BIGINT NULL              -- FK to job_audit_log.job_id
    file_name               VARCHAR(255)
    source_file_path        VARCHAR(1024) NULL
    source_system           VARCHAR(50) NULL
    file_size_bytes         BIGINT NULL
    status                  ENUM('RUNNING','SUCCESS','FAILED')
    start_time              DATETIME
    end_time                DATETIME NULL
    processing_time_seconds DECIMAL(10,3) NULL
    rows_read               INT NULL
    rows_written            INT NULL
    rows_rejected           INT NULL
    failure_stage           VARCHAR(100) NULL
    error_message           TEXT NULL
    created_date            DATETIME

Same fault-tolerance contract as ``audit_logger.py``: audit failures are
logged and swallowed, never raised, so the ETL pipeline is unaffected.

Usage
-----
    file_audit_logger = FileAuditLogger(job_id=job_id)
    file_audit_id, start_time = file_audit_logger.start_file(
        "sales_2026_06.csv", file_path="s3://bucket/key", source_system="S3"
    )
    try:
        # ... read/validate/process file ...
        file_audit_logger.finish_file(
            file_audit_id, start_time, rows_read=5000, rows_written=4980, rows_rejected=20
        )
    except Exception:
        file_audit_logger.fail_file(
            file_audit_id, start_time, traceback.format_exc(),
            failure_stage="SCHEMA_VALIDATION",
        )
        raise
"""

import datetime
import os

from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection

_FILE_AUDIT_TABLE = "file_audit_log"

MAX_ERROR_LENGTH = 65000

MODE_INSERT = "insert"
MODE_UPDATE = "update"


class FileAuditLogger:
    """
    Manages INSERT/UPDATE operations on the ``file_audit_log`` table.

    Parameters
    ----------
    job_id : int, optional
        The parent ``job_audit_log.job_id`` this file belongs to. Pass the
        value returned by ``AuditLogger.start_job()`` to link file-level
        records to the pipeline run. Leave as None if not tracking a parent job.
    """

    def __init__(self, job_id=None):
        self.job_id = job_id

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start_file(self, file_name, file_path=None, source_system="S3"):
        """
        Insert a RUNNING audit record for a file.

        Returns
        -------
        tuple(int or None, datetime.datetime)
            (file_audit_id, start_time). file_audit_id is None if the audit
            DB is unreachable. start_time must be passed back into
            finish_file()/fail_file() so processing time can be computed
            without an extra SELECT.
        """
        start_time = datetime.datetime.now()
        file_size_bytes = self._get_file_size(file_path)

        logger.info(
            "[FileAuditLogger] File started: file='%s' | job_id=%s | source_system=%s",
            file_name, self.job_id, source_system,
        )

        sql = (
            f"INSERT INTO `{_FILE_AUDIT_TABLE}` "
            f"(job_id, file_name, source_file_path, source_system, file_size_bytes, "
            f" status, start_time, created_date) "
            f"VALUES (%s, %s, %s, %s, %s, 'RUNNING', %s, %s)"
        )
        params = (
            self.job_id, file_name, file_path, source_system, file_size_bytes,
            start_time, start_time,
        )
        file_audit_id = self._execute_query(sql, params, context="start_file", mode=MODE_INSERT)
        return file_audit_id, start_time

    def finish_file(self, file_audit_id, start_time, rows_read=0, rows_written=0,
                     rows_rejected=0):
        """Mark the file record SUCCESS and record row counts."""
        self._update_file(
            file_audit_id, start_time, status="SUCCESS",
            rows_read=rows_read, rows_written=rows_written, rows_rejected=rows_rejected,
        )

    def fail_file(self, file_audit_id, start_time, error_message, failure_stage=None,
                   rows_read=0, rows_written=0, rows_rejected=0):
        """
        Mark the file record FAILED with error details and any partial
        counts. Caller must re-raise after this call.
        """
        truncated_error = (error_message or "")[:MAX_ERROR_LENGTH]
        self._update_file(
            file_audit_id, start_time, status="FAILED", failure_stage=failure_stage,
            error_message=truncated_error, rows_read=rows_read, rows_written=rows_written,
            rows_rejected=rows_rejected,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _update_file(self, file_audit_id, start_time, status, failure_stage=None,
                      error_message=None, rows_read=0, rows_written=0, rows_rejected=0):
        """Shared SUCCESS/FAILED update logic for finish_file/fail_file."""
        if file_audit_id is None:
            logger.warning(
                "[FileAuditLogger] update(%s) skipped: file_audit_id is None.", status
            )
            return

        end_time = datetime.datetime.now()
        processing_seconds = (
            round((end_time - start_time).total_seconds(), 3) if start_time else 0.0
        )

        if status == "SUCCESS":
            logger.info(
                "[FileAuditLogger] File finished: file_audit_id=%s | rows_read=%s | "
                "rows_written=%s | rows_rejected=%s | processing_time=%.3fs",
                file_audit_id, rows_read, rows_written, rows_rejected, processing_seconds,
            )
        else:
            logger.error(
                "[FileAuditLogger] File failed: file_audit_id=%s | stage=%s | "
                "processing_time=%.3fs",
                file_audit_id, failure_stage, processing_seconds,
            )

        sql = (
            f"UPDATE `{_FILE_AUDIT_TABLE}` SET "
            f"  status = %s, end_time = %s, processing_time_seconds = %s, "
            f"  rows_read = %s, rows_written = %s, rows_rejected = %s, "
            f"  failure_stage = %s, error_message = %s "
            f"WHERE file_audit_id = %s"
        )
        params = (
            status, end_time, processing_seconds,
            int(rows_read or 0), int(rows_written or 0), int(rows_rejected or 0),
            failure_stage, error_message, file_audit_id,
        )
        self._execute_query(sql, params, context=f"_update_file({status})", mode=MODE_UPDATE)

    @staticmethod
    def _get_file_size(file_path):
        """
        Return file size in bytes for local paths. Returns None for remote
        paths (e.g. s3://...) or if the file can't be stat'd -- this is
        best-effort metadata, never a reason to fail the pipeline.
        """
        if not file_path:
            return None
        try:
            return os.path.getsize(file_path)
        except OSError as exc:
            logger.warning(
                "[FileAuditLogger] Could not determine file size for '%s': %s",
                file_path, exc,
            )
            return None

    def _execute_query(self, sql, params, context, mode):
        """
        Execute a single parameterized statement and manage the connection
        lifecycle (commit/rollback/close). Never raises -- errors are logged
        and swallowed so the ETL pipeline is unaffected by audit DB issues.

        SQL text contains only hardcoded column/table names; runtime values
        are bound exclusively through ``params``, eliminating SQL injection risk.

        Parameters
        ----------
        mode : {MODE_INSERT, MODE_UPDATE}
            MODE_INSERT returns cursor.lastrowid, MODE_UPDATE returns None.
        """
        connection = cursor = None
        try:
            connection = get_mysql_connection()
            cursor = connection.cursor()
            cursor.execute(sql, params)
            connection.commit()

            if mode == MODE_INSERT:
                return cursor.lastrowid
            return None
        except Exception as exc:
            logger.error(
                "[FileAuditLogger] %s: operation failed. Pipeline continues. Error: %s",
                context, exc, exc_info=True,
            )
            if connection:
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
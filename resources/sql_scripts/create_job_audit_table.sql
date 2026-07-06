-- =============================================================================
-- File    : create_job_audit_table.sql
-- Purpose : Creates the job audit table used by the ETL pipeline.
-- =============================================================================

CREATE TABLE IF NOT EXISTS job_audit_log (

    -- Primary Key
    job_id INT UNSIGNED NOT NULL AUTO_INCREMENT,

    -- Job Information
    job_name VARCHAR(100) NOT NULL,
    pipeline_version VARCHAR(50) NOT NULL DEFAULT '1.0.0',

    environment ENUM(
        'DEV',
        'TEST',
        'PROD'
    ) NOT NULL DEFAULT 'DEV',

    trigger_type ENUM(
        'MANUAL',
        'AIRFLOW',
        'CRON',
        'API',
        'TEST'
    ) NOT NULL DEFAULT 'MANUAL',

    -- Timing
    start_time DATETIME NOT NULL,
    end_time DATETIME NULL,

    execution_time_seconds DECIMAL(12,3) NULL,

    -- Processing Metrics
    files_processed INT UNSIGNED DEFAULT 0,

    rows_read BIGINT UNSIGNED DEFAULT 0,

    customer_mart_rows_written BIGINT UNSIGNED DEFAULT 0,

    sales_mart_rows_written BIGINT UNSIGNED DEFAULT 0,

    files_rejected INT UNSIGNED DEFAULT 0,

    -- Job Status
    status ENUM(
        'RUNNING',
        'SUCCESS',
        'FAILED'
    ) NOT NULL DEFAULT 'RUNNING',

    failure_stage VARCHAR(150) NULL,

    error_message TEXT NULL,

    -- Spark Information
    spark_application_id VARCHAR(100) NULL,

    -- Audit Information
    created_date DATETIME NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    updated_date DATETIME NOT NULL
        DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (job_id),

    INDEX idx_job_name (job_name),

    INDEX idx_status (status),

    INDEX idx_start_time (start_time),

    INDEX idx_environment (environment),

    INDEX idx_job_status (job_name, status)

)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci
COMMENT = 'Stores one record for every ETL pipeline execution.';


-- ============================================================================
-- Verification
-- ============================================================================
-- DESCRIBE job_audit_log;
--
-- SELECT *
-- FROM job_audit_log
-- ORDER BY created_date DESC;
--
-- SHOW CREATE TABLE job_audit_log;
-- ============================================================================
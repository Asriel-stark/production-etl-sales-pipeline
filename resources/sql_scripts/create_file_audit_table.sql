-- =============================================================================
-- File    : create_file_audit_table.sql
-- Purpose : Stores file-level execution metrics for every file processed by
--           the ETL pipeline.
-- =============================================================================

CREATE TABLE IF NOT EXISTS file_audit_log (

    -- -------------------------------------------------------------------------
    -- Identity
    -- -------------------------------------------------------------------------
    file_audit_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    -- Parent Job
    job_id INT UNSIGNED NOT NULL,

    -- -------------------------------------------------------------------------
    -- File Information
    -- -------------------------------------------------------------------------
    file_name VARCHAR(255) NOT NULL,

    source_file_path VARCHAR(767) NOT NULL,

    file_size_bytes BIGINT UNSIGNED NOT NULL,

    source_system VARCHAR(50)
        NOT NULL DEFAULT 'S3',

    -- -------------------------------------------------------------------------
    -- Processing Time
    -- -------------------------------------------------------------------------
    start_time DATETIME NOT NULL,

    end_time DATETIME NULL,

    processing_time_seconds DECIMAL(12,3) NULL,

    -- -------------------------------------------------------------------------
    -- Processing Metrics
    -- -------------------------------------------------------------------------
    rows_read BIGINT UNSIGNED
        NOT NULL DEFAULT 0,

    rows_written BIGINT UNSIGNED
        NOT NULL DEFAULT 0,

    rows_rejected BIGINT UNSIGNED
        NOT NULL DEFAULT 0,

    -- -------------------------------------------------------------------------
    -- Processing Status
    -- -------------------------------------------------------------------------
    status ENUM(
        'RUNNING',
        'SUCCESS',
        'FAILED'
    ) NOT NULL DEFAULT 'RUNNING',

    failure_stage VARCHAR(100) NULL,

    error_message TEXT NULL,

    -- -------------------------------------------------------------------------
    -- Audit Information
    -- -------------------------------------------------------------------------
    created_date DATETIME
        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    updated_date DATETIME
        NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,

    -- -------------------------------------------------------------------------
    -- Constraints
    -- -------------------------------------------------------------------------
    PRIMARY KEY (file_audit_id),

    CONSTRAINT fk_file_audit_job
        FOREIGN KEY (job_id)
        REFERENCES job_audit_log(job_id)
        ON DELETE CASCADE,

    -- -------------------------------------------------------------------------
    -- Indexes
    -- -------------------------------------------------------------------------
    INDEX idx_job_id (job_id),

    INDEX idx_status (status),

    INDEX idx_file_name (file_name),

    INDEX idx_job_file (job_id, file_name),

    INDEX idx_start_time (start_time)

)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci
COMMENT = 'Stores one audit record for every file processed by an ETL job.';


-- ============================================================================
-- Verification
-- ============================================================================
-- DESCRIBE file_audit_log;
--
-- SHOW CREATE TABLE file_audit_log;
--
-- SELECT *
-- FROM file_audit_log
-- ORDER BY created_date DESC;
-- ============================================================================
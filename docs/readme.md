# 🚀 Production ETL Pipeline — Apache Spark, AWS S3 & MySQL

A production-inspired, batch ETL pipeline built with **Apache Spark**, **PySpark**, **AWS S3**, and **MySQL**. This project demonstrates enterprise-grade data engineering practices — schema validation, fault-tolerant processing, job and file-level audit logging, dimensional modeling, and analytical data mart creation.

---

## 📑 Table of Contents

- [Project Overview](#project-overview)
- [Key Features](#key-features)
- [Technology Stack](#technology-stack)
- [Architecture](#architecture)
- [ETL Pipeline Flow](#etl-pipeline-flow)
- [Project Structure](#project-structure)
- [Data Processing Features](#data-processing-features)
- [Job Audit Framework](#job-audit-framework)
- [File Audit Framework](#file-audit-framework)
- [Security](#security)
- [Fault Tolerance](#fault-tolerance)
- [Getting Started](#getting-started)
- [Output](#output)
- [Future Enhancements](#future-enhancements)
- [Author](#author)

---

## Project Overview

This project simulates a real-world enterprise data engineering pipeline that:

1. Ingests raw sales data from **Amazon S3**
2. Validates incoming files against an expected schema
3. Transforms and enriches the data using **Apache Spark**
4. Loads analytics-ready datasets into **MySQL** and partitioned **Parquet** files

The pipeline follows a modular, layered architecture designed for **maintainability**, **reliability**, and **scalability** — mirroring patterns used in production data platforms.

---

## Key Features

| Feature | Description |
|---|---|
| ⚡ Batch ETL Pipeline | End-to-end orchestration using Apache Spark |
| ☁️ S3 Ingestion | Automated sales data ingestion from AWS S3 |
| ✅ Schema Validation | Rejects malformed/incomplete files before processing |
| 🛡️ Fault Tolerance | Isolated failure handling at the file level |
| 📊 Data Marts | Customer Data Mart & Sales Team Data Mart generation |
| 📝 Job Audit Framework | Execution-level metrics for every pipeline run |
| 📁 File Audit Framework | Per-file processing and rejection tracking |
| 🔐 AES Encryption | Encrypted AWS credentials at rest |
| 🗄️ MySQL Integration | Parameterized queries, secure connectivity |
| 📦 Partitioned Parquet | Optimized analytical output format |
| 🧩 Modular Architecture | Cleanly separated read/write/transform/utility layers |
| 🧾 Structured Logging | Consistent, business-event-level logging throughout |

---

## Technology Stack

| Category | Technologies |
|----------|--------------|
| Language | Python |
| Processing Engine | Apache Spark, PySpark |
| Cloud Storage | AWS S3 (via Boto3) |
| Database | MySQL |
| File Formats | CSV, Parquet |
| Security | AES Encryption |
| Logging | Python `logging` module |

---

## Architecture

```
                    +----------------------+
                    |     Amazon S3        |
                    |  Source CSV Files    |
                    +----------+-----------+
                               |
                               v
                    Download & Validation
                               |
                               v
                  Schema Validation Module
                               |
             +-----------------+----------------+
             |                                  |
             | Valid Files                      | Invalid Files
             |                                  |
             v                                  v
     Spark Transformation               Error Folder
             |
             v
      Dimension Table Joins
             |
             v
     Data Enrichment & Cleaning
             |
             +---------------------------+
             |                           |
             v                           v
Customer Data Mart             Sales Team Data Mart
             |
             +-------------+-------------+
                           |
                           v
                 MySQL + Parquet Output
                           |
                           v
              Job Audit & File Audit Logs
```

---

## ETL Pipeline Flow

1. Initialize Spark Session
2. Connect to AWS S3
3. Download source files
4. Validate schema
5. Reject invalid files (routed to error folder)
6. Read valid CSV files using Spark
7. Transform and cleanse data
8. Join dimension tables
9. Build Customer Data Mart
10. Build Sales Team Data Mart
11. Write results to MySQL
12. Generate partitioned Parquet datasets
13. Upload outputs to S3
14. Move processed files to archive
15. Update Job Audit Log
16. Update File Audit Log
17. Clean up local workspace

---

## Project Structure

```text
production-etl-sales-pipeline/
│
├── docs/
│   ├── architecture.png
│   ├── database_schema.drawio.png
│   └── README.md
│
├── resources/
│   ├── __init__.py
│   │
│   ├── dev/
│   │   ├── config.py
│   │   └── requirements.txt
│   │
│   ├── qa/
│   │   ├── config.py
│   │   └── requirements.txt
│   │
│   ├── prod/
│   │   ├── config.py
│   │   └── requirements.txt
│   │
│   └── sql_scripts/
│       ├── table_scripts.sql
│       ├── create_job_audit_table.sql
│       └── create_file_audit_table.sql
│
├── src/
│   ├── __init__.py
│   │
│   ├── main/
│   │   ├── __init__.py
│   │   │
│   │   ├── delete/
│   │   │   ├── aws_delete.py
│   │   │   ├── database_delete.py
│   │   │   └── local_file_delete.py
│   │   │
│   │   ├── download/
│   │   │   └── aws_file_download.py
│   │   │
│   │   ├── move/
│   │   │   └── move_files.py
│   │   │
│   │   ├── read/
│   │   │   ├── aws_read.py
│   │   │   └── database_read.py
│   │   │
│   │   ├── transformations/
│   │   │   └── jobs/
│   │   │       ├── main.py
│   │   │       ├── dimension_tables_join.py
│   │   │       ├── customer_mart_sql_tranform_write.py
│   │   │       └── sales_mart_sql_transform_write.py
│   │   │
│   │   ├── upload/
│   │   │   └── upload_to_s3.py
│   │   │
│   │   ├── utility/
│   │   │   ├── audit_logger.py
│   │   │   ├── file_audit_logger.py
│   │   │   ├── encrypt_decrypt.py
│   │   │   ├── logging_config.py
│   │   │   ├── my_sql_session.py
│   │   │   ├── s3_client_object.py
│   │   │   └── spark_session.py
│   │   │
│   │   └── write/
│   │       ├── database_write.py
│   │       └── parquet_writer.py
│   │
│   └── test/
│       ├── generate_csv_data.py
│       ├── generate_customer_table_data.py
│       ├── generate_datewise_sales_data.py
│       ├── extra_column_csv_generated_data.py
│       ├── less_column_csv_generated_data.py
│       ├── sales_data_upload_s3.py
│       ├── scratch_pad.py
│       └── test.py
│
├── .gitignore
├── README.md
└── LICENSE
```

### Module Responsibilities

| Layer | Responsibility |
|---|---|
| `main/download/` | Pulls raw source files from S3 into the local workspace |
| `main/read/` | Reads data from S3 and MySQL into Spark DataFrames |
| `main/transformations/jobs/` | Core business logic: joins, enrichment, mart generation |
| `main/write/` | Writes final datasets to MySQL and Parquet |
| `main/upload/` | Pushes processed outputs back to S3 |
| `main/move/` | Archives processed source files |
| `main/delete/` | Cleans up local, S3, and database artifacts post-run |
| `main/utility/` | Shared infrastructure: Spark session, MySQL session, S3 client, encryption, logging, audit logging |
| `resources/` | Environment-specific configs (`dev`, `qa`, `prod`) and SQL DDL scripts |
| `test/` | Synthetic data generators and test scaffolding for pipeline validation |

---

## Data Processing Features

### Schema Validation
- Validates presence of mandatory columns
- Detects and flags extra/unexpected columns
- Rejects malformed files before they enter the transformation stage
- Records every validation failure for audit purposes

### Spark Transformations
- Explicit schema casting for type safety
- Data cleansing (nulls, duplicates, malformed records)
- Data enrichment via dimension joins
- Data mart construction (Customer, Sales Team)

### Customer Data Mart
Joins transactional sales data with customer and store dimensions to produce customer-level analytical datasets.

### Sales Team Data Mart
Aggregates sales performance by team/representative for reporting and analytics use cases.

---

## Job Audit Framework

Captures execution-level metrics for every pipeline run:

- Job ID
- Pipeline Version
- Environment
- Trigger Type
- Start Time / End Time / Execution Time
- Files Processed
- Rows Read / Rows Written
- Status
- Failure Stage
- Error Message
- Spark Application ID

---

## File Audit Framework

Tracks processing details for every individual file:

- File Name
- Source Path
- File Size
- Processing Time
- Rows Read / Rows Written / Rows Rejected
- Status
- Failure Stage
- Error Message

---

## Security

- AES-encrypted AWS credentials (see `utility/encrypt_decrypt.py`)
- Fully parameterized SQL queries (no string-concatenated SQL)
- Modular, centralized database connectivity (`my_sql_session.py`)
- No plaintext secrets committed to source control (environment-based config)

---

## Fault Tolerance

The pipeline is designed to keep running even if individual files fail:

- File-level exception handling with isolated failure domains
- Job-level and file-level audit logging for full traceability
- Failed files routed to a dedicated error location rather than halting the run
- Automatic local workspace cleanup after each run
- Detailed, structured logging throughout every stage

---

## Getting Started

```bash
# Clone the repository
git clone https://github.com/Asriel-stark/production-etl-sales-pipeline.git
cd production-etl-sales-pipeline

# Install dependencies (per environment)
pip install -r resources/dev/requirements.txt

# Configure environment settings
# Edit resources/dev/config.py with your AWS/MySQL credentials

# Run the pipeline
python src/main/transformations/jobs/main.py
```

> **Note:** Update `resources/<env>/config.py` with your own AWS and MySQL connection details before running. Never commit real credentials — use encrypted values via `encrypt_decrypt.py`.

---

## Output

The pipeline produces:

- Customer Data Mart
- Sales Team Data Mart
- Partitioned Parquet datasets
- Job Audit Logs
- File Audit Logs

---

## Future Enhancements

- [ ] Apache Airflow orchestration
- [ ] Change Data Capture (CDC)
- [ ] Incremental loading
- [ ] Slowly Changing Dimension (SCD Type 2)
- [ ] Delta Lake integration
- [ ] Docker deployment
- [ ] CI/CD pipeline
- [ ] Unit test coverage
- [ ] Data Quality Framework
- [ ] Great Expectations integration

---

## Author

**Deekshith Poojary**

- GitHub: [@Asriel-stark](https://github.com/Asriel-stark)
- LinkedIn: [deekshith-d-029668284](https://www.linkedin.com/in/deekshith-d-029668284/)

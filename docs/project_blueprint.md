# Real-Time Ride-Hailing Data Warehouse — Phased Project Blueprint

## Project Overview
This document outlines the 7-phase execution roadmap for building a production-style, Change Data Capture (CDC)-based Lakehouse Data Warehouse for a simulated Uber-inspired ride-hailing domain.

---

## Architecture Flowchart

```
[ PostgreSQL (OLTP) ] ──(WAL Replication)──> [ Debezium CDC ]
                                                     │
                                                     ▼
                                            [ Redpanda Broker ]
                                                     │
                                                     ▼
                                            [ Databricks Streaming ]
                                                     │
       ┌─────────────────────────────────────────────┼─────────────────────────────────────────────┐
       ▼                                             ▼                                             ▼
[ Bronze Layer ]                             [ Silver Layer ]                              [ Gold Layer ]
Append-only raw CDC                          CDC Replay, Deduplication,                    dbt Kimball Star Schema
JSON event logs + SHA256                     SCD Type 2 + Delta MERGE                      Facts & Dimensions +
Event Fingerprinting                         LSN Guard + Soda Core DQ                      dbt-expectations DQ
       │                                             │                                             │
       └─────────────────────────────────────────────┼─────────────────────────────────────────────┘
                                                     ▼
                                     [ Airflow DQ & Alert Guardrails ]
                                                     │
                                                     ▼
                                              [ Metabase BI ]
```

---

## Phased Execution Roadmap

### Phase 1: Local Containerized Infrastructure & Networking
- **Objective**: Provision local multi-container infrastructure using Docker Compose.
- **Components**:
  - PostgreSQL 15+ (`wal_level = logical`)
  - Redpanda Broker & Redpanda Console
  - Debezium Connect
  - Apache Airflow (Scheduler, Webserver, Postgres backend)
  - Metabase BI Engine
- **Deliverables**: Verified `docker-compose.yml`, container startup health checks, and inter-container network connectivity.

---

### Phase 2: Source OLTP Schema & Data Generation Engine
- **Objective**: Build the live production transactional simulator.
- **Components**:
  - DDL setup for `users`, `drivers`, `vehicles`, `cities`, `trip_requests`, `trips`, `driver_locations`, `payments`, `ratings`, `promotions`.
  - Multi-threaded Python data generator introducing realistic status delays, updates, driver cancellations, and high-frequency GPS telemetry.
- **Deliverables**: Database schema initialized in Postgres, Python generator script writing live transactional load.

---

### Phase 3: CDC Pipeline & Event Streaming Configuration
- **Objective**: Establish real-time CDC event extraction via Postgres WAL with schema drift readiness.
- **Components**:
  - Debezium Postgres connector JSON registration payload with DDL change capturing.
  - Redpanda topics for each OLTP table.
  - Event schema validation in Redpanda Console.
- **Deliverables**: Active Debezium connector streaming CDC `INSERT`, `UPDATE`, and `DELETE` events into Redpanda topics with schema change tracking.

---

### Phase 4: Databricks Medallion Ingestion (Bronze & Silver Layers) with Idempotency & Data Quality
- **Objective**: Process streaming CDC events into Delta Lake Bronze and Silver layers with end-to-end idempotency and automated data quality assertions.
- **Components**:
  - **Bronze Layer**:
    - Spark Structured Streaming jobs reading Redpanda topics into append-only Delta Lake tables.
    - **Idempotency Engine**: Deterministic `_cdc_event_hash` generation (`SHA256(source.lsn + source.txId + table + pk + op)`) and watermark deduplication.
    - **Schema Drift & Corrupted Records DLQ**: Delta `.option("mergeSchema", "true")` auto-evolution and Dead-Letter Queue (`bronze_corrupted_records`) capturing malformed payloads via `_corrupt_record`.
  - **Silver Layer**:
    - **Idempotent CDC State Replay**: Delta `MERGE INTO` logic resolving event ordering using LSN sequence guards (`source.cdc_lsn > target.last_cdc_lsn`), preventing duplicate state mutation on replayed micro-batches.
    - **SCD Type 2 Modeling**: Maintaining history for `dim_rider` and `dim_driver`.
    - **Data Quality Assertions**: Integrated **Soda Core** scan executions (`checks.yml` using SodaCL) enforcing missingness, uniqueness, and value range rules on Silver tables.
- **Deliverables**: Operational Bronze & Silver Delta Lake tables in Databricks workspace with verified idempotency under streaming restarts and Soda Core DQ scans passing.

---

### Phase 5: Gold Layer Kimball Star Schema & dbt Modeling with Data Quality Testing
- **Objective**: Build an analytics-ready dimensional warehouse with automated dbt data quality suites.
- **Components**:
  - `dbt-core` project setup connected to Databricks / Silver tables.
  - **Dimensions**: `dim_rider` (SCD2), `dim_driver` (SCD2), `dim_vehicle`, `dim_city`, `dim_date`.
  - **Facts**: `fact_trips`, `fact_trip_requests` (funnel analysis), `fact_driver_pings` (telemetry).
  - **Gold Data Quality Framework**: `dbt-expectations` test suite enforcing referential integrity, column value ranges, non-null constraints, and auto-generated data lineage documentation.
- **Deliverables**: Production-grade Gold layer star schema with dbt test suite passing.

---

### Phase 6: End-to-End Pipeline Orchestration & Data Quality Guardrails with Apache Airflow
- **Objective**: Automate end-to-end pipeline execution, monitoring, and quality gate enforcement.
- **Components**:
  - Airflow DAGs triggering Spark streaming health checks, Silver batch merges, automated Soda Core scans, and dbt transformations.
  - **Quality Gates & Alerting**: Automated pipeline halting and alerting if Soda Core scans or dbt tests fail.
- **Deliverables**: Airflow DAGs running scheduled/monitored pipeline workflows with active DQ guardrails.

---

### Phase 7: Analytics BI Dashboards & Technical Documentation
- **Objective**: Visualize business metrics and document technical architecture.
- **Components**:
  - Metabase dashboards covering trip funnel conversion (`searching` -> `matched` -> `completed`), driver utilization, revenue metrics, and ETA accuracy.
  - Technical walkthrough detailing CDC design choices, idempotency mechanics (SHA256 + LSN guard), and Soda Core DQ implementation.
- **Deliverables**: Interactive BI dashboards and public project documentation.


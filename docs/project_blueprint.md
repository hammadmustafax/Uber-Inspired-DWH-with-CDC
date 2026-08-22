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
       ┌─────────────────────────────────────────────┴─────────────────────────────────────────────┐
       ▼                                             ▼                                             ▼
[ Bronze Layer ]                             [ Silver Layer ]                              [ Gold Layer ]
Append-only raw CDC                        CDC Replay, Deduplication,                     dbt Kimball Star Schema
JSON event logs                            SCD Type 2 dimensions                          Facts & Dimensions
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
- **Objective**: Establish real-time CDC event extraction via Postgres WAL.
- **Components**:
  - Debezium Postgres connector JSON registration payload.
  - Redpanda topics for each OLTP table.
  - Event schema validation in Redpanda Console.
- **Deliverables**: Active Debezium connector streaming CDC `INSERT`, `UPDATE`, and `DELETE` events into Redpanda topics.

---

### Phase 4: Databricks Medallion Ingestion (Bronze & Silver Layers)
- **Objective**: Process streaming CDC events into Delta Lake Bronze and Silver layers.
- **Components**:
  - **Bronze Layer**: Spark Structured Streaming jobs reading Redpanda Kafka topics into append-only Delta Lake tables.
  - **Silver Layer**: CDC state-replay logic resolving event ordering (`ts_ms`/`updated_at`), deduplicating row states, and modeling SCD Type 2 dimensions (`dim_rider`, `dim_driver`).
- **Deliverables**: Operational Bronze & Silver Delta Lake tables in Databricks Community Edition workspace.

---

### Phase 5: Gold Layer Kimball Star Schema & dbt Modeling
- **Objective**: Build an analytics-ready dimensional warehouse.
- **Components**:
  - `dbt-core` project setup connected to Databricks / Silver tables.
  - **Dimensions**: `dim_rider` (SCD2), `dim_driver` (SCD2), `dim_vehicle`, `dim_city`, `dim_date`.
  - **Facts**: `fact_trips`, `fact_trip_requests` (funnel analysis), `fact_driver_pings` (telemetry).
  - dbt data quality assertions and auto-generated data lineage documentation.
- **Deliverables**: Production-grade Gold layer star schema with dbt test suite passing.

---

### Phase 6: End-to-End Pipeline Orchestration with Apache Airflow
- **Objective**: Automate end-to-end pipeline execution and monitoring.
- **Components**:
  - Airflow DAGs triggering Spark streaming health checks, Silver batch merges, dbt transformations, and data quality checks.
  - Alerting and error handling logic.
- **Deliverables**: Airflow DAGs running scheduled/monitored pipeline workflows.

---

### Phase 7: Analytics BI Dashboards & Technical Documentation
- **Objective**: Visualize business metrics and document technical architecture.
- **Components**:
  - Metabase dashboards covering trip funnel conversion (`searching` -> `matched` -> `completed`), driver utilization, revenue metrics, and ETA accuracy.
  - Technical walkthrough detailing CDC design choices vs. batch polling.
- **Deliverables**: Interactive BI dashboards and public project documentation.

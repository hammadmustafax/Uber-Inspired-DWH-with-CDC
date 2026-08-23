# AGENT.md — Project Context & Architectural Blueprint

## Project Overview
This repository implements an end-to-end, production-grade **Change Data Capture (CDC)-based Lakehouse Data Warehouse** for a simulated ride-hailing platform (Uber-inspired). It transitions raw OLTP transactional state changes through a streaming event pipeline into a medallion lakehouse architecture, culminating in a Kimball star schema modeled with dbt and visualized via Metabase dashboards — all built using open-source / free-tier technologies ($0 stack).

---

## 1. Core Architectural Pipeline

```
[ PostgreSQL (OLTP) ] --(WAL Logical Replication)--> [ Debezium CDC ]
                                                             │
                                                             ▼
                                                    [ Redpanda Broker ]
                                                             │
                                                             ▼
                                                    [ Databricks Streaming ]
                                                             │
       ┌─────────────────────────────────────────────────────┴────────────────────────────────────────────────┐
       ▼                                                     ▼                                                ▼
[ Bronze Layer ]                                     [ Silver Layer ]                                 [ Gold Layer ]
Append-only raw CDC JSON                           CDC Replay, Deduplicated state,                  dbt-core Kimball Star Schema
events + metadata                                  SCD Type 2 dimensions                            (Facts & Dimensions)
                                                                                                      │
                                                                                                      ▼
                                                                                             [ Metabase BI Dashboards ]
```

- **Orchestrator**: Apache Airflow (Dockerized) triggering Databricks jobs and dbt transformations.

---

## 2. Source System (OLTP Postgres Schema)

### Entities & Relationships
1. **Identity & Assets**:
   - `users`: `user_id` (PK), `first_name`, `last_name`, `email`, `phone`, `signup_at`, `home_city_id` (FK), `updated_at`
   - `drivers`: `driver_id` (PK), `user_id` (FK), `license_number`, `vehicle_id` (FK), `status` (`offline`, `available`, `en_route`, `on_trip`), `rating_avg`, `onboarded_at`, `updated_at`
   - `vehicles`: `vehicle_id` (PK), `driver_id` (FK), `make`, `model`, `year`, `plate_number`, `vehicle_type` (`uberx`, `comfort`, `xl`, `black`), `updated_at`
   - `cities`: `city_id` (PK), `city_name`, `country`, `surge_enabled`, `timezone`

2. **Core Trip Lifecycle**:
   - `trip_requests`: `request_id` (PK), `rider_id` (FK), `city_id` (FK), `pickup_lat`, `pickup_lng`, `dropoff_lat`, `dropoff_lng`, `requested_vehicle_type`, `requested_at`, `estimated_fare`, `surge_multiplier`, `status` (`searching`, `matched`, `expired`, `cancelled`), `updated_at`
   - `trips`: `trip_id` (PK), `request_id` (FK), `rider_id` (FK), `driver_id` (FK), `vehicle_id` (FK), `pickup_lat`, `pickup_lng`, `dropoff_lat`, `dropoff_lng`, `status` (`matched`, `driver_en_route`, `arrived`, `in_progress`, `completed`, `cancelled`), transition timestamps (`requested_at`, `matched_at`, `driver_arrived_at`, `started_at`, `completed_at`, `cancelled_at`), `distance_km`, `duration_seconds`, `base_fare`, `surge_multiplier`, `final_fare`, `cancellation_reason`, `cancelled_by`, `updated_at`

3. **Telemetry & Operations**:
   - `driver_locations`: `location_id` (PK), `driver_id` (FK), `trip_id` (FK nullable), `lat`, `lng`, `heading`, `speed_kmh`, `recorded_at` (High-frequency append-only telemetry stream)

4. **Payments & Feedback**:
   - `payments`: `payment_id` (PK), `trip_id` (FK), `rider_id` (FK), `amount`, `payment_method`, `status` (`pending`, `succeeded`, `failed`, `refunded`), `processed_at`, `updated_at`
   - `ratings`: `rating_id` (PK), `trip_id` (FK), `rated_by`, `rating_value`, `comment`, `created_at`
   - `promotions`: `promo_id` (PK), `rider_id` (FK), `code`, `discount_type`, `discount_value`, `applied_trip_id` (FK nullable), `applied_at`

---

## 3. Key Pipeline Design Patterns

1. **Change Data Capture (CDC) via Postgres WAL**:
   - Captures low-latency `INSERT`, `UPDATE`, and `DELETE` events.
   - Eliminates polling query overhead on OLTP and preserves intermediate state transitions (e.g., `matched` -> `driver_en_route` -> `arrived` -> `in_progress` -> `completed`).

2. **Medallion Architecture (Databricks / Delta Lake)**:
   - **Bronze**: Raw CDC envelope ingestion (payload with `before`, `after`, `op`, `ts_ms`).
   - **Silver**: State reconstruction using windowed ordering (`op != 'D'`, deduplication on PK, latest `ts_ms` / `updated_at`). Type 2 SCD processing for changing attributes.
   - **Gold**: dbt-managed star schema optimized for analytical query workloads.

3. **Dimensional Data Modeling (Kimball Star Schema)**:
   - **Dimensions**:
     - `dim_rider` (SCD Type 2 — tracks city/address changes over time)
     - `dim_driver` (SCD Type 2 — tracks vehicle assignment and rating trajectory)
     - `dim_vehicle`
     - `dim_city`
     - `dim_date`
   - **Fact Tables**:
     - `fact_trips`: Granularity of 1 row per trip. Contains latency metrics (time-to-match, wait time), financial metrics (base fare, surge multiplier, final fare), and status flags.
     - `fact_trip_requests`: Conversion and drop-off funnel analysis (`searching` -> `matched` -> `completed`/`cancelled`).
     - `fact_driver_pings`: High-throughput telemetry fact table for driver utilization, spatial positioning, and ETA estimation accuracy.

---

## 4. Technology Stack & Tools

| Component | Technology | Role / Configuration |
|---|---|---|
| **OLTP Source** | PostgreSQL 15+ | Dockerized, WAL enabled (`wal_level = logical`) |
| **Data Generator** | Python (Faker / Async/Multiprocessing) | Simulates real-time rides, driver movement, and state updates |
| **CDC Engine** | Debezium | Reads Postgres WAL logs, emits JSON change records |
| **Message Broker** | Redpanda | Kafka API-compatible event hub |
| **Compute / Engine** | Databricks CE / Spark Structured Streaming | Delta Lake medallion processing |
| **Data Modeling** | dbt-core | SQL transformations, schema testing, lineage |
| **Orchestration** | Apache Airflow | Dockerized DAGs managing streaming & batch jobs |
| **Visualization** | Metabase | Dashboards for ride volume, revenue, driver utilization |

---

## 5. Coding Standards & Guidelines for Agents

To ensure long-term maintainability, clarity, and consistency across all pipeline components, AI agents and developers modifying this codebase must adhere to the following principles:

1. **Code Simplicity & Readability**:
   - Write natural, clean, and straightforward code.
   - Avoid overly complex, deeply nested functions or impenetrable one-liners.
   - Break down complicated operations into small, single-purpose helper functions with explicit input/output signatures.

2. **Precise & Unambiguous Commenting**:
   - Document every key logic area, data flow, function, and state transition with clear inline comments.
   - Provide precise explanations for *why* specific transformations, windowing logic, or schema mappings are applied, leaving no room for ambiguity.

3. **Strict Dependency Management (`requirements.txt`)**:
   - Every time a new external library or package is imported in any Python script/module (e.g., `faker`, `psycopg2-binary`, `confluent-kafka`, `pyspark`, `dbt-core`), immediately add the library name to [requirements.txt](file:///d:/Uber_DWH/requirements.txt).

4. **Zero Code Redundancy (DRY Principle)**:
   - Eliminate duplicated logic, hardcoded configurations, or repeated helper routines across modules.
   - Extract common data structures, database connectors, and reusable utility functions into centralized modules in `src/` or `scripts/`.

---

## 6. Development & Execution Roadmap

1. **Infrastructure & OLTP Setup**: Configure Docker Compose with Postgres (logical replication enabled), Redpanda, Debezium connector, and Airflow.
2. **Data Generator**: Develop Python traffic simulator writing to Postgres with realistic state delays and telemetry output.
3. **CDC Ingestion & Streaming**: Configure Debezium PostgreSQL connector to stream change tables into Redpanda topics.
4. **Bronze Layer Ingestion**: Set up Spark Structured Streaming in Databricks to sink Redpanda JSON events into Delta Lake Bronze tables.
5. **Silver Layer Processing**: Build CDC replay logic, upserts/merge strategies, and SCD Type 2 dimension models in Delta Lake.
6. **Gold Layer & dbt Modeling**: Implement Kimball star schema models, foreign key relationships, tests, and documentation with dbt-core.
7. **Orchestration & BI**: Package pipeline execution into Airflow DAGs and build executive dashboards in Metabase.

---

## 7. Phased Project Blueprint

- **Phase 1: Local Containerized Infrastructure & Networking**: Provision multi-container Docker environment (Postgres with WAL logical replication, Redpanda broker + console, Debezium Connect, Airflow, and Metabase).
- **Phase 2: OLTP Schema & Realistic Traffic Simulator**: Implement Postgres DDLs for ride-hailing domain and develop a multi-threaded Python data generator simulating driver availability, ride state transitions, payments, and high-frequency GPS pings.
- **Phase 3: CDC Pipeline & Event Streaming**: Register Debezium Postgres connector via REST API to tail the WAL and stream JSON change events per table into dedicated Redpanda topics.
- **Phase 4: Databricks Medallion Ingestion (Bronze & Silver Layers)**: Build Spark Structured Streaming jobs in Databricks Community Edition to ingest Redpanda topics into Delta Lake Bronze (raw append-only), then build Silver state reconstruction (CDC event ordering, deduplication, and SCD Type 2 tracking).
- **Phase 5: Gold Layer & dbt Dimensional Modeling**: Build dbt-core project to transform Silver Delta tables into a Kimball star schema (`dim_rider`, `dim_driver`, `dim_vehicle`, `dim_city`, `dim_date`, `fact_trips`, `fact_trip_requests`, `fact_driver_pings`) with tests and auto-generated documentation.
- **Phase 6: End-to-End Airflow Pipeline Orchestration**: Develop Airflow DAGs to orchestrate streaming job health monitoring, Silver batch compaction/merges, dbt transformations, and data quality assertions.
- **Phase 7: BI Dashboards & Project Deliverables**: Configure Metabase dashboards (trip funnel analysis, revenue metrics, driver utilization) and compile full architecture documentation and technical write-up.

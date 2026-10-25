# File Map (`file_map.md`)

> **Notice to AI Coding Agents**:
> Consult this file map **before** browsing or scanning the workspace directory. Locate the relevant file keys, module descriptions, and schemas below to pinpoint target files directly.

---

## 1. Root & Orchestration Configuration

| File Key / Path | Content & Purpose |
|---|---|
| [AGENT.md](file:///d:/Uber_DWH/AGENT.md) | Architectural blueprint, source OLTP relational schema definitions, Medallion layer conventions, technology stack breakdown, development roadmap, and mandatory coding standards for AI coding agents. |
| [docker-compose.yml](file:///d:/Uber_DWH/docker-compose.yml) | Multi-container Docker deployment specification. Provisions PostgreSQL (with `wal_level=logical`), Adminer DB UI (port 8081), Redpanda Kafka broker (port 19092) + Console (port 8080), Debezium Connect (port 8083), Apache Airflow (scheduler, webserver on port 8085, init), Metabase BI (port 3000), and Spark Master (port 8088/7077) + Worker. |
| [ports.txt](file:///d:/Uber_DWH/ports.txt) | Quick reference cheat sheet listing all exposed UI endpoints and service ports (Adminer: 8081, Redpanda Console: 8080, Airflow: 8085, Metabase: 3000, Spark Master: 8088, Spark App: 4040). |
| [requirements.txt](file:///d:/Uber_DWH/requirements.txt) | Python dependencies for local execution: `sqlalchemy`, `psycopg2-binary`, `faker`, `python-dotenv`, `pyspark`, `delta-spark`, `soda-core-spark-df`, `setuptools`, and `pyhive`. |
| [file_map.md](file:///d:/Uber_DWH/file_map.md) | Central repository file index mapping every file path to its content and technical purpose. Coding agents consult this map to locate files without scanning directories. |
| [.agents/rules/file_map_rule.md](file:///d:/Uber_DWH/.agents/rules/file_map_rule.md) | Workspace agent rule instructing AI coding agents to always look up files in `file_map.md` rather than running recursive directory scans. |
| [.gitignore](file:///d:/Uber_DWH/.gitignore) | Git exclusion rules for Python caches, virtual environments, IDE files, detailed phase plans, logs, local Delta Lake files, and temp Spark shuffle directories. |
| [uber_dw_project_statement.md](file:///d:/Uber_DWH/uber_dw_project_statement.md) | Comprehensive engineering project charter and technical problem statement explaining why WAL-based CDC was selected over batch polling, the $0 open-source technology stack, and high-level architectural requirements. |

---

## 2. Source OLTP Database Schema & Infrastructure

| File Key / Path | Content & Purpose |
|---|---|
| [src/schema/ddl.sql](file:///d:/Uber_DWH/src/schema/ddl.sql) | DDL script defining all 10 PostgreSQL relational tables (`cities`, `users`, `vehicles`, `drivers`, `trip_requests`, `trips`, `driver_locations`, `payments`, `ratings`, `promotions`). Configures automatic `updated_at` triggers and enables `REPLICA IDENTITY FULL` on each table for full row-image CDC capture. |
| [infrastructure/postgres/init-dbs.sql](file:///d:/Uber_DWH/infrastructure/postgres/init-dbs.sql) | Initial PostgreSQL container bootstrap script. Creates the `uber_oltp` transactional database and `airflow_db` backend database, granting full privileges to `postgres`. |

---

## 3. Real-Time Data Generator & Operational Simulator

| File Key / Path | Content & Purpose |
|---|---|
| [generator/config.py](file:///d:/Uber_DWH/generator/config.py) | Configuration parameters for database connectivity (`DATABASE_URL`), seeding counts (riders, drivers, vehicles), simulation event timings (`TRIP_REQUEST_INTERVAL_SEC`, `TELEMETRY_INTERVAL_SEC`), cancellation rates, vehicle types, and operating cities geographic center points. |
| [generator/db.py](file:///d:/Uber_DWH/generator/db.py) | SQLAlchemy engine initialization with connection pooling (pool size 10, max overflow 20), `SessionLocal` factory, declarative base, and `get_db_session()` thread-safe transactional context manager. |
| [generator/models.py](file:///d:/Uber_DWH/generator/models.py) | SQLAlchemy ORM model definitions for all 10 OLTP tables: `City`, `User`, `Vehicle`, `Driver`, `TripRequest`, `Trip`, `DriverLocation`, `Payment`, `Rating`, and `Promotion`. |
| [generator/telemetry.py](file:///d:/Uber_DWH/generator/telemetry.py) | Geospatial calculation engine. Provides `haversine_distance_km()` for great-circle distance, `calculate_heading()` for bearing angles, coordinate randomizers within city radii, waypoint stepping (`move_towards`), and `create_driver_telemetry_ping()`. |
| [generator/seed.py](file:///d:/Uber_DWH/generator/seed.py) | Seed data script using Faker. Generates baseline operating records: `seed_cities()`, `seed_users()`, `seed_drivers_and_vehicles()`, and orchestrator `run_all_seeds()`. |
| [generator/simulator.py](file:///d:/Uber_DWH/generator/simulator.py) | Real-time asynchronous ride-hailing operational engine (`run_simulation_loop`). Handles trip request creation, driver dispatch/matching, step-by-step state machine transitions (`searching` -> `matched` -> `driver_en_route` -> `arrived` -> `in_progress` -> `completed` / `cancelled`), dynamic fares, payment settlement, and rider reviews. |
| [generator/main.py](file:///d:/Uber_DWH/generator/main.py) | CLI argument parser for data generation (`--seed`, `--run`, `--duration`). Launches baseline seeding and/or asynchronous simulation loop. |
| [generator/simulate_trips.py](file:///d:/Uber_DWH/generator/simulate_trips.py) | Convenience execution wrapper script pointing to `generator.main.main()` for launching the simulation pipeline. |

---

## 4. CDC Extraction & Event Streaming (Phase 3)

| File Key / Path | Content & Purpose |
|---|---|
| [src/cdc_connectors/postgres_cdc_connector.json](file:///d:/Uber_DWH/src/cdc_connectors/postgres_cdc_connector.json) | Debezium PostgreSQL connector configuration for Kafka Connect. Configures WAL logical decoding plugin (`pgoutput`), replication slot (`debezium_uber_slot`), publication name (`debezium_uber_publication`), table inclusions, JSON schema emission, and schema drift history topic (`schema-changes.uber_cdc`). |
| [scripts/register_cdc_connector.py](file:///d:/Uber_DWH/scripts/register_cdc_connector.py) | Python automation script using standard library `urllib` to poll Debezium Connect REST API, submit `postgres_cdc_connector.json`, verify registration status, and report task health. |

---

## 5. Medallion Architecture: Bronze & Silver Layers (Phase 4)

| File Key / Path | Content & Purpose |
|---|---|
| [src/databricks/bronze/ingest_redpanda_to_bronze.py](file:///d:/Uber_DWH/src/databricks/bronze/ingest_redpanda_to_bronze.py) | Spark Structured Streaming pipeline reading raw JSON CDC events from Redpanda topics into Delta Lake Bronze tables. Computes SHA256 event fingerprints for deduplication, captures raw `before`/`after` envelopes with Debezium metadata, and routes unparseable events into a Dead Letter Queue (`bronze_corrupted_records`). |
| [src/databricks/silver/merge_bronze_to_silver.py](file:///d:/Uber_DWH/src/databricks/silver/merge_bronze_to_silver.py) | Delta Lake Silver layer state replay and SCD processor. Replays micro-batch CDC events using Delta `MERGE`. Implements SCD Type 1 upserts/soft-deletes for mutable entities and SCD Type 2 historical dimension tracking (`is_current`, `start_date`, `end_date`) for `users` and `drivers`. |
| [src/databricks/silver/soda_checks.yml](file:///d:/Uber_DWH/src/databricks/silver/soda_checks.yml) | SodaCL data quality audit rules for Silver Delta tables: uniqueness and non-null constraints on primary keys, email formatting, valid rating ranges (0.0 to 5.0), non-negative fares, surge multiplier minimums (>= 1.0), and valid GPS coordinate boundaries (-90 to +90 lat, -180 to +180 lng). |
| [src/databricks/silver/run_soda_scan.py](file:///d:/Uber_DWH/src/databricks/silver/run_soda_scan.py) | In-memory Soda Core audit runner via `soda-core-spark`. Mounts Silver Delta tables as Spark temporary SQL views and executes assertions from `soda_checks.yml`. |

---

## 6. Gold Layer Dimensional Warehouse: dbt-core (Phase 5)

| File Key / Path | Content & Purpose |
|---|---|
| [dbt_warehouse/dbt_project.yml](file:///d:/Uber_DWH/dbt_warehouse/dbt_project.yml) | dbt project root configuration (`uber_dw_gold`). Defines model path configurations, staging schema view materializations, and Kimball marts star schema configurations (tables for `dim_*` and Delta Lake `merge` incremental materializations for `fact_trips`, `fact_trip_requests`, `fact_driver_pings`). |
| [dbt_warehouse/packages.yml](file:///d:/Uber_DWH/dbt_warehouse/packages.yml) | dbt package dependencies: imports `dbt-labs/dbt_utils` (surrogate keys, schema tests) and `calogica/dbt_expectations` (advanced Great Expectations-style assertions). |
| [dbt_warehouse/profiles.yml](file:///d:/Uber_DWH/dbt_warehouse/profiles.yml) | Active connection profiles for local development and Databricks endpoints. |
| [dbt_warehouse/profiles.yml.example](file:///d:/Uber_DWH/dbt_warehouse/profiles.yml.example) | Template connection configurations for Databricks Lakehouse endpoints, local Spark Thrift servers, and DuckDB development engines. |

---

## 7. Containerization & Docker Customizations

| File Key / Path | Content & Purpose |
|---|---|
| [docker/spark/Dockerfile](file:///d:/Uber_DWH/docker/spark/Dockerfile) | Custom Spark Docker image definition based on `apache/spark:3.5.1`. Pre-installs Delta Lake (`delta-spark==3.0.0`), `soda-core-spark`, `psycopg2-binary`, and pre-downloads Spark Kafka and Delta jars. |

---

## 8. Scripts, Utilities & Testing

| File Key / Path | Content & Purpose |
|---|---|
| [scripts/clean_simulation_data.py](file:///d:/Uber_DWH/scripts/clean_simulation_data.py) | Database maintenance utility to truncate transactional tables (`trips`, `trip_requests`, `driver_locations`, `payments`, `ratings`, `promotions`) with `CASCADE` while preserving baseline seed dimensions (`cities`, `users`, `drivers`, `vehicles`). |
| [scripts/test_silver_standalone.py](file:///d:/Uber_DWH/scripts/test_silver_standalone.py) | Standalone script demonstrating and verifying Silver SCD Type 1 and Type 2 Delta merge logic locally with an in-memory test Spark batch. |
| [tests/test_silver_merge.py](file:///d:/Uber_DWH/tests/test_silver_merge.py) | PySpark unit tests evaluating `process_silver_scd1` and `process_silver_scd2` from `src/databricks/silver/merge_bronze_to_silver.py`. |

---

## 9. Documentation & Phased Implementation Plans

| File Key / Path | Content & Purpose |
|---|---|
| [docs/project_blueprint.md](file:///d:/Uber_DWH/docs/project_blueprint.md) | High-level master architecture blueprint, ASCII pipeline diagrams, and 7-phase deliverables roadmap. |
| [docs/detailed_plans_by_phase/phase_2_implementation_plan.md](file:///d:/Uber_DWH/docs/detailed_plans_by_phase/phase_2_implementation_plan.md) | Technical implementation plan for Phase 2: PostgreSQL DDL, WAL configuration, and multi-threaded Python generator. |
| [docs/detailed_plans_by_phase/phase_3_implementation_plan.md](file:///d:/Uber_DWH/docs/detailed_plans_by_phase/phase_3_implementation_plan.md) | Technical implementation plan for Phase 3: Debezium CDC connector, Redpanda topics, and schema drift handling. |
| [docs/detailed_plans_by_phase/phase_4_implementation_plan.md](file:///d:/Uber_DWH/docs/detailed_plans_by_phase/phase_4_implementation_plan.md) | Technical implementation plan for Phase 4: Databricks/Spark Bronze streaming ingestion, Silver state reconstruction, SCD Type 2 tracking, and Soda Core tests. |
| [docs/detailed_plans_by_phase/phase_5_implementation_plan.md](file:///d:/Uber_DWH/docs/detailed_plans_by_phase/phase_5_implementation_plan.md) | Technical implementation plan for Phase 5: Kimball star schema design, dbt modeling hierarchy, surrogate keys, and dbt-expectations tests. |

---

## 10. Data, Runtime & Placeholder Directories

| Directory Key / Path | Content & Purpose |
|---|---|
| `airflow/` | Apache Airflow home directory containing `dags/` (DAG definitions), `plugins/` (custom Airflow operators), and `logs/` (task execution run logs). |
| `bi_dashboards/` | Directory designated for Metabase dashboard queries, export definitions, and analytical reporting assets. |
| `data/delta/` | Storage root for local Delta Lake tables (`silver_users`, `silver_drivers`, `silver_trips`, etc.) and streaming checkpoints. |
| `data/hadoop/bin/` | Windows Hadoop binaries (`hadoop.dll`, `winutils.exe`) required for local PySpark and Delta Lake filesystem operations on Windows OS. |

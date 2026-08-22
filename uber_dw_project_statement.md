# Real-Time Ride-Hailing Data Warehouse
### CDC-Based Lakehouse ETL Pipeline (Uber-Inspired OLTP Source)

---

## 1. Project Statement

This project builds a production-style data warehouse from scratch, starting at a simulated OLTP transactional system modeled closely on Uber's ride-hailing domain, and ending at a Kimball-style star schema serving analytics dashboards.

The goal is not to reproduce a tutorial ETL pipeline that batch-loads a static CSV. Instead, this project simulates a live, continuously-writing OLTP source with realistic transactional behavior (concurrent inserts, multi-step status updates on the same row, address/vehicle changes over time), and builds a **Change Data Capture (CDC)-based ingestion pipeline** — the same architectural pattern used in production data platforms — to stream those changes into a lakehouse, transform them through a medallion architecture, and model them into a dimensional warehouse for BI consumption.

The entire stack runs at **$0**, using open-source and free-tier tools exclusively.

### Why CDC, not batch polling

Most student ETL projects poll a source table on a schedule (`SELECT * WHERE updated_at > last_run`). This approach silently loses intermediate state changes, cannot capture deletes, and adds read load to the production database on every poll. This project instead reads directly from Postgres's **write-ahead log (WAL)** via logical replication, capturing every insert, update, and delete as an ordered event stream — mirroring how real production CDC pipelines (Uber, Netflix, Airbnb, etc.) move data from OLTP systems into analytical stores.

---

## 2. Source OLTP Schema (Uber-Inspired)

The source system is a PostgreSQL database representing a ride-hailing platform's operational data. It is intentionally modeled to require CDC rather than simple batch extraction — trip records are updated multiple times as they move through a status lifecycle, and dimensional attributes (rider address, driver vehicle) change over time.

### Identity & Assets

```sql
users (
  user_id PK, first_name, last_name, email, phone,
  signup_at, home_city_id FK, updated_at
)

drivers (
  driver_id PK, user_id FK,
  license_number, vehicle_id FK,
  status,                  -- offline / available / en_route / on_trip
  rating_avg, onboarded_at, updated_at
)

vehicles (
  vehicle_id PK, driver_id FK, make, model, year,
  plate_number, vehicle_type,   -- uberx / comfort / xl / black
  updated_at
)

cities (
  city_id PK, city_name, country, surge_enabled, timezone
)
```

### Core Trip Lifecycle

```sql
trip_requests (
  request_id PK, rider_id FK, city_id FK,
  pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
  requested_vehicle_type, requested_at,
  estimated_fare, surge_multiplier,
  status,                  -- searching / matched / expired / cancelled
  updated_at
)

trips (
  trip_id PK, request_id FK, rider_id FK, driver_id FK, vehicle_id FK,
  pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
  status,                  -- matched / driver_en_route / arrived /
                            -- in_progress / completed / cancelled
  requested_at, matched_at, driver_arrived_at,
  started_at, completed_at, cancelled_at,
  distance_km, duration_seconds,
  base_fare, surge_multiplier, final_fare,
  cancellation_reason, cancelled_by,   -- rider / driver / system
  updated_at
)
```

### Real-Time Telemetry

```sql
driver_locations (
  location_id PK, driver_id FK, trip_id FK NULLABLE,
  lat, lng, heading, speed_kmh, recorded_at
)
```

### Payments & Trust

```sql
payments (
  payment_id PK, trip_id FK, rider_id FK,
  amount, payment_method,   -- card / wallet / cash
  status,                    -- pending / succeeded / failed / refunded
  processed_at, updated_at
)

ratings (
  rating_id PK, trip_id FK, rated_by,   -- rider / driver
  rating_value, comment, created_at
)

promotions (
  promo_id PK, rider_id FK, code, discount_type, discount_value,
  applied_trip_id FK NULLABLE, applied_at
)
```

### Design Notes

- **`trip_requests` is separate from `trips`** — a meaningful share of requests never convert into a matched trip, enabling funnel analysis (request → match → completion) in the warehouse layer.
- **`trips.status`** is a full state machine with a timestamp per transition, generating repeated UPDATEs against the same primary key — the core reason this source requires CDC rather than batch extraction.
- **`driver_locations`** is high-frequency and append-only, serving as the throughput/volume showcase of the pipeline and enabling ETA-accuracy and driver-utilization analysis downstream.
- **Estimated fare (on `trip_requests`) vs. final fare (on `trips`)** reflects real fare computation behavior, where surge and actual trip distance/time can diverge from the initial estimate.
- **Dimensional attributes that change over time** (rider address, driver vehicle assignment) are the basis for Slowly Changing Dimension (Type 2) modeling in the warehouse layer.

---

## 3. Tech Stack ($0 Budget)

| Layer | Tool | Purpose |
|---|---|---|
| OLTP Source | **PostgreSQL** (Docker) | Simulated production database with logical replication (WAL) enabled |
| Load Generation | **Python** | Script simulating realistic, concurrent ride-hailing traffic and state transitions |
| Change Data Capture | **Debezium** | Reads Postgres WAL, emits structured change events (insert/update/delete) per table |
| Event Streaming | **Redpanda** | Kafka API-compatible broker; durable, ordered buffer decoupling source from lakehouse |
| Lakehouse / Compute | **Databricks Community Edition** | Structured Streaming ingestion, Bronze/Silver/Gold medallion architecture |
| Transformation | **dbt-core** | Builds and tests the Gold-layer Kimball star schema |
| Orchestration | **Apache Airflow** (Docker) | Schedules and triggers pipeline runs via Databricks REST API |
| BI / Consumption | **Metabase** (self-hosted) | Dashboards for trip funnel, driver utilization, revenue metrics |

---

## 4. Architecture Overview

```
PostgreSQL (OLTP, WAL logical replication)
        │
        ▼
   Debezium (CDC connector)
        │
        ▼
   Redpanda (Kafka-compatible event log)
        │
        ▼
Databricks — Bronze  (raw CDC events, append-only, full audit trail)
        │
        ▼
Databricks — Silver  (CDC replay, dedup, SCD Type 2 dimensions)
        │
        ▼
Databricks — Gold    (dbt-core: Kimball star schema, tested)
        │
        ▼
   Metabase (BI dashboards)
        ▲
        │
   Airflow (orchestration across all stages)
```

---

## 5. Target Gold-Layer Star Schema (Preview)

- **`dim_rider`** (SCD2 — tracks home city / address changes)
- **`dim_driver`** (SCD2 — tracks vehicle and rating changes)
- **`dim_vehicle`**
- **`dim_city`**
- **`dim_date`**
- **`fact_trips`** — grain: one row per completed trip; measures include time-to-match, wait time, trip duration, fare, surge multiplier
- **`fact_trip_requests`** — funnel analysis: searching → matched → completed/cancelled
- **`fact_driver_pings`** — high-volume telemetry fact table for utilization and ETA-accuracy analysis

---

## 6. Deliverables

- Public GitHub repository with full pipeline source code and setup instructions
- Architecture diagram
- dbt documentation (auto-generated data lineage and test results)
- Metabase dashboard covering trip funnel, revenue, and driver utilization
- Written technical walkthrough (blog post / LinkedIn article) explaining the CDC design decisions and what differentiates this from batch ETL

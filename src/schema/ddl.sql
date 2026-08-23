-- =============================================================================
-- Uber Lakehouse Platform - Source OLTP Database Schema (PostgreSQL 15+)
-- =============================================================================
-- Target Database: uber_oltp
-- Description: Defines the 10 relational tables for the ride-hailing domain,
--              including state machines, telemetry, payments, ratings, and
--              CDC (Change Data Capture) configuration via REPLICA IDENTITY FULL.
-- =============================================================================

-- Ensure we are connected to the uber_oltp database
\c uber_oltp;

-- -----------------------------------------------------------------------------
-- Helper Function: Automatic `updated_at` Timestamp Refresh
-- -----------------------------------------------------------------------------
-- Intuition: Captures exact modification times whenever a row is UPDATED.
-- CDC Relevance: Ensures Debezium receives updated_at timestamps for CDC replay ordering.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 1. Table: cities
-- Reference data representing geographic operating zones for ride services.
-- =============================================================================
CREATE TABLE IF NOT EXISTS cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    surge_enabled BOOLEAN DEFAULT TRUE NOT NULL,
    timezone VARCHAR(50) DEFAULT 'UTC' NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Trigger to auto-update updated_at on cities modifications
CREATE TRIGGER trigger_cities_updated_at
    BEFORE UPDATE ON cities
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

-- Enable full row CDC image logging for updates/deletes in Debezium
ALTER TABLE cities REPLICA IDENTITY FULL;


-- =============================================================================
-- 2. Table: users
-- Core identity table for ride-hailing riders.
-- =============================================================================
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(30) UNIQUE NOT NULL,
    signup_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    home_city_id INT REFERENCES cities(city_id) ON DELETE SET NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Trigger to auto-update updated_at on users modifications
CREATE TRIGGER trigger_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

-- Enable full row CDC image logging for updates/deletes in Debezium
ALTER TABLE users REPLICA IDENTITY FULL;


-- =============================================================================
-- 3. Table: vehicles
-- Fleet assets registered to drivers in the platform.
-- =============================================================================
CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    driver_id INT, -- Foreign key to drivers added after drivers table creation
    make VARCHAR(50) NOT NULL,
    model VARCHAR(50) NOT NULL,
    year INT NOT NULL,
    plate_number VARCHAR(20) UNIQUE NOT NULL,
    vehicle_type VARCHAR(30) CHECK (vehicle_type IN ('uberx', 'comfort', 'xl', 'black')) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Trigger to auto-update updated_at on vehicles modifications
CREATE TRIGGER trigger_vehicles_updated_at
    BEFORE UPDATE ON vehicles
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

-- Enable full row CDC image logging for updates/deletes in Debezium
ALTER TABLE vehicles REPLICA IDENTITY FULL;


-- =============================================================================
-- 4. Table: drivers
-- Driver profiles, operating statuses, vehicle assignments, and ratings.
-- =============================================================================
CREATE TABLE IF NOT EXISTS drivers (
    driver_id SERIAL PRIMARY KEY,
    user_id INT UNIQUE NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    license_number VARCHAR(50) UNIQUE NOT NULL,
    vehicle_id INT REFERENCES vehicles(vehicle_id) ON DELETE SET NULL,
    status VARCHAR(30) CHECK (status IN ('offline', 'available', 'en_route', 'on_trip')) DEFAULT 'offline' NOT NULL,
    rating_avg NUMERIC(3, 2) DEFAULT 5.00 NOT NULL,
    onboarded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Circular foreign key: link vehicles.driver_id -> drivers.driver_id
ALTER TABLE vehicles
    ADD CONSTRAINT fk_vehicles_driver
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id) ON DELETE SET NULL;

-- Trigger to auto-update updated_at on drivers modifications
CREATE TRIGGER trigger_drivers_updated_at
    BEFORE UPDATE ON drivers
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

-- Enable full row CDC image logging for updates/deletes in Debezium
ALTER TABLE drivers REPLICA IDENTITY FULL;


-- =============================================================================
-- 5. Table: trip_requests
-- Rider intent lifecycle (searching -> matched -> expired / cancelled).
-- =============================================================================
CREATE TABLE IF NOT EXISTS trip_requests (
    request_id SERIAL PRIMARY KEY,
    rider_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    city_id INT NOT NULL REFERENCES cities(city_id) ON DELETE CASCADE,
    pickup_lat NUMERIC(10, 7) NOT NULL,
    pickup_lng NUMERIC(10, 7) NOT NULL,
    dropoff_lat NUMERIC(10, 7) NOT NULL,
    dropoff_lng NUMERIC(10, 7) NOT NULL,
    requested_vehicle_type VARCHAR(30) CHECK (requested_vehicle_type IN ('uberx', 'comfort', 'xl', 'black')) NOT NULL,
    requested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    estimated_fare NUMERIC(10, 2) NOT NULL,
    surge_multiplier NUMERIC(3, 2) DEFAULT 1.00 NOT NULL,
    status VARCHAR(30) CHECK (status IN ('searching', 'matched', 'expired', 'cancelled')) DEFAULT 'searching' NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Trigger to auto-update updated_at on trip_requests modifications
CREATE TRIGGER trigger_trip_requests_updated_at
    BEFORE UPDATE ON trip_requests
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

-- Enable full row CDC image logging for updates/deletes in Debezium
ALTER TABLE trip_requests REPLICA IDENTITY FULL;


-- =============================================================================
-- 6. Table: trips
-- Core transactional state machine for ride progression and timestamps.
-- =============================================================================
CREATE TABLE IF NOT EXISTS trips (
    trip_id SERIAL PRIMARY KEY,
    request_id INT UNIQUE NOT NULL REFERENCES trip_requests(request_id) ON DELETE CASCADE,
    rider_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    driver_id INT NOT NULL REFERENCES drivers(driver_id) ON DELETE CASCADE,
    vehicle_id INT NOT NULL REFERENCES vehicles(vehicle_id) ON DELETE CASCADE,
    pickup_lat NUMERIC(10, 7) NOT NULL,
    pickup_lng NUMERIC(10, 7) NOT NULL,
    dropoff_lat NUMERIC(10, 7) NOT NULL,
    dropoff_lng NUMERIC(10, 7) NOT NULL,
    status VARCHAR(30) CHECK (status IN ('matched', 'driver_en_route', 'arrived', 'in_progress', 'completed', 'cancelled')) DEFAULT 'matched' NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    matched_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    driver_arrived_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    distance_km NUMERIC(6, 2),
    duration_seconds INT,
    base_fare NUMERIC(10, 2) NOT NULL,
    surge_multiplier NUMERIC(3, 2) DEFAULT 1.00 NOT NULL,
    final_fare NUMERIC(10, 2),
    cancellation_reason VARCHAR(255),
    cancelled_by VARCHAR(30) CHECK (cancelled_by IN ('rider', 'driver', 'system')),
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Trigger to auto-update updated_at on trips modifications
CREATE TRIGGER trigger_trips_updated_at
    BEFORE UPDATE ON trips
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

-- Enable full row CDC image logging for updates/deletes in Debezium
ALTER TABLE trips REPLICA IDENTITY FULL;


-- =============================================================================
-- 7. Table: driver_locations
-- High-frequency GPS telemetry stream for driver spatial positioning.
-- =============================================================================
CREATE TABLE IF NOT EXISTS driver_locations (
    location_id BIGSERIAL PRIMARY KEY,
    driver_id INT NOT NULL REFERENCES drivers(driver_id) ON DELETE CASCADE,
    trip_id INT REFERENCES trips(trip_id) ON DELETE SET NULL,
    lat NUMERIC(10, 7) NOT NULL,
    lng NUMERIC(10, 7) NOT NULL,
    heading NUMERIC(5, 2) DEFAULT 0.00 NOT NULL,
    speed_kmh NUMERIC(5, 2) DEFAULT 0.00 NOT NULL,
    recorded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Enable full row CDC image logging for updates/deletes in Debezium
ALTER TABLE driver_locations REPLICA IDENTITY FULL;


-- =============================================================================
-- 8. Table: payments
-- Financial settlement table for trip payment processing.
-- =============================================================================
CREATE TABLE IF NOT EXISTS payments (
    payment_id SERIAL PRIMARY KEY,
    trip_id INT UNIQUE NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE,
    rider_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    amount NUMERIC(10, 2) NOT NULL,
    payment_method VARCHAR(30) CHECK (payment_method IN ('credit_card', 'debit_card', 'cash', 'uber_wallet', 'paypal')) NOT NULL,
    status VARCHAR(30) CHECK (status IN ('pending', 'succeeded', 'failed', 'refunded')) DEFAULT 'pending' NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Trigger to auto-update updated_at on payments modifications
CREATE TRIGGER trigger_payments_updated_at
    BEFORE UPDATE ON payments
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_timestamp();

-- Enable full row CDC image logging for updates/deletes in Debezium
ALTER TABLE payments REPLICA IDENTITY FULL;


-- =============================================================================
-- 9. Table: ratings
-- Feedback and numerical ratings submitted by riders or drivers.
-- =============================================================================
CREATE TABLE IF NOT EXISTS ratings (
    rating_id SERIAL PRIMARY KEY,
    trip_id INT UNIQUE NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE,
    rated_by VARCHAR(30) CHECK (rated_by IN ('rider', 'driver')) NOT NULL,
    rating_value INT CHECK (rating_value BETWEEN 1 AND 5) NOT NULL,
    comment TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Enable full row CDC image logging for updates/deletes in Debezium
ALTER TABLE ratings REPLICA IDENTITY FULL;


-- =============================================================================
-- 10. Table: promotions
-- Promotional discount codes and application status per rider.
-- =============================================================================
CREATE TABLE IF NOT EXISTS promotions (
    promo_id SERIAL PRIMARY KEY,
    rider_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    code VARCHAR(50) NOT NULL,
    discount_type VARCHAR(30) CHECK (discount_type IN ('percentage', 'fixed_amount')) NOT NULL,
    discount_value NUMERIC(10, 2) NOT NULL,
    applied_trip_id INT REFERENCES trips(trip_id) ON DELETE SET NULL,
    applied_at TIMESTAMPTZ
);

-- Enable full row CDC image logging for updates/deletes in Debezium
ALTER TABLE promotions REPLICA IDENTITY FULL;

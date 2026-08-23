"""
=============================================================================
Uber Lakehouse Platform - SQLAlchemy ORM Models
=============================================================================
Description: Maps all 10 relational tables defined in src/schema/ddl.sql
             to Python SQLAlchemy ORM classes for seamless data generator queries.
=============================================================================
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, BigInteger, String, Numeric, Boolean,
    DateTime, ForeignKey, Text
)
from sqlalchemy.orm import relationship
from generator.db import Base


class City(Base):
    """Reflects cities table: Geographic operational zones."""
    __tablename__ = "cities"

    city_id = Column(Integer, primary_key=True, autoincrement=True)
    city_name = Column(String(100), nullable=False)
    country = Column(String(100), nullable=False)
    surge_enabled = Column(Boolean, default=True, nullable=False)
    timezone = Column(String(50), default="UTC", nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class User(Base):
    """Reflects users table: Rider accounts and personal profiles."""
    __tablename__ = "users"

    user_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(255), unique=True, nullable=False)
    phone = Column(String(30), unique=True, nullable=False)
    signup_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    home_city_id = Column(Integer, ForeignKey("cities.city_id"))
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class Vehicle(Base):
    """Reflects vehicles table: Fleet vehicles registered on the platform."""
    __tablename__ = "vehicles"

    vehicle_id = Column(Integer, primary_key=True, autoincrement=True)
    driver_id = Column(Integer, ForeignKey("drivers.driver_id"))
    make = Column(String(50), nullable=False)
    model = Column(String(50), nullable=False)
    year = Column(Integer, nullable=False)
    plate_number = Column(String(20), unique=True, nullable=False)
    vehicle_type = Column(String(30), nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class Driver(Base):
    """Reflects drivers table: Driver profiles, operating statuses, and ratings."""
    __tablename__ = "drivers"

    driver_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), unique=True, nullable=False)
    license_number = Column(String(50), unique=True, nullable=False)
    vehicle_id = Column(Integer, ForeignKey("vehicles.vehicle_id"))
    status = Column(String(30), default="offline", nullable=False)
    rating_avg = Column(Numeric(3, 2), default=5.00, nullable=False)
    onboarded_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class TripRequest(Base):
    """Reflects trip_requests table: Rider booking requests and matching funnel."""
    __tablename__ = "trip_requests"

    request_id = Column(Integer, primary_key=True, autoincrement=True)
    rider_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    city_id = Column(Integer, ForeignKey("cities.city_id"), nullable=False)
    pickup_lat = Column(Numeric(10, 7), nullable=False)
    pickup_lng = Column(Numeric(10, 7), nullable=False)
    dropoff_lat = Column(Numeric(10, 7), nullable=False)
    dropoff_lng = Column(Numeric(10, 7), nullable=False)
    requested_vehicle_type = Column(String(30), nullable=False)
    requested_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    estimated_fare = Column(Numeric(10, 2), nullable=False)
    surge_multiplier = Column(Numeric(3, 2), default=1.00, nullable=False)
    status = Column(String(30), default="searching", nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class Trip(Base):
    """Reflects trips table: Matched and active ride lifecycle transactions."""
    __tablename__ = "trips"

    trip_id = Column(Integer, primary_key=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey("trip_requests.request_id"), unique=True, nullable=False)
    rider_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    driver_id = Column(Integer, ForeignKey("drivers.driver_id"), nullable=False)
    vehicle_id = Column(Integer, ForeignKey("vehicles.vehicle_id"), nullable=False)
    pickup_lat = Column(Numeric(10, 7), nullable=False)
    pickup_lng = Column(Numeric(10, 7), nullable=False)
    dropoff_lat = Column(Numeric(10, 7), nullable=False)
    dropoff_lng = Column(Numeric(10, 7), nullable=False)
    status = Column(String(30), default="matched", nullable=False)
    requested_at = Column(DateTime, nullable=False)
    matched_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    driver_arrived_at = Column(DateTime)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    cancelled_at = Column(DateTime)
    distance_km = Column(Numeric(6, 2))
    duration_seconds = Column(Integer)
    base_fare = Column(Numeric(10, 2), nullable=False)
    surge_multiplier = Column(Numeric(3, 2), default=1.00, nullable=False)
    final_fare = Column(Numeric(10, 2))
    cancellation_reason = Column(String(255))
    cancelled_by = Column(String(30))
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class DriverLocation(Base):
    """Reflects driver_locations table: High-frequency GPS telemetry pings."""
    __tablename__ = "driver_locations"

    location_id = Column(BigInteger, primary_key=True, autoincrement=True)
    driver_id = Column(Integer, ForeignKey("drivers.driver_id"), nullable=False)
    trip_id = Column(Integer, ForeignKey("trips.trip_id"))
    lat = Column(Numeric(10, 7), nullable=False)
    lng = Column(Numeric(10, 7), nullable=False)
    heading = Column(Numeric(5, 2), default=0.00, nullable=False)
    speed_kmh = Column(Numeric(5, 2), default=0.00, nullable=False)
    recorded_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class Payment(Base):
    """Reflects payments table: Financial settlement for completed trips."""
    __tablename__ = "payments"

    payment_id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(Integer, ForeignKey("trips.trip_id"), unique=True, nullable=False)
    rider_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    amount = Column(Numeric(10, 2), nullable=False)
    payment_method = Column(String(30), nullable=False)
    status = Column(String(30), default="pending", nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class Rating(Base):
    """Reflects ratings table: Post-trip feedback and score."""
    __tablename__ = "ratings"

    rating_id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(Integer, ForeignKey("trips.trip_id"), unique=True, nullable=False)
    rated_by = Column(String(30), nullable=False)
    rating_value = Column(Integer, nullable=False)
    comment = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class Promotion(Base):
    """Reflects promotions table: Promotional discounts applied to rides."""
    __tablename__ = "promotions"

    promo_id = Column(Integer, primary_key=True, autoincrement=True)
    rider_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    code = Column(String(50), nullable=False)
    discount_type = Column(String(30), nullable=False)
    discount_value = Column(Numeric(10, 2), nullable=False)
    applied_trip_id = Column(Integer, ForeignKey("trips.trip_id"))
    applied_at = Column(DateTime)

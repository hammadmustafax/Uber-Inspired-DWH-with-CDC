"""
=============================================================================
Uber Lakehouse Platform - Real-Time Operational Event Simulator
=============================================================================
Description: Executes the ride-hailing transactional state machine, driver matching,
             telemetry pings, payment settlement, and rating creation in real time.
=============================================================================
"""

import asyncio
import random
from datetime import datetime
from sqlalchemy import func
from generator.config import (
    TRIP_REQUEST_INTERVAL_SEC, TELEMETRY_INTERVAL_SEC,
    RIDER_CANCELLATION_RATE, DRIVER_CANCELLATION_RATE, VEHICLE_TYPES
)
from generator.db import get_db_session
from generator.models import (
    City, User, Driver, Vehicle, TripRequest, Trip, Payment, Rating
)
from generator.telemetry import (
    generate_random_coordinate, haversine_distance_km, move_towards,
    create_driver_telemetry_ping
)

# Realistic fare estimation parameters
BASE_FARES = {"uberx": 2.50, "comfort": 4.00, "xl": 6.00, "black": 10.00}
PER_KM_RATES = {"uberx": 1.20, "comfort": 1.75, "xl": 2.50, "black": 3.80}

CANCELLATION_REASONS = [
    "Driver taking too long", "Changed mind", "Wrong pickup location",
    "Driver requested cash", "Vehicle issues"
]

RATING_COMMENTS = [
    "Great ride!", "Clean car and friendly driver.", "Smooth driving.",
    "Driver was quiet.", "Took a slightly longer route.", "Excellent service!"
]


def create_trip_request():
    """
    Generates a new ride request from a random rider in a random operating city.
    """
    with get_db_session() as session:
        # Pick random city and rider
        city = session.query(City).order_by(func.random()).first()
        rider = session.query(User).order_by(func.random()).first()

        if not city or not rider:
            return None

        # Generate pickup & dropoff within 12km of city center
        pickup_lat, pickup_lng = generate_random_coordinate(city.lat, city.lng, radius_km=8.0)
        dropoff_lat, dropoff_lng = generate_random_coordinate(city.lat, city.lng, radius_km=12.0)

        distance_km = haversine_distance_km(pickup_lat, pickup_lng, dropoff_lat, dropoff_lng)
        vehicle_type = random.choice(VEHICLE_TYPES)

        # Base price calculation
        base_price = BASE_FARES[vehicle_type] + (distance_km * PER_KM_RATES[vehicle_type])

        # Surge multiplier
        surge = 1.00
        if city.surge_enabled and random.random() < 0.35:
            surge = round(random.uniform(1.20, 2.20), 2)

        estimated_fare = round(base_price * surge, 2)

        # Check for immediate rider cancellation before matching
        status = "searching"
        if random.random() < RIDER_CANCELLATION_RATE:
            status = "cancelled"

        request = TripRequest(
            rider_id=rider.user_id,
            city_id=city.city_id,
            pickup_lat=pickup_lat,
            pickup_lng=pickup_lng,
            dropoff_lat=dropoff_lat,
            dropoff_lng=dropoff_lng,
            requested_vehicle_type=vehicle_type,
            requested_at=datetime.utcnow(),
            estimated_fare=estimated_fare,
            surge_multiplier=surge,
            status=status
        )
        session.add(request)
        session.flush()

        if status == "searching":
            print(f"[Request #{request.request_id}] New '{vehicle_type}' request in {city.city_name} (${estimated_fare}, Surge: {surge}x)")
        else:
            print(f"[Request #{request.request_id}] Cancelled by rider during search phase.")

        return request.request_id


def match_searching_requests():
    """
    Finds pending 'searching' trip requests and matches available drivers.
    """
    with get_db_session() as session:
        searching_requests = session.query(TripRequest).filter(TripRequest.status == "searching").limit(10).all()

        for req in searching_requests:
            # Find an available driver
            driver = session.query(Driver).filter(Driver.status == "available").first()
            if not driver:
                continue  # No available drivers right now

            vehicle = session.query(Vehicle).filter(Vehicle.vehicle_id == driver.vehicle_id).first()
            if not vehicle:
                continue

            # Update request status to matched
            req.status = "matched"

            # Create trip record
            trip = Trip(
                request_id=req.request_id,
                rider_id=req.rider_id,
                driver_id=driver.driver_id,
                vehicle_id=vehicle.vehicle_id,
                pickup_lat=req.pickup_lat,
                pickup_lng=req.pickup_lng,
                dropoff_lat=req.dropoff_lat,
                dropoff_lng=req.dropoff_lng,
                status="matched",
                requested_at=req.requested_at,
                matched_at=datetime.utcnow(),
                base_fare=BASE_FARES.get(req.requested_vehicle_type, 2.50),
                surge_multiplier=req.surge_multiplier
            )
            session.add(trip)

            # Update driver status
            driver.status = "en_route"
            print(f"[Match] Trip #{trip.trip_id} matched Driver #{driver.driver_id} for Request #{req.request_id}")


def step_active_trips():
    """
    Advances active trips through state machine transitions and generates GPS telemetry pings.
    """
    with get_db_session() as session:
        active_trips = session.query(Trip).filter(
            Trip.status.in_(["matched", "driver_en_route", "arrived", "in_progress"])
        ).all()

        for trip in active_trips:
            driver = session.query(Driver).filter(Driver.driver_id == trip.driver_id).first()
            if not driver:
                continue

            # --- State 1: Matched -> Driver En Route ---
            if trip.status == "matched":
                # Check driver cancellation chance
                if random.random() < DRIVER_CANCELLATION_RATE:
                    trip.status = "cancelled"
                    trip.cancelled_at = datetime.utcnow()
                    trip.cancelled_by = "driver"
                    trip.cancellation_reason = random.choice(CANCELLATION_REASONS)
                    driver.status = "available"
                    print(f"[Cancel] Trip #{trip.trip_id} cancelled by Driver #{driver.driver_id}")
                    continue

                trip.status = "driver_en_route"
                driver.status = "en_route"

            # --- State 2: Driver En Route (Moving to Pickup) ---
            elif trip.status == "driver_en_route":
                # Get current driver coordinates or default near pickup
                pickup_lat, pickup_lng = float(trip.pickup_lat), float(trip.pickup_lng)
                curr_lat = pickup_lat + random.uniform(-0.02, 0.02)
                curr_lng = pickup_lng + random.uniform(-0.02, 0.02)

                speed = random.uniform(30.0, 50.0)
                new_lat, new_lng, heading, arrived = move_towards(
                    curr_lat, curr_lng, pickup_lat, pickup_lng, speed, TELEMETRY_INTERVAL_SEC
                )

                create_driver_telemetry_ping(session, driver.driver_id, trip.trip_id, new_lat, new_lng, speed, heading)

                if arrived or random.random() < 0.40:
                    trip.status = "arrived"
                    trip.driver_arrived_at = datetime.utcnow()
                    print(f"[Arrived] Driver #{driver.driver_id} arrived at pickup for Trip #{trip.trip_id}")

            # --- State 3: Driver Arrived -> In Progress ---
            elif trip.status == "arrived":
                trip.status = "in_progress"
                trip.started_at = datetime.utcnow()
                driver.status = "on_trip"
                print(f"[Started] Trip #{trip.trip_id} is now IN_PROGRESS")

            # --- State 4: In Progress (Moving to Dropoff) ---
            elif trip.status == "in_progress":
                dropoff_lat, dropoff_lng = float(trip.dropoff_lat), float(trip.dropoff_lng)
                pickup_lat, pickup_lng = float(trip.pickup_lat), float(trip.pickup_lng)

                speed = random.uniform(25.0, 60.0)
                new_lat, new_lng, heading, arrived = move_towards(
                    pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, speed, TELEMETRY_INTERVAL_SEC
                )

                create_driver_telemetry_ping(session, driver.driver_id, trip.trip_id, new_lat, new_lng, speed, heading)

                # Complete trip step
                if arrived or random.random() < 0.30:
                    now = datetime.utcnow()
                    trip.status = "completed"
                    trip.completed_at = now

                    distance = haversine_distance_km(pickup_lat, pickup_lng, dropoff_lat, dropoff_lng)
                    duration = int((now - (trip.started_at or now)).total_seconds()) + random.randint(300, 1200)
                    
                    vehicle_type = session.query(Vehicle.vehicle_type).filter(Vehicle.vehicle_id == trip.vehicle_id).scalar() or "uberx"
                    final_fare = round((float(trip.base_fare) + (distance * PER_KM_RATES.get(vehicle_type, 1.20))) * float(trip.surge_multiplier), 2)

                    trip.distance_km = distance
                    trip.duration_seconds = duration
                    trip.final_fare = final_fare

                    # Free up driver
                    driver.status = "available"

                    # 1. Create Payment record
                    payment = Payment(
                        trip_id=trip.trip_id,
                        rider_id=trip.rider_id,
                        amount=final_fare,
                        payment_method=random.choice(["credit_card", "debit_card", "uber_wallet", "paypal"]),
                        status="succeeded",
                        processed_at=now
                    )
                    session.add(payment)

                    # 2. Create Rating record
                    rating_val = random.choices([5, 4, 3, 2, 1], weights=[70, 15, 8, 4, 3])[0]
                    rating = Rating(
                        trip_id=trip.trip_id,
                        rated_by="rider",
                        rating_value=rating_val,
                        comment=random.choice(RATING_COMMENTS) if rating_val <= 4 else "Great!",
                        created_at=now
                    )
                    session.add(rating)

                    print(f"[Completed] Trip #{trip.trip_id} completed! Distance: {distance}km, Final Fare: ${final_fare}, Rating: {rating_val}★")


async def run_simulation_loop(duration_seconds: int = None):
    """
    Main asynchronous event loop triggering requests, driver matches, and telemetry steps.
    """
    print("=" * 60)
    print("Launching Real-Time Operational Ride Simulator...")
    print("=" * 60)

    start_time = datetime.utcnow()

    while True:
        try:
            # 1. Generate new trip request
            create_trip_request()

            # 2. Match searching requests with available drivers
            match_searching_requests()

            # 3. Advance active trip states & emit telemetry pings
            step_active_trips()

            # Check max duration exit condition if set
            if duration_seconds:
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                if elapsed >= duration_seconds:
                    print(f"[Simulator] Reached target duration of {duration_seconds}s. Stopping.")
                    break

            await asyncio.sleep(TRIP_REQUEST_INTERVAL_SEC)

        except Exception as e:
            print(f"[Simulation Loop Error] {e}")
            await asyncio.sleep(1.0)

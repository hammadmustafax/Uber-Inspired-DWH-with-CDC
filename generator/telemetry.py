"""
=============================================================================
Uber Lakehouse Platform - GPS Telemetry & Spatial Movement Simulator
=============================================================================
Description: Provides Haversine distance calculations, heading/bearing math,
             and realistic GPS trajectory step updates for active drivers.
=============================================================================
"""

import math
import random
from datetime import datetime
from generator.models import DriverLocation


def haversine_distance_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """
    Intuition: Calculates the shortest distance over the Earth's surface (great-circle distance)
               between two latitude/longitude coordinates.
    Formula: Haversine equation using Earth radius R = 6371 km.
    """
    lat1, lng1, lat2, lng2 = map(float, [lat1, lng1, lat2, lng2])
    R = 6371.0  # Earth's radius in kilometers

    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)

    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lng / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return round(R * c, 2)


def calculate_heading(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """
    Calculates the compass bearing / heading angle (0° to 360°) from point 1 to point 2.
    """
    lat1, lng1, lat2, lng2 = map(float, [lat1, lng1, lat2, lng2])
    d_lng = math.radians(lng2 - lng1)
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)

    x = math.sin(d_lng) * math.cos(lat2_rad)
    y = math.cos(lat1_rad) * math.sin(lat2_rad) - (
        math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(d_lng)
    )

    initial_bearing = math.atan2(x, y)
    compass_bearing = (math.degrees(initial_bearing) + 360) % 360

    return round(compass_bearing, 2)


def generate_random_coordinate(center_lat: float, center_lng: float, radius_km: float = 10.0):
    """
    Generates a random GPS coordinate within a given kilometer radius of a city center.
    """
    center_lat = float(center_lat)
    center_lng = float(center_lng)
    # 1 degree latitude is approximately 111 km
    radius_in_degrees = radius_km / 111.0

    u = random.random()
    v = random.random()

    w = radius_in_degrees * math.sqrt(u)
    t = 2 * math.pi * v

    lat_offset = w * math.cos(t)
    lng_offset = w * math.sin(t) / math.cos(math.radians(center_lat))

    return (
        round(center_lat + lat_offset, 7),
        round(center_lng + lng_offset, 7)
    )


def move_towards(
    current_lat: float, current_lng: float,
    target_lat: float, target_lng: float,
    speed_kmh: float, delta_time_sec: float
):
    """
    Simulates moving a vehicle from (current_lat, current_lng) towards (target_lat, target_lng)
    at speed_kmh over delta_time_sec seconds.
    Returns: (new_lat, new_lng, heading, has_arrived)
    """
    current_lat, current_lng, target_lat, target_lng = map(float, [current_lat, current_lng, target_lat, target_lng])
    total_distance_km = haversine_distance_km(current_lat, current_lng, target_lat, target_lng)
    heading = calculate_heading(current_lat, current_lng, target_lat, target_lng)

    # Distance traveled in this time step (speed_kmh * hours)
    step_distance_km = speed_kmh * (delta_time_sec / 3600.0)

    # If step distance exceeds remaining distance, vehicle has arrived at destination
    if total_distance_km <= step_distance_km or total_distance_km == 0:
        return (target_lat, target_lng, heading, True)

    # Calculate fraction of trajectory completed
    fraction = step_distance_km / total_distance_km
    new_lat = current_lat + (target_lat - current_lat) * fraction
    new_lng = current_lng + (target_lng - current_lng) * fraction

    return (round(new_lat, 7), round(new_lng, 7), heading, False)


def create_driver_telemetry_ping(session, driver_id: int, trip_id: int, lat: float, lng: float, speed_kmh: float, heading: float):
    """
    Inserts a high-frequency driver GPS telemetry record into driver_locations.
    """
    ping = DriverLocation(
        driver_id=driver_id,
        trip_id=trip_id,
        lat=lat,
        lng=lng,
        heading=heading,
        speed_kmh=speed_kmh,
        recorded_at=datetime.utcnow()
    )
    session.add(ping)
    return ping

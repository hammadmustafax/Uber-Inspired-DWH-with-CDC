"""
=============================================================================
Uber Lakehouse Platform - Data Generator Configuration
=============================================================================
Description: Centralized configuration parameters for database connections,
             baseline data seeding volumes, and simulation parameters.
=============================================================================
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# -----------------------------------------------------------------------------
# Database Configuration
# Default connects to local Postgres exposed on port 5433 via docker-compose
# -----------------------------------------------------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5433/uber_oltp"
)

# -----------------------------------------------------------------------------
# Baseline Data Seeding Volume Defaults
# -----------------------------------------------------------------------------
DEFAULT_RIDERS_COUNT = 5000   # Total simulated riders (users table)
DEFAULT_DRIVERS_COUNT = 500   # Total active drivers (drivers table)
DEFAULT_VEHICLES_COUNT = 500  # Fleet vehicles assigned to drivers

# -----------------------------------------------------------------------------
# Simulation Parameters & Probabilities
# -----------------------------------------------------------------------------
# Delay in seconds between generating new trip requests
TRIP_REQUEST_INTERVAL_SEC = 0.5

# Time interval in seconds between driver GPS telemetry pings
TELEMETRY_INTERVAL_SEC = 2.0

# Probability of a rider cancelling during search or match phase (8%)
RIDER_CANCELLATION_RATE = 0.08

# Probability of a driver cancelling after matching (3%)
DRIVER_CANCELLATION_RATE = 0.03

# -----------------------------------------------------------------------------
# Geographic Cities for Simulation (City Center Coordinates & Timezones)
# -----------------------------------------------------------------------------
CITIES_DATA = [
    {
        "city_name": "New York",
        "country": "United States",
        "lat": 40.7128,
        "lng": -74.0060,
        "surge_enabled": True,
        "timezone": "America/New_York"
    },
    {
        "city_name": "San Francisco",
        "country": "United States",
        "lat": 37.7749,
        "lng": -122.4194,
        "surge_enabled": True,
        "timezone": "America/Los_Angeles"
    },
    {
        "city_name": "London",
        "country": "United Kingdom",
        "lat": 51.5074,
        "lng": -0.1278,
        "surge_enabled": True,
        "timezone": "Europe/London"
    },
    {
        "city_name": "São Paulo",
        "country": "Brazil",
        "lat": -23.5505,
        "lng": -46.6333,
        "surge_enabled": True,
        "timezone": "America/Sao_Paulo"
    },
    {
        "city_name": "Tokyo",
        "country": "Japan",
        "lat": 35.6762,
        "lng": 139.6503,
        "surge_enabled": False,  # Example: surge pricing legally restricted
        "timezone": "Asia/Tokyo"
    }
]

# Vehicle Categories matching schema ENUM / CHECK constraints
VEHICLE_TYPES = ["uberx", "comfort", "xl", "black"]

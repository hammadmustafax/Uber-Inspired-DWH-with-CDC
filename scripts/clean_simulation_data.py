"""
=============================================================================
Uber Lakehouse Platform - Simulation Data Cleanup Utility
=============================================================================
Description: Cleans up transient simulation transactional data (trips, requests,
             locations, payments, ratings, promotions) while preserving baseline
             seed tables (cities, users, drivers, vehicles).
=============================================================================
"""

import sys
import os

# Add root project directory to import path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from generator.db import get_db_session


def clean_simulation_data():
    """
    Truncates transactional tables and resets driver statuses back to available.
    """
    print("[Cleanup] Truncating simulation transactional tables...")
    with get_db_session() as session:
        # 1. Truncate transaction and telemetry tables
        truncate_query = text("""
            TRUNCATE TABLE 
                driver_locations,
                ratings,
                payments,
                promotions,
                trips,
                trip_requests
            RESTART IDENTITY CASCADE;
        """)
        session.execute(truncate_query)
        
        # 2. Reset driver statuses back to 'available'
        reset_drivers_query = text("UPDATE drivers SET status = 'available';")
        session.execute(reset_drivers_query)
        
        print("[Cleanup] Successfully wiped previous simulation records.")
        print("[Cleanup] Reset all driver statuses to 'available'.")


if __name__ == "__main__":
    clean_simulation_data()

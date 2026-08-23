"""
=============================================================================
Uber Lakehouse Platform - Baseline Database Seeder
=============================================================================
Description: Populates the PostgreSQL database with initial reference data
             (Cities, Users, Vehicles, Drivers) using Faker for realistic
             domain attributes.
=============================================================================
"""

import random
# pyrefly: ignore [missing-import]
from faker import Faker
from generator.config import (
    CITIES_DATA, DEFAULT_RIDERS_COUNT, DEFAULT_DRIVERS_COUNT, VEHICLE_TYPES
)
from generator.db import get_db_session
from generator.models import City, User, Vehicle, Driver

fake = Faker()

# Sample vehicle makes and models for realistic fleet generation
VEHICLE_MODELS = [
    ("Toyota", "Camry"), ("Honda", "Accord"), ("Tesla", "Model 3"),
    ("Hyundai", "Sonata"), ("Chevrolet", "Suburban"), ("Ford", "Explorer"),
    ("BMW", "5 Series"), ("Mercedes-Benz", "E-Class"), ("Toyota", "Corolla"),
    ("Honda", "Civic"), ("Tesla", "Model Y"), ("Hyundai", "Elantra"),
    ("Chevrolet", "Malibu"), ("Ford", "Mustang"), ("BMW", "3 Series"),
    ("Mercedes-Benz", "C-Class"), ("GWM", "Haval"), ("BYD", "Seal")
]


def seed_cities():
    """
    Seeds baseline geographic cities if the cities table is empty.
    Returns: List of created City ORM instances.
    """
    with get_db_session() as session:
        existing_cities = session.query(City).all()
        if existing_cities:
            print(f"[Seed] Cities already seeded ({len(existing_cities)} cities found).")
            return existing_cities

        print(f"[Seed] Seeding {len(CITIES_DATA)} default operating cities...")
        cities_to_create = []
        for c in CITIES_DATA:
            city = City(
                city_name=c["city_name"],
                country=c["country"],
                lat=c["lat"],
                lng=c["lng"],
                surge_enabled=c["surge_enabled"],
                timezone=c["timezone"]
            )
            cities_to_create.append(city)
            session.add(city)
        
        session.flush()
        print("[Seed] Cities successfully seeded.")
        return cities_to_create


def seed_users(num_users=DEFAULT_RIDERS_COUNT):
    """
    Seeds rider accounts (users) linked to random home cities.
    """
    with get_db_session() as session:
        existing_user_count = session.query(User).count()
        if existing_user_count >= num_users:
            print(f"[Seed] Users already seeded ({existing_user_count} users found).")
            return

        city_ids = [c.city_id for c in session.query(City.city_id).all()]
        if not city_ids:
            raise ValueError("[Seed Error] Cities must be seeded before users.")

        needed = num_users - existing_user_count
        print(f"[Seed] Generating {needed} rider accounts...")

        # Batch insert for memory efficiency
        batch_size = 500
        for i in range(0, needed, batch_size):
            batch = []
            for _ in range(min(batch_size, needed - i)):
                u = User(
                    first_name=fake.first_name(),
                    last_name=fake.last_name(),
                    email=fake.unique.email(),
                    phone=fake.unique.phone_number()[:30],
                    home_city_id=random.choice(city_ids)
                )
                batch.append(u)
            session.bulk_save_objects(batch)
            session.commit()

        print(f"[Seed] Successfully seeded {needed} riders.")


def seed_drivers_and_vehicles(num_drivers=DEFAULT_DRIVERS_COUNT):
    """
    Seeds driver user accounts, assigns fleet vehicles, and creates driver profiles.
    """
    with get_db_session() as session:
        existing_driver_count = session.query(Driver).count()
        if existing_driver_count >= num_drivers:
            print(f"[Seed] Drivers already seeded ({existing_driver_count} drivers found).")
            return

        city_ids = [c.city_id for c in session.query(City.city_id).all()]
        needed = num_drivers - existing_driver_count
        print(f"[Seed] Generating {needed} driver accounts and vehicles...")

        statuses = ["available", "available", "available", "offline"]

        for _ in range(needed):
            # 1. Create a user account for the driver
            driver_user = User(
                first_name=fake.first_name(),
                last_name=fake.last_name(),
                email=fake.unique.email(),
                phone=fake.unique.phone_number()[:30],
                home_city_id=random.choice(city_ids)
            )
            session.add(driver_user)
            session.flush()

            # 2. Create vehicle asset
            make, model = random.choice(VEHICLE_MODELS)
            vehicle = Vehicle(
                make=make,
                model=model,
                year=random.randint(2018, 2024),
                plate_number=fake.unique.license_plate()[:20],
                vehicle_type=random.choice(VEHICLE_TYPES)
            )
            session.add(vehicle)
            session.flush()

            # 3. Create driver profile linking user and vehicle
            driver = Driver(
                user_id=driver_user.user_id,
                license_number=fake.unique.bothify(text="??######"),
                vehicle_id=vehicle.vehicle_id,
                status=random.choice(statuses),
                rating_avg=round(random.uniform(4.20, 5.00), 2)
            )
            session.add(driver)
            session.flush()

            # 4. Link vehicle back to driver
            vehicle.driver_id = driver.driver_id

        print(f"[Seed] Successfully seeded {needed} drivers and vehicles.")


def run_all_seeds():
    """
    Master function to seed cities, users, drivers, and vehicles in order.
    """
    print("=" * 60)
    print("Starting Baseline Database Seeding...")
    print("=" * 60)
    seed_cities()
    seed_users()
    seed_drivers_and_vehicles()
    print("=" * 60)
    print("Baseline Seeding Complete!")
    print("=" * 60)


if __name__ == "__main__":
    run_all_seeds()

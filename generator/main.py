"""
=============================================================================
Uber Lakehouse Platform - Generator CLI Entrypoint
=============================================================================
Description: Command Line Interface (CLI) for seeding initial database data
             and executing the real-time operational ride simulator.
=============================================================================
"""

import argparse
import asyncio
import sys
from generator.seed import run_all_seeds
from generator.simulator import run_simulation_loop


def main():
    parser = argparse.ArgumentParser(
        description="Uber Lakehouse Platform - Real-time OLTP Traffic & CDC Event Generator"
    )
    parser.add_argument(
        "--seed",
        action="store_true",
        help="Execute baseline database seeding (Cities, Users, Drivers, Vehicles)"
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Launch the real-time continuous trip simulator and GPS telemetry loop"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Optional simulation runtime limit in seconds (e.g., --duration 60)"
    )

    args = parser.parse_args()

    # If no flags are explicitly provided, default to seeding then running simulation
    if not args.seed and not args.run:
        print("[CLI] No specific flags provided. Executing full pipeline (Seeding + Simulation)...")
        run_all_seeds()
        asyncio.run(run_simulation_loop(duration_seconds=args.duration))
        return

    if args.seed:
        run_all_seeds()

    if args.run:
        asyncio.run(run_simulation_loop(duration_seconds=args.duration))


if __name__ == "__main__":
    main()

"""
=============================================================================
Uber Lakehouse Platform - Traffic & Event Simulator Entrypoint
=============================================================================
Description: Main entrypoint for launching the ride-hailing OLTP simulator.
             Executes seeding and continuous event stream generation.
=============================================================================
"""

import sys
from generator.main import main

if __name__ == "__main__":
    main()

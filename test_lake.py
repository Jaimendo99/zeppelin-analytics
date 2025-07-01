#!/usr/bin/env python3
"""
Test script for data lake loading.

This script simulates the lake loading process and refreshing mechanism without starting the
full FastAPI server. It's useful for debugging and testing the data lake loading process.
"""

import asyncio
import time
from datetime import datetime, timedelta
import pandas as pd
from dataframeloader import load_lake, lake
from apiClient import APIClient
from db import get_database
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# How long to run the test (in seconds)
TEST_DURATION = 180  # 3 minutes
# How often to refresh the lake (in seconds)
REFRESH_INTERVAL = 60  # 1 minute for testing (faster than production)


async def test_lake_loading():
    """Run a test of the lake loading process."""
    print(f"[{datetime.now().isoformat()}] Starting lake loading test...")

    # Initialize connections
    api_user = os.getenv("API_USER")
    api_password = os.getenv("API_PASSWORD")

    if not api_user or not api_password:
        print("Error: API_USER or API_PASSWORD environment variables not set")
        return

    api_client = APIClient(api_user, api_password)
    mongo_client = get_database()

    if not mongo_client:
        print("Error: Could not connect to MongoDB")
        return

    # Initial lake load
    print(f"[{datetime.now().isoformat()}] Performing initial lake load...")
    success = await load_lake(api=api_client, db=mongo_client)

    if not success:
        print("Error: Failed to load lake")
        return

    print(f"[{datetime.now().isoformat()}] Initial lake load complete with {len(lake)} rows")

    # Run periodic refreshes
    start_time = time.time()
    last_refresh_time = start_time

    while time.time() - start_time < TEST_DURATION:
        current_time = time.time()

        # Check if it's time to refresh
        if current_time - last_refresh_time >= REFRESH_INTERVAL:
            print(f"[{datetime.now().isoformat()}] Refreshing data lake...")
            await load_lake(api=api_client, db=mongo_client)
            last_refresh_time = current_time
            print(f"[{datetime.now().isoformat()}] Lake refresh complete with {len(lake)} rows")

        # Wait a bit before checking again
        await asyncio.sleep(5)

        # Print status
        elapsed = time.time() - start_time
        remaining = TEST_DURATION - elapsed
        next_refresh = REFRESH_INTERVAL - (time.time() - last_refresh_time)

        print(f"Test running for {elapsed:.1f}s (remaining: {remaining:.1f}s), "
              f"next refresh in {next_refresh:.1f}s")

    print(f"[{datetime.now().isoformat()}] Test complete after {TEST_DURATION} seconds")


if __name__ == "__main__":
    asyncio.run(test_lake_loading())

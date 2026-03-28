import requests
import os
import time
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent / ".env")
API_KEY = os.getenv("API_KEY")

# 30 Indian cities for larger dataset
CITIES = [
    "Bengaluru", "Mumbai", "Delhi", "Chennai", "Hyderabad",
    "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Surat",
    "Lucknow", "Kanpur", "Nagpur", "Indore", "Thane",
    "Bhopal", "Visakhapatnam", "Patna", "Vadodara", "Ghaziabad",
    "Ludhiana", "Agra", "Nashik", "Meerut", "Rajkot",
    "Varanasi", "Srinagar", "Aurangabad", "Dhanbad", "Coimbatore"
]

BATCH_SIZE = 10  # process 10 cities at a time

def extract_batch(batch):
    """Extract weather data for a single batch of cities"""
    batch_data = []
    for city in batch:
        try:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                batch_data.append(response.json())
                print(f"  ✅ Extracted: {city}")
            else:
                print(f"  ❌ Failed: {city} (Status: {response.status_code})")
        except requests.exceptions.Timeout:
            print(f"  ⏱️ Timeout: {city} - skipping")
        except requests.exceptions.ConnectionError:
            print(f"  🔌 Connection error: {city} - skipping")
    return batch_data

def extract_weather_data():
    """Extract weather data in batches"""
    all_data = []

    # Split cities into batches
    batches = [CITIES[i:i+BATCH_SIZE] for i in range(0, len(CITIES), BATCH_SIZE)]
    total_batches = len(batches)

    for idx, batch in enumerate(batches):
        print(f"\n📦 Processing Batch {idx+1}/{total_batches}: {batch}")
        batch_data = extract_batch(batch)
        all_data.extend(batch_data)

        # Wait between batches to avoid API rate limiting
        if idx < total_batches - 1:
            print(f"  ⏳ Waiting 2 seconds before next batch...")
            time.sleep(2)

    print(f"\n📊 Total records extracted: {len(all_data)}")
    return all_data
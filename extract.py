import requests
import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

API_KEY = os.getenv("API_KEY")

CITIES = ["Kurnool", "Anantapuram", "Kochi", "Chennai", "Hyderabad"]

def extract_weather_data():
    all_data = []
    for city in CITIES:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url)
        if response.status_code == 200:
            all_data.append(response.json())
            print(f"Extracted data for {city}")
        else:
            print(f"Failed for {city}: {response.status_code}")
    return all_data



from dotenv import load_dotenv
from pathlib import Path
import os
import requests

# Load .env explicitly
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# Check if key is being read
api_key = os.getenv("API_KEY")
print(f"API Key found: {api_key}")

# Test API call
url = f"http://api.openweathermap.org/data/2.5/weather?q=Bengaluru&appid={api_key}&units=metric"
response = requests.get(url)
print(f"Status Code: {response.status_code}")
print(f"Response: {response.json()}")
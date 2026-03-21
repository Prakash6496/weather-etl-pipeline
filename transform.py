import pandas as pd
from datetime import datetime

def transform_weather_data(raw_data):
    transformed = []
    for entry in raw_data:
        transformed.append({
            "city":        entry["name"],
            "country":     entry["sys"]["country"],
            "temperature": entry["main"]["temp"],
            "feels_like":  entry["main"]["feels_like"],
            "humidity":    entry["main"]["humidity"],
            "pressure":    entry["main"]["pressure"],
            "weather":     entry["weather"][0]["description"],
            "wind_speed":  entry["wind"]["speed"],
            "extracted_at": datetime.now()
        })

    df = pd.DataFrame(transformed)
    print(f"Transformed {len(df)} records")
    return df
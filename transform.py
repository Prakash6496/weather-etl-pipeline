import pandas as pd
from datetime import datetime

def transform_weather_data(raw_data):
    """Transform and clean raw weather data"""

    if not raw_data:
        print("⚠️ No data to transform")
        return pd.DataFrame()

    # --- EXTRACT fields from raw JSON ---
    transformed = []
    for entry in raw_data:
        try:
            transformed.append({
                "city":         entry.get("name"),
                "country":      entry.get("sys", {}).get("country"),
                "temperature":  entry.get("main", {}).get("temp"),
                "feels_like":   entry.get("main", {}).get("feels_like"),
                "humidity":     entry.get("main", {}).get("humidity"),
                "pressure":     entry.get("main", {}).get("pressure"),
                "weather":      entry.get("weather", [{}])[0].get("description"),
                "wind_speed":   entry.get("wind", {}).get("speed"),
                "extracted_at": datetime.now()
            })
        except Exception as e:
            print(f"  ⚠️ Skipping malformed record: {e}")

    df = pd.DataFrame(transformed)
    print(f"\n🔍 Raw records before cleaning: {len(df)}")

    # --- STEP 1: Handle NULL values ---
    null_counts = df.isnull().sum()
    if null_counts.any():
        print(f"  🔧 Nulls found:\n{null_counts[null_counts > 0]}")
    df["temperature"] = df["temperature"].fillna(df["temperature"].mean())
    df["humidity"] = df["humidity"].fillna(df["humidity"].median())
    df["wind_speed"] = df["wind_speed"].fillna(0.0)
    df["weather"] = df["weather"].fillna("unknown")
    df.dropna(subset=["city", "country"], inplace=True)  # drop if city/country missing
    print(f"  ✅ Null handling done")

    # --- STEP 2: Remove duplicates ---
    before = len(df)
    df.drop_duplicates(subset=["city", "country"], keep="first", inplace=True)
    after = len(df)
    print(f"  ✅ Duplicates removed: {before - after} rows dropped")

    # --- STEP 3: Fix data types ---
    df["temperature"] = df["temperature"].astype(float).round(2)
    df["feels_like"]  = df["feels_like"].astype(float).round(2)
    df["humidity"]    = df["humidity"].astype(int)
    df["pressure"]    = df["pressure"].astype(int)
    df["wind_speed"]  = df["wind_speed"].astype(float).round(2)
    df["city"]        = df["city"].str.strip().str.title()
    df["country"]     = df["country"].str.strip().str.upper()
    print(f"  ✅ Data types fixed")

    # --- STEP 4: Remove invalid temperature readings ---
    before = len(df)
    df = df[(df["temperature"] >= -50) & (df["temperature"] <= 60)]
    print(f"  ✅ Invalid temperatures removed: {before - len(df)} rows dropped")

    print(f"\n✅ Final clean records: {len(df)}")
    return df
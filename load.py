import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent / ".env")

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def create_table():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id            SERIAL PRIMARY KEY,
            city          VARCHAR(100),
            country       VARCHAR(10),
            temperature   FLOAT,
            feels_like    FLOAT,
            humidity      INT,
            pressure      INT,
            weather       VARCHAR(200),
            wind_speed    FLOAT,
            extracted_at  TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Table ready")

def load_data(df):
    if df.empty:
        print("⚠️ No data to load")
        return

    conn = get_connection()
    cur = conn.cursor()

    loaded = 0
    skipped = 0

    for _, row in df.iterrows():
        # Check for duplicate in same day before inserting
        cur.execute("""
            SELECT id FROM weather_data
            WHERE city = %s AND DATE(extracted_at) = CURRENT_DATE
        """, (row["city"],))

        if cur.fetchone():
            skipped += 1  # already loaded today, skip
        else:
            cur.execute("""
                INSERT INTO weather_data
                (city, country, temperature, feels_like, humidity, pressure, weather, wind_speed, extracted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row["city"], row["country"], row["temperature"],
                row["feels_like"], row["humidity"], row["pressure"],
                row["weather"], row["wind_speed"], row["extracted_at"]
            ))
            loaded += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Loaded: {loaded} records | Skipped duplicates: {skipped}")
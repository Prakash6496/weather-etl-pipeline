import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

load_dotenv()

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
    conn = get_connection()
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO weather_data
            (city, country, temperature, feels_like, humidity, pressure, weather, wind_speed, extracted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row["city"], row["country"], row["temperature"],
            row["feels_like"], row["humidity"], row["pressure"],
            row["weather"], row["wind_speed"], row["extracted_at"]
        ))
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Loaded {len(df)} records into PostgreSQL")
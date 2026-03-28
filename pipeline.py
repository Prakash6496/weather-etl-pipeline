from extract import extract_weather_data
from transform import transform_weather_data
from load import create_table, load_data
from datetime import datetime

def run_pipeline():
    print(f"\n🚀 Pipeline started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    raw_data = extract_weather_data()
    df = transform_weather_data(raw_data)
    create_table()
    load_data(df)

    print("=" * 50)
    print(f"🎉 Pipeline completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

if __name__ == "__main__":
    run_pipeline()
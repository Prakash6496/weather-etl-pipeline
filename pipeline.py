from extract import extract_weather_data
from transform import transform_weather_data
from load import create_table, load_data

print("🚀 Starting ETL Pipeline...")
raw_data = extract_weather_data()
df = transform_weather_data(raw_data)
create_table()
load_data(df)
print("🎉 Pipeline completed successfully!")


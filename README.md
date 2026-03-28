# 🌤️ Weather ETL Pipeline

An end-to-end ETL data pipeline that extracts live weather data, 
transforms and cleans it, and loads it into a PostgreSQL database.

## 🏗️ Architecture
```
OpenWeatherMap API → Extract → Transform → Load → PostgreSQL
```

## ⚙️ Tech Stack
- **Language:** Python
- **Libraries:** Pandas, Requests, psycopg2, APScheduler
- **Database:** PostgreSQL
- **Tools:** Git, VS Code

## 🚀 Features
- Extracts live weather data for 30 Indian cities
- Batch processing (3 batches of 10 cities each)
- Data cleaning — handles nulls, duplicates, type errors
- Duplicate prevention before loading into database
- Automated daily scheduling at 8:00 AM using APScheduler

## 📁 Project Structure
```
weather_etl_pipeline/
├── extract.py       # Fetches data from OpenWeatherMap API
├── transform.py     # Cleans and transforms raw data
├── load.py          # Loads clean data into PostgreSQL
├── pipeline.py      # Orchestrates the full ETL flow
├── scheduler.py     # Automates daily pipeline execution
└── requirements.txt # Project dependencies
```

## ▶️ How to Run
1. Clone the repo
2. Install dependencies: `pip install -r requirements.txt`
3. Create `.env` file with your API key and DB credentials
4. Run pipeline: `python pipeline.py`
5. Schedule automation: `python scheduler.py`

## 📊 Data Collected
| Field | Description |
|---|---|
| city | City name |
| temperature | Current temperature (°C) |
| humidity | Humidity percentage |
| weather | Weather description |
| wind_speed | Wind speed (m/s) |
| extracted_at | Timestamp of extraction |
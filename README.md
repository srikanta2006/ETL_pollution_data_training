🌫️ Air Quality ETL Pipeline — Tekworks Logistics

A complete Extract → Transform → Load → Analyze pipeline that fetches multi-city air-quality data from APIs, cleans & enriches it, loads it into Supabase, and generates a full analytics report + visualizations.

🚀 Project Overview

This pipeline automates the full lifecycle of air-quality data:

1️⃣ Extract raw environmental data from two APIs (primary + secondary fallback).
2️⃣ Transform and clean raw JSON into structured tabular datasets.
3️⃣ Load enriched data into a Supabase PostgreSQL table.
4️⃣ Analyze the dataset to produce metrics, KPIs, and visual reports.

🛠️ Tech Stack
Layer	Technology
Language	Python 3.12
Database	Supabase (PostgreSQL)
Visualizations	Matplotlib + Pandas
Orchestration	Python Scripts
Storage	Local CSV Staging
📂 Project Structure
ETL_PIPE_LINE_LOGISTICS/
│
├── extract.py
├── transform.py
├── load.py
├── etl_analysis.py
├── run_pipeline.py
│
├── data/
│   ├── raw/
│   ├── staged/
│   └── processed/
│
├── .env        # API Keys + Supabase Keys (ignored)
├── .gitignore
└── README.md

🔌 1. Setup
Install Dependencies
pip install -r requirements.txt

Environment Variables

Create a .env file:

API_KEY_PRIMARY=your_key
API_KEY_SECONDARY=your_key
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_service_role_key

🌐 2. Extract Layer

The extract module:

✔ fetches AQI data for multiple cities
✔ retries with a second API if primary fails
✔ saves raw JSON into /data/raw/

Run extract only:

python extract.py

🔄 3. Transform Layer

The transform module:

✔ reads all raw JSON files
✔ normalizes fields (pm2_5, ozone, uv_index, severity_score, etc.)
✔ generates risk labels
✔ outputs clean CSV → /data/staged/air_quality_transformed.csv

Run transform only:

python transform.py

🛢️ 4. Load Layer (Supabase)

The load module:

✔ reads staged CSV
✔ inserts rows into Supabase table air_quality_data
✔ ensures type safety
✔ avoids duplicate writes

Run load:

python load.py

📊 5. Analysis Layer

The analysis module:

✔ fetches data back from Supabase
✔ generates KPI metrics
✔ saves multiple reports:

summary_metrics.csv
pollution_trends.csv
city_risk_distribution.csv
pm2_5_histogram.png
risk_per_city.png
pm2_5_hourly_trend.png
severity_vs_pm2_5.png


Run analytics:

python etl_analysis.py

🔁 6. Full Pipeline Runner

Run the entire ETL workflow in one command:

python run_pipeline.py


This triggers:

1️⃣ Extract
2️⃣ Transform
3️⃣ Load
4️⃣ Analysis

and prints a full execution log.

📈 Generated Insights

The system produces:

Highest pollution city

Hourly worst pollution trends

Risk category distribution

Severity correlation

Histograms + Barplots + Lineplots

All exported inside:

data/processed/

🧩 Future Enhancements

Schedule pipeline with Airflow / Cron

Add predictive modelling (LSTM AQI prediction)

Add dashboard (Streamlit / React)

👨‍💻 Author

Srikanta Bellamkonda
B.Tech Student | Developer | Innovator
Hyderabad, Telangana 🇮🇳

LinkedIn: https://www.linkedin.com/in/srikanta-bellamkonda/

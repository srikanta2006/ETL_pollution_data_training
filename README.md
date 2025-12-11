🌆 URBAN AIR QUALITY MONITORING ETL PIPELINE

An end-to-end Python ETL pipeline to extract, transform, load, and analyze urban air quality data using OpenAQ API and Supabase.

📌 TABLE OF CONTENTS

Introduction

Features

Project Structure

Configuration

Installation

Usage

Supabase Table Structure

ETL Workflow

Reports & Outputs

Troubleshooting

License

🏁 INTRODUCTION

This project implements an Urban Air Quality Monitoring ETL Pipeline:

Extract: Collects live air quality data from OpenAQ API.

Transform: Cleans and flattens the data, calculates AQI categories, severity scores, and risk levels.

Load: Inserts processed data into a Supabase PostgreSQL database.

Analyze: Generates KPIs, trends, risk distribution, and visualizations.

Ideal for data engineers, environmental analysts, and smart city projects.

✨ FEATURES

✅ Automated data extraction from multiple cities

✅ Data transformation with derived features: AQI category, severity score, risk classification

✅ Integration with Supabase for scalable storage

✅ Generates summary metrics, trends, and visualizations

✅ Modular design: can run extract, transform, load, analysis independently

📁 PROJECT STRUCTURE
project-root/
│
├── extract.py          # Extracts AQI JSON from OpenAQ
├── transform.py        # Cleans & transforms raw data
├── load.py             # Loads data into Supabase
├── etl_analysis.py     # Analyzes data & generates reports
├── run_pipeline.py     # Runs full ETL pipeline
│
├── data/
│   ├── raw/            # Raw JSON files
│   ├── staged/         # Transformed CSV files
│   └── processed/      # Analysis outputs & plots
│
├── requirements.txt    # Python dependencies
└── README.md           # Project documentation

⚙️ CONFIGURATION
1. CLONE THE REPOSITORY
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>

2. CREATE & ACTIVATE VIRTUAL ENVIRONMENT
python -m venv venv


Windows

venv\Scripts\activate


macOS / Linux

source venv/bin/activate

3. INSTALL DEPENDENCIES
pip install -r requirements.txt

4. SETUP ENVIRONMENT VARIABLES

Create a .env file in the root:

# API KEYS
OPENAQ_API_BASE=https://api.openaq.org/v2/latest
AQ_CITIES=Delhi,Bengaluru,Hyderabad,Mumbai,Kolkata

# SUPABASE CONFIG
SUPABASE_URL=https://your-instance.supabase.co
SUPABASE_KEY=your_service_role_key


⚠️ Do not commit .env file. Keep keys secret.

🏗️ SUPABASE TABLE STRUCTURE (REQUIRED)

Table Name: air_quality_data

Column	Type
city	TEXT
time	TIMESTAMPTZ
pm10	DOUBLE PRECISION
pm2_5	DOUBLE PRECISION
carbon_monoxide	DOUBLE PRECISION
nitrogen_dioxide	DOUBLE PRECISION
sulphur_dioxide	DOUBLE PRECISION
ozone	DOUBLE PRECISION
uv_index	DOUBLE PRECISION
severity_score	DOUBLE PRECISION
risk_flag	TEXT
latitude	DOUBLE PRECISION
longitude	DOUBLE PRECISION
🛠️ ETL WORKFLOW

Extract → Fetches JSON data from OpenAQ for multiple cities.

Transform → Cleans data, calculates AQI, severity, and risk flags.

Load → Inserts the transformed data into Supabase table.

Analysis → Computes metrics, generates CSV reports & plots.

🚀 USAGE
Run Full ETL Pipeline:
python run_pipeline.py

Run Modules Individually:

Extract: python extract.py

Transform: python transform.py

Load: python load.py

Analysis: python etl_analysis.py

📊 REPORTS & OUTPUTS

Generated reports and plots are saved in data/processed/:

summary_metrics.csv → Key KPIs

pollution_trends.csv → City-wise pollution trends

city_risk_distribution.csv → Risk distribution per city

pm2_5_histogram.png → PM2.5 distribution

risk_per_city.png → Risk levels per city

pm2_5_hourly_trend.png → Hourly PM2.5 trends

severity_vs_pm2_5.png → Severity vs PM2.5 scatter plot

⚠️ TROUBLESHOOTING

No numeric data to plot: Ensure risk_flag and severity_score columns exist in Supabase.

Supabase connection issues: Verify .env keys.

Missing raw files: Check data/raw/ directory and API connectivity.

📄 LICENSE

MIT License © 2025

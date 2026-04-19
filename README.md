Data Engineering Portfolio
GCP | BigQuery | Python | dbt | ETL Pipelines

A collection of end-to-end data engineering pipelines built on Google Cloud Platform, demonstrating real-world ETL patterns using Python, BigQuery, Cloud Storage, and dbt.

---

Projects

---

1. Automobile Data Pipeline
automobile_etl.ipynb

An ETL pipeline that ingests, cleans, and transforms a real-world automobile dataset into an analytical summary table in BigQuery.

Pipeline flow:
Kaggle CSV → Google Cloud Storage → BigQuery (raw) → Python ETL → BigQuery (transformed)

What it does:
- Extracts 205 rows of automobile data from BigQuery
- Handles missing values (? placeholders replaced with nulls)
- Casts string columns to correct numeric types
- Engineers two new features: price_tier (Budget / Mid-Range / Premium) and efficiency_rating (Low / Medium / High)
- Loads the transformed dataset back to BigQuery as automobile_summary

Tech: Python, pandas, google-cloud-bigquery, BigQuery, GCS

---

2. Cairo Weather Pipeline
weather_pipeline.ipynb

A live API pipeline that pulls real-time hourly weather data for Cairo, Egypt, enriches it with derived features, and loads it into BigQuery for analysis.

Pipeline flow:
Open-Meteo API (live) → Python → Transform → BigQuery

What it does:
- Calls the Open-Meteo REST API to fetch 168 hours of Cairo weather forecasts
- Parses JSON response into a structured pandas DataFrame
- Engineers features: comfort_level (Cold / Comfortable / Hot) and time_of_day (Night / Morning / Afternoon / Evening)
- Tracks ingested_at timestamp for pipeline auditability
- Structured as modular extract(), transform(), load() functions — ready for Airflow orchestration

Tech: Python, requests, pandas, google-cloud-bigquery, Open-Meteo API

---

3. Automobile dbt Pipeline
automobile_dbt/

A dbt project that sits on top of the raw BigQuery automobile data and transforms it through a layered architecture — staging, marts, and aggregated summaries.

Pipeline flow:
BigQuery (raw) → dbt staging → dbt marts → BigQuery (transformed)

What it does:
- Staging layer cleans and casts the raw data using SAFE_CAST and NULLIF
- Mart layer adds business logic: price tiers, efficiency ratings, and performance-per-dollar scores
- Aggregated mart summarizes data by manufacturer with avg price, horsepower, and mpg
- Schema tests validate data quality on every key column
- Auto-generated documentation with full lineage graph

Tech: dbt, BigQuery, SQL

---

Stack

| Layer           | Tool                                          |
|-----------------|-----------------------------------------------|
| Cloud Platform  | Google Cloud Platform (GCP)                   |
| Data Warehouse  | BigQuery                                      |
| Transformations | dbt                                           |
| Object Storage  | Google Cloud Storage                          |
| Language        | Python 3                                      |
| Libraries       | pandas, google-cloud-bigquery, requests       |
| Environment     | Jupyter Notebook (Anaconda)                   |
| Auth            | Application Default Credentials (gcloud ADC) |

---

Project Structure

de-portfolio/
├── Automobile_ETL.ipynb               # Automobile dataset ETL pipeline
├── Weather_Pipeline.ipynb             # Live Cairo weather API pipeline
├── automobile_dbt/                    # dbt transformation project
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_automobile.sql
│   │   └── marts/
│   │       ├── mart_automobile_summary.sql
│   │       └── mart_make_summary.sql
│   ├── models/schema.yml
│   ├── models/sources.yml
│   └── dbt_project.yml
├── airflow-pipeline/                  # Airflow orchestration
│   ├── dags/
│   │   ├── automobile_pipeline.py
│   │   └── weather_pipeline.py
│   └── docker-compose.yaml
└── README.md
---

Setup and Running Locally

Prerequisites:
- Python 3.8+
- Google Cloud SDK installed
- A GCP project with BigQuery API enabled
- dbt-bigquery installed

Installation:

    git clone https://github.com/yossherif/de-portfolio.git
    cd de-portfolio
    pip install google-cloud-bigquery pandas requests pyarrow db-dtypes pandas-gbq dbt-bigquery

Authentication:

    gcloud auth application-default login

Configuration:
Update the PROJECT variable in each notebook to point to your GCP project:

    PROJECT = "your-gcp-project-id"

Running the Python pipelines:
Open either notebook in Jupyter and run all cells.

    jupyter notebook

Running the dbt pipeline:

    cd automobile_dbt
    dbt run
    dbt test

---

Key Concepts Demonstrated

- ETL Pipeline Design — modular extract, transform, load functions
- Cloud Data Warehousing — loading and querying data in BigQuery
- Data Modeling — layered dbt architecture (staging → marts)
- Data Quality — schema tests on every key column using dbt test
- Data Cleaning — handling missing values and type casting in real datasets
- Feature Engineering — deriving business-logic columns from raw data
- API Ingestion — pulling live JSON data from a REST API
- Pipeline Auditability — tracking ingestion timestamps

---

LinkedIn: www.linkedin.com/in/youssef-sherif-09497728b
GitHub: https://github.com/yossherif

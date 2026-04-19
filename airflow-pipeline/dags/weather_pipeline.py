from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def failure_alert(context):
    message = Mail(
        from_email='yossherif@gmail.com',
        to_emails='yossherif@gmail.com',
        subject=f"DAG Failed: {context['dag'].dag_id}",
        html_content=f"""
            <h3>DAG Failed</h3>
            <p><b>DAG:</b> {context['dag'].dag_id}</p>
            <p><b>Task:</b> {context['task_instance'].task_id}</p>
            <p><b>Time:</b> {context['execution_date']}</p>
        """
    )
    sg = SendGridAPIClient(Variable.get("sendgrid_api_key"))
    sg.send(message)
    print("Alert email sent!")
    
    
    
def extract_weather(**context):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 30.06,
        "longitude": 31.25,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "Africa/Cairo"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    hourly = data["hourly"]
    df = pd.DataFrame({
        "timestamp": pd.to_datetime(hourly["time"]),
        "temperature_c": hourly["temperature_2m"],
        "humidity_pct": hourly["relative_humidity_2m"],
        "wind_speed_kmh": hourly["wind_speed_10m"]
    })

    df.to_csv("/tmp/weather_raw.csv", index=False)
    print(f"Extracted {len(df)} rows")


def transform_weather(**context):
    df = pd.read_csv("/tmp/weather_raw.csv")
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df["city"] = "Cairo"
    df["country"] = "Egypt"
    df["comfort_level"] = df["temperature_c"].apply(
        lambda x: "Cold" if x < 15
        else "Comfortable" if x < 28
        else "Hot"
    )
    df["time_of_day"] = df["timestamp"].dt.hour.apply(
        lambda x: "Night" if x < 6
        else "Morning" if x < 12
        else "Afternoon" if x < 18
        else "Evening"
    )
    df["ingested_at"] = datetime.now().isoformat()

    df.to_csv("/tmp/weather_transformed.csv", index=False)
    print(f"Transformed {len(df)} rows")


def load_weather(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project="expanded-flame-491820-j2")

    df = pd.read_csv("/tmp/weather_transformed.csv")

    table_id = "expanded-flame-491820-j2.automobile.cairo_weather_orchestrated"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_id}")


default_args = {
    "owner": "youssef",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["yossherif@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False
}

with DAG(
    dag_id="cairo_weather_pipeline",
    default_args=default_args,
    description="Daily Cairo weather pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",   # runs every hour — fresh weather data
    catchup=False,
    tags=["weather", "cairo", "api", "bigquery"]
) as dag:

    task_extract = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather
    )

    task_transform = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather
    )

    task_load = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather
    )

    task_extract >> task_transform >> task_load
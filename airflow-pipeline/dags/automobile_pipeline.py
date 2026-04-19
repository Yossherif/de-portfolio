from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def failure_alert(context):
    api_key = Variable.get("sendgrid_api_key")  
    
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
    
    sg = SendGridAPIClient(api_key)
    sg.send(message)
    print("Alert email sent!")

def extract(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project="expanded-flame-491820-j2")
    query = """
        SELECT *
        FROM `expanded-flame-491820-j2.automobile.automobile`
    """
    df = client.query(query).to_dataframe()
    print(f"Extracted {len(df)} rows")
    df.to_csv("/tmp/automobile_raw.csv", index=False)

def transform(**context):
    df = pd.read_csv("/tmp/automobile_raw.csv")
    df = df.replace("?", None)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["horsepower"] = pd.to_numeric(df["horsepower"], errors="coerce")
    df["city-mpg"] = pd.to_numeric(df["city-mpg"], errors="coerce")
    df["price_tier"] = df["price"].apply(
        lambda x: "Unknown" if pd.isna(x)
        else "Budget" if x < 10000
        else "Mid-Range" if x < 20000
        else "Premium"
    )
    df["efficiency_rating"] = df["city-mpg"].apply(
        lambda x: "Unknown" if pd.isna(x)
        else "High" if x > 30
        else "Medium" if x > 20
        else "Low"
    )
    df["ingested_at"] = datetime.now().isoformat()
    df.to_csv("/tmp/automobile_transformed.csv", index=False)
    print(f"Transformed {len(df)} rows")

def load(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project="expanded-flame-491820-j2")
    df = pd.read_csv("/tmp/automobile_transformed.csv")
    table_id = "expanded-flame-491820-j2.automobile.automobile_orchestrated"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_id}")

def validate(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project="expanded-flame-491820-j2")
    query = """
        SELECT
            COUNT(*) as total_rows,
            COUNTIF(price IS NULL) as null_prices,
            COUNTIF(price_tier IS NULL) as null_tiers
        FROM `expanded-flame-491820-j2.automobile.automobile_orchestrated`
    """
    df = client.query(query).to_dataframe()
    row = df.iloc[0]
    print(f"Total rows: {row['total_rows']}")
    print(f"Null prices: {row['null_prices']}")
    print(f"Null tiers: {row['null_tiers']}")
    if row["total_rows"] < 100:
        raise ValueError(f"Too few rows loaded: {row['total_rows']}")
    print("Data quality check passed!")


default_args = {
    "owner": "youssef",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_alert
}

with DAG(
    dag_id="automobile_pipeline",
    default_args=default_args,
    description="ETL pipeline for automobile dataset",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["automobile", "bigquery", "etl"]
) as dag:

    task_extract = PythonOperator(task_id="extract", python_callable=extract)
    task_transform = PythonOperator(task_id="transform", python_callable=transform)
    task_load = PythonOperator(task_id="load", python_callable=load)
    task_validate = PythonOperator(task_id="validate", python_callable=validate)

    task_extract >> task_transform >> task_load >> task_validate
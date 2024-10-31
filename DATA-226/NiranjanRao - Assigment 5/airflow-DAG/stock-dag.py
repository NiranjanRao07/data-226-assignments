from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta, datetime
import snowflake.connector
import requests


# Snowflake connection function
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


# Task to extract the last 90 days of stock prices from Alpha Vantage API
@task
def extract_stock_data():
    symbol = "LMT"
    api_key = Variable.get(
        "vantage_api_key"
    )  # Assuming API key is stored as an Airflow variable
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    r = requests.get(url)
    data = r.json()
    return data  # Return raw data from the API


# Task to transform the extracted data into a format ready for loading into Snowflake
@task
def transform_stock_data(data):
    symbol = "LMT"
    results = (
        []
    )  # Empty list to hold the transformed data (open, high, low, close, volume)

    # Extract the last 90 days of stock prices
    for date in list(data["Time Series (Daily)"].keys())[
        :90
    ]:  # Loop through the last 90 days
        daily_data = data["Time Series (Daily)"][date]

        # Create a record with the relevant fields
        record = {
            "date": date,
            "open": float(daily_data["1. open"]),
            "high": float(daily_data["2. high"]),
            "low": float(daily_data["3. low"]),
            "close": float(daily_data["4. close"]),
            "volume": int(daily_data["5. volume"]),
            "symbol": symbol,
        }
        results.append(record)  # Append the transformed record to the list

    return results  # Return the transformed data


# Task to load the transformed data into Snowflake
@task
def load_to_snowflake(data):
    cur = return_snowflake_conn()

    # Create database, schema, and table if they don't exist
    cur.execute("CREATE DATABASE IF NOT EXISTS stock_data_db;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS stock_data_db.raw_data;")
    cur.execute(
        """
        CREATE OR REPLACE TABLE stock_data_db.raw_data.stock_prices (
            date DATE NOT NULL,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INTEGER,
            symbol STRING,
            PRIMARY KEY (date, symbol)
        );
    """
    )

    # Insert the transformed data into Snowflake
    insert_query = """
        INSERT INTO stock_data_db.raw_data.stock_prices (date, open, high, low, close, volume, symbol)
        VALUES (%(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(symbol)s)
    """

    for record in data:
        cur.execute(insert_query, record)  # Insert each record into the Snowflake table

    cur.close()


# Defining the DAG
with DAG(
    dag_id="Stock_DAG",
    start_date=datetime(2024, 9, 21),
    schedule_interval="30 2 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ETL"],
) as dag:

    # Define the workflow
    raw_data = extract_stock_data()
    transformed_data = transform_stock_data(raw_data)
    load_to_snowflake(transformed_data)

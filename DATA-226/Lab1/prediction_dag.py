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
    symbol = "MCD"
    api_key = Variable.get("vantage_api_key")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    r = requests.get(url)
    data = r.json()
    return data


# Task to transform the extracted data into a format ready for loading into Snowflake
@task
def transform_stock_data(data):
    symbol = "MCD"
    results = []
    for date in list(data["Time Series (Daily)"].keys())[:90]:
        daily_data = data["Time Series (Daily)"][date]
        record = {
            "date": date,
            "open": float(daily_data["1. open"]),
            "high": float(daily_data["2. high"]),
            "low": float(daily_data["3. low"]),
            "close": float(daily_data["4. close"]),
            "volume": int(daily_data["5. volume"]),
            "symbol": symbol,
        }
        results.append(record)
    return results


# Task to load the transformed data into Snowflake
@task
def load_to_snowflake(data):
    cur = return_snowflake_conn()
    cur.execute("CREATE DATABASE IF NOT EXISTS stock_data_db;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS stock_data_db.raw_data;")
    cur.execute(
        """
        CREATE OR REPLACE TABLE stock_data_db.raw_data.stock_prices (
            date TIMESTAMP_NTZ NOT NULL,  -- Changed to TIMESTAMP_NTZ
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
    insert_query = """
        INSERT INTO stock_data_db.raw_data.stock_prices (date, open, high, low, close, volume, symbol)
        VALUES (%(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(symbol)s)
    """
    for record in data:
        cur.execute(insert_query, record)
    cur.close()


# Task to train the predictive model
@task
def train(cur, train_input_table, train_view, forecast_function_name):
    cur.execute("CREATE SCHEMA IF NOT EXISTS stock_data_db.adhoc;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS stock_data_db.analytics;")

    # Create a view with training related columns
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE::TIMESTAMP_NTZ AS DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""  # Cast date to TIMESTAMP_NTZ

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise


# Task to generate predictions
@task
def predict(
    cur, forecast_function_name, train_input_table, forecast_table, final_table
):
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""

    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT REPLACE(series, '"', '') AS SYMBOL, ts AS DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise


# Defining the DAG
with DAG(
    dag_id="prediction_DAG",
    start_date=datetime(2024, 10, 12),
    schedule_interval="17 1 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ETL"],
) as dag:

    raw_data = extract_stock_data()
    transformed_data = transform_stock_data(raw_data)
    load_to_snowflake(transformed_data)

    # Train and predict tasks
    train_input_table = "stock_data_db.raw_data.stock_prices"
    train_view = "stock_data_db.adhoc.market_data_view"
    forecast_table = "stock_data_db.adhoc.market_data_forecast"
    forecast_function_name = "stock_data_db.analytics.predict_stock_price"
    final_table = "stock_data_db.analytics.market_data"
    cur = return_snowflake_conn()

    train(cur, train_input_table, train_view, forecast_function_name)
    predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)

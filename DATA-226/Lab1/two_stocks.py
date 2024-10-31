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
    symbols = ["LMT", "MCD"]  # List of stock symbols to extract data for
    api_key = Variable.get("vantage_api_key")
    stock_data = {}

    for symbol in symbols:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        r = requests.get(url)
        data = r.json()
        stock_data[symbol] = data

    return stock_data


# Task to transform the extracted data into a format ready for loading into Snowflake
@task
def transform_stock_data(stock_data):
    results = []
    for symbol, data in stock_data.items():
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
            date TIMESTAMP_NTZ NOT NULL,
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


# Task to create and train a model for a specific stock symbol
@task
def train_stock_model(symbol):
    cur = return_snowflake_conn()
    cur.execute("CREATE SCHEMA IF NOT EXISTS stock_data_db.adhoc;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS stock_data_db.analytics;")
    train_input_table = "stock_data_db.raw_data.stock_prices"
    train_view = f"stock_data_db.adhoc.{symbol}_market_data_view"
    forecast_function_name = f"stock_data_db.analytics.predict_{symbol}_stock_price"

    # Create a view with training related columns for the specified symbol
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE::TIMESTAMP_NTZ AS DATE, CLOSE, SYMBOL
        FROM {train_input_table}
        WHERE SYMBOL = '{symbol}';"""  # Filter by symbol

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


# Task to generate predictions for a specific stock symbol
@task
def predict_stock(symbol):
    cur = return_snowflake_conn()
    train_input_table = "stock_data_db.raw_data.stock_prices"
    forecast_table = f"stock_data_db.adhoc.{symbol}_market_data_forecast"
    final_table = f"stock_data_db.analytics.{symbol}_market_data"
    forecast_function_name = f"stock_data_db.analytics.predict_{symbol}_stock_price"

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
        WHERE SYMBOL = '{symbol}'  -- Filter by symbol
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
    dag_id="two_stocks_DAG",
    start_date=datetime(2024, 10, 13),
    schedule_interval="40 2 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ETL"],
) as dag:

    raw_data = extract_stock_data()
    transformed_data = transform_stock_data(raw_data)
    load_to_snowflake(transformed_data)

    stock_symbols = ["LMT", "MCD"]

    for symbol in stock_symbols:
        train_stock_model(symbol)  # Train the model for each symbol
        predict_stock(symbol)  # Generate predictions for each symbol

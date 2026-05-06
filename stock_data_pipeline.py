from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import snowflake.connector
import os
import logging

# Snowflake credentials from Airflow Variables
SNOWFLAKE_USER = Variable.get("snowflake_user")
SNOWFLAKE_PASSWORD = Variable.get("snowflake_password")
SNOWFLAKE_ACCOUNT = Variable.get("snowflake_account")
SNOWFLAKE_WAREHOUSE = Variable.get("snowflake_warehouse")
SNOWFLAKE_DATABASE = Variable.get("snowflake_database")
SNOWFLAKE_SCHEMA = Variable.get("snowflake_schema")

LOCAL_FILE_PATH = "/tmp/stock_data.csv"
STOCKS = ["GOOGL", "AAPL"]


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    dag_id="stock_price_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Daily ETL pipeline to fetch stock data and load it into Snowflake",
)


def get_snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def create_snowflake_table():
    conn = None
    cursor = None

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STOCK_DATA (
                SYMBOL STRING,
                DATE DATE,
                OPEN FLOAT,
                CLOSE FLOAT,
                LOW FLOAT,
                HIGH FLOAT,
                VOLUME INTEGER
            )
            """
        )

        logging.info("STOCK_DATA table is ready in Snowflake.")

    except Exception as e:
        logging.error(f"Error creating Snowflake table: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def validate_stock_data(df):
    required_columns = ["DATE", "OPEN", "CLOSE", "LOW", "HIGH", "VOLUME", "SYMBOL"]

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    if df.empty:
        raise ValueError("Stock data is empty. No records were fetched from yFinance.")

    null_count = df[required_columns].isnull().sum().sum()
    if null_count > 0:
        logging.warning(f"Found {null_count} null values in stock data.")
        df = df.dropna(subset=["DATE", "OPEN", "CLOSE", "LOW", "HIGH", "VOLUME"])

    duplicate_count = df.duplicated(subset=["DATE", "SYMBOL"]).sum()
    if duplicate_count > 0:
        logging.warning(f"Found {duplicate_count} duplicate records. Removing duplicates.")
        df = df.drop_duplicates(subset=["DATE", "SYMBOL"])

    df["DATE"] = pd.to_datetime(df["DATE"], errors="raise").dt.strftime("%Y-%m-%d")

    logging.info(f"Data validation completed successfully. Final row count: {len(df)}")
    return df


def fetch_stock_data():
    try:
        df_list = []

        for stock in STOCKS:
            logging.info(f"Fetching stock data for {stock}")

            df = yf.download(stock, period="180d", interval="1d")

            if df.empty:
                logging.warning(f"No data returned for {stock}")
                continue

            df.reset_index(inplace=True)
            df["SYMBOL"] = stock
            df_list.append(df)

        if not df_list:
            raise ValueError("No stock data was fetched for any ticker.")

        df_final = pd.concat(df_list, ignore_index=True)

        df_final.rename(
            columns={
                "Date": "DATE",
                "Open": "OPEN",
                "Close": "CLOSE",
                "Low": "LOW",
                "High": "HIGH",
                "Volume": "VOLUME",
            },
            inplace=True,
        )

        df_final = validate_stock_data(df_final)

        df_final = df_final[
            ["SYMBOL", "DATE", "OPEN", "CLOSE", "LOW", "HIGH", "VOLUME"]
        ]

        df_final.to_csv(LOCAL_FILE_PATH, index=False, header=True)

        logging.info(f"Stock data saved successfully to {LOCAL_FILE_PATH}")

    except Exception as e:
        logging.error(f"Error fetching stock data: {e}")
        raise


def load_into_snowflake():
    conn = None
    cursor = None

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        stage_name = "TEMP_STOCK_STAGE"

        cursor.execute(f"CREATE TEMPORARY STAGE {stage_name}")

        cursor.execute(
            f"""
            PUT file://{LOCAL_FILE_PATH}
            @{stage_name}
            AUTO_COMPRESS = FALSE
            OVERWRITE = TRUE
            """
        )

        cursor.execute(
            f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STOCK_DATA
            FROM @{stage_name}/{os.path.basename(LOCAL_FILE_PATH)}
            FILE_FORMAT = (
                TYPE = 'CSV',
                SKIP_HEADER = 1,
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            """
        )

        logging.info("Stock data loaded successfully into Snowflake.")

    except Exception as e:
        logging.error(f"Error loading data into Snowflake: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def run_pipeline():
    logging.info("Stock ETL pipeline started.")

    create_snowflake_table()
    fetch_stock_data()
    load_into_snowflake()

    logging.info("Stock ETL pipeline completed successfully.")


run_stock_pipeline = PythonOperator(
    task_id="run_stock_pipeline",
    python_callable=run_pipeline,
    dag=dag,
)

run_stock_pipeline

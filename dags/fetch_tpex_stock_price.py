from datetime import datetime, timedelta
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException

@dag(
    'fetch_tpex_stock_price',
    description='Fetch tpex stock price and store in PSQL',
    schedule='0 8 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        'owner': 'ivan',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=['stock', 'tpex'],
)
def fetch_tpex_stock_data_dag():
    """
    This DAG fetches daily stock price data from TPEX and inserts it into a PostgreSQL database.
    """

    create_table = SQLExecuteQueryOperator(
        task_id='create_tw_stock_price_table',
        conn_id='PSQL_container',
        sql="""
            CREATE TABLE IF NOT EXISTS tw_stock_price (
                stock_id VARCHAR(20),
                stock_name VARCHAR(50),
                shares NUMERIC,
                trades INT,
                amount NUMERIC,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                trade_date DATE,
                market VARCHAR(10),
                PRIMARY KEY (stock_id, trade_date)
            );
        """,
    )

    @task(execution_timeout=timedelta(minutes=1))
    def fetch_otc_price_task(logical_date: datetime):
        """
        Fetches stock price data for all listed stocks on a given date.
        """
        index_date = logical_date.date()
        yyyymmdd = index_date.strftime('%Y%m%d')
        
        print(f"Fetching stock data for date: {yyyymmdd}")

        url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
        
        params = {
            "date": index_date.strftime('%Y/%m/%d'),
            "type": "EW",
            "id": "",
            "response": "json"
        }

        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if data.get('stat').upper() != 'OK' or 'tables' not in data:
                print(f"Warning: Unexpected response format for {yyyymmdd}. Response: {data}")
                return AirflowFailException(f"Warning: Unexpected response format for {yyyymmdd}.")
            
            table = data['tables'][0]
            print("=" * 50)
            print("table", table)  # Debugging output
            if not table.get('data'):
                print(f"No stock data found for {yyyymmdd}.")
                return AirflowFailException(f"No stock data found for {yyyymmdd}.")

            df = pd.DataFrame(table['data'], columns=table['fields']).iloc[:, :-7]
            df.columns = df.columns.str.strip()
            
            print("=" * 50)
            print(df.head())  # Debugging output

            # Data cleaning and type conversion
            # Using .apply(pd.to_numeric, errors='coerce') is more robust
            def clean_numeric(series):
                return pd.to_numeric(series.astype(str).str.replace(',', '').str.strip(), errors='coerce')

            df_cleaned = pd.DataFrame()
            df_cleaned['stock_id'] = df['代號'].str.strip()
            df_cleaned['stock_name'] = df['名稱'].str.strip()
            df_cleaned['shares'] = clean_numeric(df['成交股數'])/1000
            df_cleaned['trades'] = clean_numeric(df['成交筆數'])
            df_cleaned['amount'] = clean_numeric(df['成交金額(元)'])/10000
            df_cleaned['open'] = pd.to_numeric(df['開盤'], errors='coerce')
            df_cleaned['high'] = pd.to_numeric(df['最高'], errors='coerce')
            df_cleaned['low'] = pd.to_numeric(df['最低'], errors='coerce')
            df_cleaned['close'] = pd.to_numeric(df['收盤'], errors='coerce')

            df_cleaned['trade_date'] = index_date.strftime('%Y-%m-%d')
            df_cleaned['market'] = 'OTC'

            print(df_cleaned[df_cleaned.isnull().any(axis=1)])
            df_cleaned = df_cleaned.fillna(0)

            # 檢查是否還有 NaN
            if df_cleaned.isnull().values.any():
                print("Data contains NaN after fillna. Raising exception for logging.")
                print(df_cleaned[df_cleaned.isnull().any(axis=1)])
                raise AirflowFailException("Data contains NaN values after fillna.")

            print(f"Successfully fetched {len(df)} rows for date: {yyyymmdd}")
            print("=" * 50)
            print(df_cleaned.head())  # Debugging output
            
            # Prepare data for insertion
            return df_cleaned.values.tolist()

        except requests.exceptions.RequestException as e:
            print(f"Request failed for {yyyymmdd}: {e}")
            raise AirflowFailException(f"API request failed: {e}")
        except (ValueError, IndexError, KeyError) as e:
            print(f"Data parsing failed for {yyyymmdd}: {e}")
            raise AirflowFailException(f"Data parsing failed: {e}")

    @task
    def insert_to_postgres(records: list):
        """
        Inserts fetched stock data into the PostgreSQL table in batches.
        """
        if not records:
            print("No data to insert. Skipping insertion task.")
            return

        print(f"Preparing to insert {len(records)} rows into the database.")
        
        hook = PostgresHook(postgres_conn_id='PSQL_container')
        
        # Column order for insertion
        target_fields = ['stock_id', 'stock_name', 'shares', 'trades', 'amount', 'open', 'high', 'low', 'close', 'trade_date', 'market']
        
        try:
            # Batch insertion
            hook.insert_rows(
                table='tw_stock_price',
                rows=records,
                target_fields=target_fields,
                replace=True,  # Use ON CONFLICT DO UPDATE to handle duplicates
                replace_index=['stock_id', 'trade_date']
            )
            print(f"Successfully inserted {len(records)} rows into tw_stock_price.")
        except Exception as e:
            print(f"Failed to insert data into PostgreSQL: {e}")
            raise AirflowFailException(f"Database insertion failed: {e}")
            
    # Task dependencies
    create_table >> insert_to_postgres(fetch_otc_price_task())

fetch_tpex_stock_data_dag()
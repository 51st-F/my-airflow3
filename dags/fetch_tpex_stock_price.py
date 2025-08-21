from datetime import datetime, timedelta
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException, AirflowSkipException

from utils.bot_telegram import send_task_telegram_notification, send_insert_success_notification
from utils.datasets import dataset_tpex_stock

@dag(
    dag_id='fetch_tpex_stock_price',
    description='Fetch tpex stock price and store in PSQL',
    schedule='45 8 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'on_failure_callback': send_task_telegram_notification,
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

    @task.branch
    def check_if_data_exists(**kwargs):
        """
        先檢查資料是否存在，若存在則跳過，若不存在則執行資料抓取。
        """
        logical_date = kwargs['dag_run'].run_after
        execution_date_str = logical_date.date().strftime('%Y-%m-%d')
        hook = PostgresHook(postgres_conn_id='PSQL_container')
        
        sql_query = f"SELECT count(*) FROM tw_stock_price WHERE trade_date = '{execution_date_str}' AND market = 'OTC';"
        
        try:
            result = hook.get_first(sql_query)
            count = result[0]
            print("=" * 20)
            print(f"Data count for {execution_date_str}: {count}")
            
            if count >= 99:
                print(f"Data for {execution_date_str} already exists. Skipping.")
                return 'data_already_exists'
            else:
                print(f"Data for {execution_date_str} is missing or has an abnormal count ({count}). Proceeding with data fetching.")
                return 'data_not_found'
            
        except Exception as e:
            raise AirflowFailException(f"Database check failed: {e}")

    @task(execution_timeout=timedelta(minutes=1))
    def fetch_otc_price_task(**kwargs):
        """
        Fetches stock price data for all listed stocks on a given date.
        """
        logical_date = kwargs['dag_run'].run_after
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
                raise AirflowFailException(f"Warning: Unexpected response format for {yyyymmdd}. Response: {data}")
            
            table = data['tables'][0]
            if not table.get('data') or table['totalCount'] == 0:
                raise AirflowSkipException(f"No stock data found for {data.get('date')}.")

            df = pd.DataFrame(table['data'], columns=table['fields']).iloc[:, :-7]
            df.columns = df.columns.str.strip()

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
                print(df_cleaned[df_cleaned.isnull().any(axis=1)])
                raise AirflowFailException("Data contains NaN values after fillna. Raising exception for logging.")

            print(f"Successfully fetched {len(df)} rows for date: {yyyymmdd}")
            
            return df_cleaned.values.tolist()

        except requests.exceptions.RequestException as e:
            raise AirflowFailException(f"API request failed for {yyyymmdd}: {e}")
        except (ValueError, IndexError, KeyError) as e:
            raise AirflowFailException(f"Data parsing failed for {yyyymmdd}: {e}")

    @task(outlets=[dataset_tpex_stock])
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
            return len(records)
        
        except Exception as e:
            raise AirflowFailException(f"PostgreSQL Database insertion failed: {e}")
            
    @task
    def data_already_exists():
        print("Data for the current date already exists, so skipping data fetching and insertion.")
        
    @task
    def data_not_found():
        print("No data found for the current date.")

    @task
    def send_telegram_msg(inserted_count: int):
        send_insert_success_notification(inserted_count)

    check_exists = check_if_data_exists()
    fetched_data = fetch_otc_price_task()
    insert_data = insert_to_postgres(fetched_data)
        
    create_table >> check_exists
    check_exists >> data_already_exists()
    check_exists >> data_not_found() >> fetched_data >> insert_data >> send_telegram_msg(insert_data)

fetch_tpex_stock_data_dag()
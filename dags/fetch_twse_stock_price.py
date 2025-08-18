from datetime import datetime, timedelta
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException, AirflowSkipException

from utils.datasets import dataset_twse_stock

@dag(
    'fetch_twse_stock_price',
    description='Fetch twse stock price and store in PSQL',
    schedule='30 9 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        'owner': 'ivan',
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=['stock', 'twse'],
)
def fetch_twse_stock_data_dag():
    """
    This DAG fetches daily stock price data from TWSE and inserts it into a PostgreSQL database.
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
    def check_if_data_exists(logical_date: datetime):
        """
        先檢查資料是否存在，若存在則跳過，若不存在則執行資料抓取。
        """
        execution_date_str = logical_date.date().strftime('%Y-%m-%d')
        hook = PostgresHook(postgres_conn_id='PSQL_container')
        
        sql_query = f"SELECT count(*) FROM tw_stock_price WHERE trade_date = '{execution_date_str}' AND market = 'TSE';"
        
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

    @task
    def fetch_sii_price_task(logical_date: datetime):
        """
        Fetches stock price data for all listed stocks on a given date.
        """
        index_date = logical_date.date()
        yyyymmdd = index_date.strftime('%Y%m%d')
        
        print(f"Fetching stock data for date: {yyyymmdd}")

        url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        params = {
            "date": yyyymmdd,
            "type": "ALLBUT0999",
            "response": "json"
        }

        try:
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if data.get('stat').upper() == '很抱歉，沒有符合條件的資料!':
                raise AirflowSkipException(f"Warning: Unexpected response format for {yyyymmdd}. Response: {data}. Maybe holiday?")
            
            table = data['tables'][8]
            if not table.get('data'):
                raise AirflowFailException(f"No stock data found for {yyyymmdd}.")

            df = pd.DataFrame(table['data'], columns=table['fields']).iloc[:, :-7]

            # Data cleaning and type conversion
            # Using .apply(pd.to_numeric, errors='coerce') is more robust
            for col in ["成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價", "收盤價"]:
                df[col] = df[col].astype(str).str.replace(",", "").replace("--", "0").apply(pd.to_numeric, errors='coerce')

            # Unit conversion
            df["成交股數"] = df["成交股數"] / 1000
            df["成交金額"] = df["成交金額"] / 10000 
            df["交易日期"] = index_date.strftime('%Y-%m-%d')
            df["市場別"] = "TSE"

            print(df[df.isnull().any(axis=1)])
            df = df.fillna(0)

            # 檢查是否還有 NaN
            if df.isnull().values.any():
                print(df[df.isnull().any(axis=1)])
                raise AirflowFailException("Data contains NaN values after fillna. Raising exception for logging.")
            
            print(f"Successfully fetched {len(df)} rows for date: {yyyymmdd}")
            
            return df[['證券代號', '證券名稱', '成交股數', '成交筆數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '交易日期', '市場別']].values.tolist()

        except requests.exceptions.RequestException as e:
            raise AirflowFailException(f"API request failed for {yyyymmdd}: {e}")
        except (ValueError, IndexError, KeyError) as e:
            raise AirflowFailException(f"Data parsing failed for {yyyymmdd}: {e}")

    @task(outlets=[dataset_twse_stock])
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
            raise AirflowFailException(f"PostgreSQL Database insertion failed: {e}")
            
    @task
    def data_already_exists():
        print("Data for the current date already exists, so skipping data fetching and insertion.")
        
    @task
    def data_not_found():
        print("No data found for the current date.")

    check_exists = check_if_data_exists()
    fetched_data = fetch_sii_price_task()
        
    create_table >> check_exists
    check_exists >> data_already_exists()
    check_exists >> data_not_found() >> fetched_data >> insert_to_postgres(fetched_data)
    
fetch_twse_stock_data_dag()
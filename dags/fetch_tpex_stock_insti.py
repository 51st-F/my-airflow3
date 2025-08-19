from datetime import datetime, timedelta
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException, AirflowSkipException

from utils.datasets import dataset_tpex_stock

@dag(
    'fetch_tpex_stock_insti',
    description='Fetch tpex stock insti and store in PSQL',
    schedule=[dataset_tpex_stock],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        'owner': 'ivan',
        'retries': 2,
        'retry_delay': timedelta(minutes=30),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=['stock', 'tpex'],
)
def fetch_tpex_stock_data_dag_insti_trading():
    """
    This DAG fetches daily stock insti data from TPEX and inserts it into a PostgreSQL database.
    """

    create_table = SQLExecuteQueryOperator(
        task_id='create_tpex_stock_insti_table',
        conn_id='PSQL_container',
        sql="""
            CREATE TABLE IF NOT EXISTS tpex_stock_insti (
                trade_date DATE NOT NULL,
                stock_id VARCHAR(20) NOT NULL,
                stock_name VARCHAR(50) NOT NULL,

                foreign_excl_dealer_buy BIGINT,
                foreign_excl_dealer_sell BIGINT,
                foreign_excl_dealer_net BIGINT,

                foreign_dealer_buy BIGINT,
                foreign_dealer_sell BIGINT,
                foreign_dealer_net BIGINT,

                foreign_buy BIGINT,
                foreign_sell BIGINT,
                foreign_net BIGINT,

                investment_trust_buy BIGINT,
                investment_trust_sell BIGINT,
                investment_trust_net BIGINT,

                dealer_self_buy BIGINT,
                dealer_self_sell BIGINT,
                dealer_self_net BIGINT,

                dealer_hedge_buy BIGINT,
                dealer_hedge_sell BIGINT,
                dealer_hedge_net BIGINT,

                dealer_buy BIGINT,
                dealer_sell BIGINT,
                dealer_net BIGINT,

                total_net BIGINT,

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
        
        sql_query = f"SELECT count(*) FROM tpex_stock_insti WHERE trade_date = '{execution_date_str}';"
        
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
    def fetch_sii_insti_task(**kwargs):
        """
        Fetches stock insti data for all listed stocks on a given date.
        """
        logical_date = kwargs['dag_run'].run_after
        index_date = logical_date.date()
        yyyymmdd = index_date.strftime('%Y%m%d')
        
        print(f"Fetching stock data for date: {yyyymmdd}")

        url = "https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade"

        params = {
            "type": "Daily",
            "sect": "EW",
            "date": index_date.strftime('%Y/%m/%d'),
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

            column_name = [
                'stock_id',
                'stock_name',

                'foreign_excl_dealer_buy',
                'foreign_excl_dealer_sell',
                'foreign_excl_dealer_net',

                'foreign_dealer_buy',
                'foreign_dealer_sell',
                'foreign_dealer_net',

                'foreign_buy',
                'foreign_sell',
                'foreign_net',

                'investment_trust_buy',
                'investment_trust_sell',
                'investment_trust_net',

                'dealer_self_buy',
                'dealer_self_sell',
                'dealer_self_net',

                'dealer_hedge_buy',
                'dealer_hedge_sell',
                'dealer_hedge_net',

                'dealer_buy',
                'dealer_sell',
                'dealer_net',

                'total_net',
            ]

            df = pd.DataFrame(table['data'], columns=column_name)

            for col in df.columns[2:]:
                df[col] = df[col].str.replace(",", "").astype(int)

            df['stock_id'] = df['stock_id'].astype(str)
            df['trade_date'] = index_date.strftime('%Y-%m-%d')
            
            print(df[df.isnull().any(axis=1)])
            df = df.fillna(0)

            # 檢查是否還有 NaN
            if df.isnull().values.any():
                print(df[df.isnull().any(axis=1)])
                raise AirflowFailException("Data contains NaN values after fillna. Raising exception for logging.")
            
            print(f"Successfully fetched {len(df)} rows for date: {yyyymmdd}")
            
            return df.values.tolist()

        except requests.exceptions.RequestException as e:
            raise AirflowFailException(f"API request failed for {yyyymmdd}: {e}")
        except (ValueError, IndexError, KeyError) as e:
            raise AirflowFailException(f"Data parsing failed for {yyyymmdd}: {e}")

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
        target_fields = [
            'stock_id',
            'stock_name',

            'foreign_excl_dealer_buy',
            'foreign_excl_dealer_sell',
            'foreign_excl_dealer_net',

            'foreign_dealer_buy',
            'foreign_dealer_sell',
            'foreign_dealer_net',

            'foreign_buy',
            'foreign_sell',
            'foreign_net',

            'investment_trust_buy',
            'investment_trust_sell',
            'investment_trust_net',

            'dealer_self_buy',
            'dealer_self_sell',
            'dealer_self_net',

            'dealer_hedge_buy',
            'dealer_hedge_sell',
            'dealer_hedge_net',

            'dealer_buy',
            'dealer_sell',
            'dealer_net',

            'total_net',
            'trade_date'
        ]
        
        try:
            # Batch insertion
            hook.insert_rows(
                table='tpex_stock_insti',
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
    fetched_data = fetch_sii_insti_task()
        
    create_table >> check_exists
    check_exists >> data_already_exists()
    check_exists >> data_not_found() >> fetched_data >> insert_to_postgres(fetched_data)
    
fetch_tpex_stock_data_dag_insti_trading()
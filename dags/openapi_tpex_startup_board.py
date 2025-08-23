
from datetime import datetime, timedelta
import requests
import json

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

from utils.bot_telegram import send_task_telegram_notification
from utils.database import db_openapi_tpex

def call_tpex_api(path: str, method: str = "GET", **kwargs):
    # 取得 task instance，抓取 execution context 中資訊
    execution_date = kwargs.get("execution_date")

    # 呼叫 API
    url = f"https://www.tpex.org.tw/openapi/v1{path}"
    response = requests.request(method, url)
    response.raise_for_status()
    data = response.json()

    # 印出部分資料（方便除錯）
    print("data lens", len(data))
    print(json.dumps(data[:25], indent=2, ensure_ascii=False) if isinstance(data, list) else str(data))

    # 儲存到 MongoDB
    # collection = db_openapi_tpex["startup_board"]

    # doc = {
    #     "path": path,
    #     "method": method,
    #     "timestamp": datetime.now(),
    #     "execution_date": execution_date,
    #     "data": data
    # }

    # from bson import json_util
    # import traceback

    # try:
    #     collection.insert_one(doc)
    # except Exception as e:
    #     print("Mongo insert error:", e)
    #     print("Doc preview:", json.dumps(doc, default=json_util.default)[:1000])
    #     traceback.print_exc()

@dag(
    dag_id="openapi_tpex_startup_board",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["tpex", "startup_board"],
)
def fetch_startup_board():
    # 創櫃板公司市場現況
    get__tpex_gisa_highlight = PythonOperator(
        task_id="get__tpex_gisa_highlight",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_gisa_highlight", "method": "GET"},
    )

    # 創櫃板公司資訊
    get__tpex_gisa_company = PythonOperator(
        task_id="get__tpex_gisa_company",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_gisa_company", "method": "GET"},
    )

    # 於登錄創櫃板前辦理籌資資訊
    get__tpex_gisa_financing_before = PythonOperator(
        task_id="get__tpex_gisa_financing_before",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_gisa_financing_before", "method": "GET"},
    )

    # 創櫃板公司透過籌資系統辦理籌資資訊
    get__tpex_gisa_financing_history = PythonOperator(
        task_id="get__tpex_gisa_financing_history",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_gisa_financing_history", "method": "GET"},
    )

    # 創櫃板辦理中籌資資訊
    get__tpex_gisa_financing_in_process = PythonOperator(
        task_id="get__tpex_gisa_financing_in_process",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_gisa_financing_in_process", "method": "GET"},
    )


    get__tpex_gisa_highlight >> get__tpex_gisa_company >> get__tpex_gisa_financing_before >> get__tpex_gisa_financing_history >> get__tpex_gisa_financing_in_process
    # 定義 task 執行順序（平行執行）
    # return [get__tpex_gisa_highlight, get__tpex_gisa_company, get__tpex_gisa_financing_before, get__tpex_gisa_financing_history, get__tpex_gisa_financing_in_process]

# 實例化 DAG
dag = fetch_startup_board()

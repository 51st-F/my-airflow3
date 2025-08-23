
from datetime import datetime, timedelta
import requests
import json

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

from utils.bot_telegram import send_task_telegram_notification
from utils.database import db_openapi_twse

def call_twse_api(path: str, method: str = "GET", **kwargs):
    # 取得 task instance，抓取 execution context 中資訊
    execution_date = kwargs.get("execution_date")

    # 呼叫 API
    url = f"https://openapi.twse.com.tw/v1{path}"
    response = requests.request(method, url)
    response.raise_for_status()
    data = response.json()

    # 印出部分資料（方便除錯）
    print("data lens", len(data))
    print(json.dumps(data[:25], indent=2, ensure_ascii=False) if isinstance(data, list) else str(data))

    # 儲存到 MongoDB
    # collection = db_openapi_twse["warrants"]

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
    dag_id="openapi_twse_warrants",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["twse", "warrants"],
)
def fetch_warrants():
    # 上市認購(售)權證年度發行量概況統計表
    get__opendata_t187ap36_l = PythonOperator(
        task_id="get__opendata_t187ap36_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap36_L", "method": "GET"},
    )

    # 上市認購(售)權證交易人數檔
    get__opendata_t187ap43_l = PythonOperator(
        task_id="get__opendata_t187ap43_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap43_L", "method": "GET"},
    )

    # 上市認購(售)權證每日成交資料檔
    get__opendata_t187ap42_l = PythonOperator(
        task_id="get__opendata_t187ap42_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap42_L", "method": "GET"},
    )


    get__opendata_t187ap36_l >> get__opendata_t187ap43_l >> get__opendata_t187ap42_l
    # 定義 task 執行順序（平行執行）
    # return [get__opendata_t187ap36_l, get__opendata_t187ap43_l, get__opendata_t187ap42_l]

# 實例化 DAG
dag = fetch_warrants()

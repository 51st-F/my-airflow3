
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
    # collection = db_openapi_twse["indices"]

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
    dag_id="openapi_twse_indices",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["twse", "indices"],
)
def fetch_indices():
    # 每日上市上櫃跨市場成交資訊
    get__exchangereport_mi_index4 = PythonOperator(
        task_id="get__exchangereport_mi_index4",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/MI_INDEX4", "method": "GET"},
    )

    # 寶島股價指數歷史資料
    get__indicesreport_frmsa = PythonOperator(
        task_id="get__indicesreport_frmsa",
        python_callable=call_twse_api,
        op_kwargs={"path": "/indicesReport/FRMSA", "method": "GET"},
    )

    # 臺灣 50 指數歷史資料
    get__indicesreport_tai50i = PythonOperator(
        task_id="get__indicesreport_tai50i",
        python_callable=call_twse_api,
        op_kwargs={"path": "/indicesReport/TAI50I", "method": "GET"},
    )

    # 發行量加權股價指數歷史資料
    get__indicesreport_mi_5mins_hist = PythonOperator(
        task_id="get__indicesreport_mi_5mins_hist",
        python_callable=call_twse_api,
        op_kwargs={"path": "/indicesReport/MI_5MINS_HIST", "method": "GET"},
    )

    # 發行量加權股價報酬指數
    get__indicesreport_mfi94u = PythonOperator(
        task_id="get__indicesreport_mfi94u",
        python_callable=call_twse_api,
        op_kwargs={"path": "/indicesReport/MFI94U", "method": "GET"},
    )


    get__exchangereport_mi_index4 >> get__indicesreport_frmsa >> get__indicesreport_tai50i >> get__indicesreport_mi_5mins_hist >> get__indicesreport_mfi94u
    # 定義 task 執行順序（平行執行）
    # return [get__exchangereport_mi_index4, get__indicesreport_frmsa, get__indicesreport_tai50i, get__indicesreport_mi_5mins_hist, get__indicesreport_mfi94u]

# 實例化 DAG
dag = fetch_indices()

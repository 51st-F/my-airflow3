
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
    # collection = db_openapi_tpex["emerging"]

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
    dag_id="openapi_tpex_emerging",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["tpex", "emerging"],
)
def fetch_emerging():
    # 興櫃處置有價證券資訊
    get__tpex_esb_disposal_information = PythonOperator(
        task_id="get__tpex_esb_disposal_information",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_esb_disposal_information", "method": "GET"},
    )

    # 興櫃公布注意有價證券資訊
    get__tpex_esb_warning_information = PythonOperator(
        task_id="get__tpex_esb_warning_information",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_esb_warning_information", "method": "GET"},
    )

    # 興櫃推薦證券商與推薦之股票
    get__tpex_esb_recommended_dealer = PythonOperator(
        task_id="get__tpex_esb_recommended_dealer",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_esb_recommended_dealer", "method": "GET"},
    )

    # 興櫃股票當日行情表
    get__tpex_esb_latest_statistics = PythonOperator(
        task_id="get__tpex_esb_latest_statistics",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_esb_latest_statistics", "method": "GET"},
    )

    # 興櫃股票市場現況
    get__tpex_esb_highlight = PythonOperator(
        task_id="get__tpex_esb_highlight",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_esb_highlight", "method": "GET"},
    )


    get__tpex_esb_disposal_information >> get__tpex_esb_warning_information >> get__tpex_esb_recommended_dealer >> get__tpex_esb_latest_statistics >> get__tpex_esb_highlight
    # 定義 task 執行順序（平行執行）
    # return [get__tpex_esb_disposal_information, get__tpex_esb_warning_information, get__tpex_esb_recommended_dealer, get__tpex_esb_latest_statistics, get__tpex_esb_highlight]

# 實例化 DAG
dag = fetch_emerging()

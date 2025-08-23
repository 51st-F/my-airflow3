
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
    # collection = db_openapi_twse["others"]

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
    dag_id="openapi_twse_others",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["twse", "others"],
)
def fetch_others():
    # 基金基本資料彙總表
    get__opendata_t187ap47_l = PythonOperator(
        task_id="get__opendata_t187ap47_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap47_L", "method": "GET"},
    )

    # 證交所活動訊息
    get__news_eventlist = PythonOperator(
        task_id="get__news_eventlist",
        python_callable=call_twse_api,
        op_kwargs={"path": "/news/eventList", "method": "GET"},
    )

    # 證交所新聞
    get__news_newslist = PythonOperator(
        task_id="get__news_newslist",
        python_callable=call_twse_api,
        op_kwargs={"path": "/news/newsList", "method": "GET"},
    )

    # 中央登錄公債補息資料表
    get__exchangereport_bfi61u = PythonOperator(
        task_id="get__exchangereport_bfi61u",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/BFI61U", "method": "GET"},
    )


    get__opendata_t187ap47_l >> get__news_eventlist >> get__news_newslist >> get__exchangereport_bfi61u
    # 定義 task 執行順序（平行執行）
    # return [get__opendata_t187ap47_l, get__news_eventlist, get__news_newslist, get__exchangereport_bfi61u]

# 實例化 DAG
dag = fetch_others()

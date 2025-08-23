
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
    # collection = db_openapi_tpex["warrants"]

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
    dag_id="openapi_tpex_warrants",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["tpex", "warrants"],
)
def fetch_warrants():
    # 黃金現貨權證發行基本資料
    get__tpex_warrant_gold = PythonOperator(
        task_id="get__tpex_warrant_gold",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_gold", "method": "GET"},
    )

    # 黃金現貨權證收盤行情
    get__tpex_warrant_gold_quts = PythonOperator(
        task_id="get__tpex_warrant_gold_quts",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_gold_quts", "method": "GET"},
    )

    # 上櫃權證收盤行情日報表
    get__tpex_warrant_daily_quts = PythonOperator(
        task_id="get__tpex_warrant_daily_quts",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_daily_quts", "method": "GET"},
    )

    # 上櫃權證收盤行情月報表
    get__tpex_warrant_monthly_quts = PythonOperator(
        task_id="get__tpex_warrant_monthly_quts",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_monthly_quts", "method": "GET"},
    )

    # 上櫃權證發行基本資料
    get__tpex_warrant_issue = PythonOperator(
        task_id="get__tpex_warrant_issue",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_issue", "method": "GET"},
    )

    # 上櫃牛熊證收盤行情(不含展延型牛熊證)日報表
    get__tpex_warrant_wcb_daily_quts = PythonOperator(
        task_id="get__tpex_warrant_wcb_daily_quts",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_wcb_daily_quts", "method": "GET"},
    )

    # 上櫃牛熊證收盤行情(不含展延型牛熊證)月報表
    get__tpex_warrant_wcb_monthly_quts = PythonOperator(
        task_id="get__tpex_warrant_wcb_monthly_quts",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_wcb_monthly_quts", "method": "GET"},
    )

    # 上櫃牛熊證發行基本資料(不含展延型牛熊證)
    get__tpex_warrant_wcb_issue = PythonOperator(
        task_id="get__tpex_warrant_wcb_issue",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_wcb_issue", "method": "GET"},
    )

    # 上櫃展延型牛熊證收盤行情日報表
    get__tpex_warrant_wxy_daily_quts = PythonOperator(
        task_id="get__tpex_warrant_wxy_daily_quts",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_wxy_daily_quts", "method": "GET"},
    )

    # 上櫃展延型牛熊證收盤行情月報表
    get__tpex_warrant_wxy_monthly_quts = PythonOperator(
        task_id="get__tpex_warrant_wxy_monthly_quts",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_wxy_monthly_quts", "method": "GET"},
    )

    # 上櫃展延型牛熊證發行基本資料
    get__tpex_warrant_wxy_issue = PythonOperator(
        task_id="get__tpex_warrant_wxy_issue",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_wxy_issue", "method": "GET"},
    )

    # 單筆權證成交資料
    get__tpex_warrant_quts = PythonOperator(
        task_id="get__tpex_warrant_quts",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_quts", "method": "GET"},
    )

    # 每日權證交易人數(上櫃)
    get__tpex_warrant_statistics = PythonOperator(
        task_id="get__tpex_warrant_statistics",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_statistics", "method": "GET"},
    )

    # 上櫃股票權證資訊
    get__tpex_warrant = PythonOperator(
        task_id="get__tpex_warrant",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant", "method": "GET"},
    )

    # 上櫃權證當日暫停/恢復交易資訊
    get__tpex_warrant_suspend_today = PythonOperator(
        task_id="get__tpex_warrant_suspend_today",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_suspend_today", "method": "GET"},
    )

    # 上櫃權證歷史暫停/恢復交易資訊
    get__tpex_warrant_suspend_history = PythonOperator(
        task_id="get__tpex_warrant_suspend_history",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_warrant_suspend_history", "method": "GET"},
    )

    # 上櫃權證基本資料彙總表
    get__mopsfin_t187ap37_o = PythonOperator(
        task_id="get__mopsfin_t187ap37_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap37_O", "method": "GET"},
    )

    # 上櫃認購(售)權證每日成交資料檔
    get__mopsfin_t187ap42_o = PythonOperator(
        task_id="get__mopsfin_t187ap42_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap42_O", "method": "GET"},
    )

    # 上櫃認購(售)權證年度發行量概況統計表
    get__mopsfin_t187ap36_o = PythonOperator(
        task_id="get__mopsfin_t187ap36_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap36_O", "method": "GET"},
    )


    get__tpex_warrant_gold >> get__tpex_warrant_gold_quts >> get__tpex_warrant_daily_quts >> get__tpex_warrant_monthly_quts >> get__tpex_warrant_issue >> get__tpex_warrant_wcb_daily_quts >> get__tpex_warrant_wcb_monthly_quts >> get__tpex_warrant_wcb_issue >> get__tpex_warrant_wxy_daily_quts >> get__tpex_warrant_wxy_monthly_quts >> get__tpex_warrant_wxy_issue >> get__tpex_warrant_quts >> get__tpex_warrant_statistics >> get__tpex_warrant >> get__tpex_warrant_suspend_today >> get__tpex_warrant_suspend_history >> get__mopsfin_t187ap37_o >> get__mopsfin_t187ap42_o >> get__mopsfin_t187ap36_o
    # 定義 task 執行順序（平行執行）
    # return [get__tpex_warrant_gold, get__tpex_warrant_gold_quts, get__tpex_warrant_daily_quts, get__tpex_warrant_monthly_quts, get__tpex_warrant_issue, get__tpex_warrant_wcb_daily_quts, get__tpex_warrant_wcb_monthly_quts, get__tpex_warrant_wcb_issue, get__tpex_warrant_wxy_daily_quts, get__tpex_warrant_wxy_monthly_quts, get__tpex_warrant_wxy_issue, get__tpex_warrant_quts, get__tpex_warrant_statistics, get__tpex_warrant, get__tpex_warrant_suspend_today, get__tpex_warrant_suspend_history, get__mopsfin_t187ap37_o, get__mopsfin_t187ap42_o, get__mopsfin_t187ap36_o]

# 實例化 DAG
dag = fetch_warrants()

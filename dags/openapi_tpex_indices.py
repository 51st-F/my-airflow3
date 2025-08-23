
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
    # collection = db_openapi_tpex["indices"]

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
    dag_id="openapi_tpex_indices",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["tpex", "indices"],
)
def fetch_indices():
    # 櫃買指數歷史資料
    get__tpex_index = PythonOperator(
        task_id="get__tpex_index",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_index", "method": "GET"},
    )

    # 富櫃50指數歷史收盤指數
    get__tpex50_index = PythonOperator(
        task_id="get__tpex50_index",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex50_index", "method": "GET"},
    )

    # 櫃買「富櫃200指數」當日收盤指數
    get__tpex200_change = PythonOperator(
        task_id="get__tpex200_change",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex200_change", "method": "GET"},
    )

    # 上櫃公司治理指數當日成分股資訊
    get__tpcgi_constituents = PythonOperator(
        task_id="get__tpcgi_constituents",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpcgi_constituents", "method": "GET"},
    )

    # 上櫃公司治理指數歷史收盤指數
    get__tpcgi_reward_index = PythonOperator(
        task_id="get__tpcgi_reward_index",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpcgi_reward_index", "method": "GET"},
    )

    # 上櫃公司治理指數當日收盤指數
    get__tpcgi_change = PythonOperator(
        task_id="get__tpcgi_change",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpcgi_change", "method": "GET"},
    )

    # 櫃買指數成分股
    get__tpex_index_consti = PythonOperator(
        task_id="get__tpex_index_consti",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_index_consti", "method": "GET"},
    )

    # 櫃買指數與報酬指數之收市指數
    get__tpex_reward_index = PythonOperator(
        task_id="get__tpex_reward_index",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_reward_index", "method": "GET"},
    )

    # 櫃買「富櫃50指數」當日成分股
    get__tpex50_constituents = PythonOperator(
        task_id="get__tpex50_constituents",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex50_constituents", "method": "GET"},
    )

    # 櫃買「富櫃50指數」當日收盤指數
    get__tpex50_change = PythonOperator(
        task_id="get__tpex50_change",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex50_change", "method": "GET"},
    )

    # 櫃買「高殖利率指數」當日成分股
    get__tphd_constituents = PythonOperator(
        task_id="get__tphd_constituents",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tphd_constituents", "method": "GET"},
    )

    # 櫃買「高殖利率指數」當日收盤指數
    get__tphd_change = PythonOperator(
        task_id="get__tphd_change",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tphd_change", "method": "GET"},
    )

    # 櫃買「薪酬指數」當日成分股
    get__tpci_constituents = PythonOperator(
        task_id="get__tpci_constituents",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpci_constituents", "method": "GET"},
    )

    # 櫃買「薪酬指數」當日收盤指數
    get__tpci_change = PythonOperator(
        task_id="get__tpci_change",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpci_change", "method": "GET"},
    )

    # 櫃買「薪酬指數」歷史收盤指數
    get__tpci_reward_index = PythonOperator(
        task_id="get__tpci_reward_index",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpci_reward_index", "method": "GET"},
    )

    # 櫃買「勞工就業88指數」當日成分股
    get__tpex_emp88_constituents = PythonOperator(
        task_id="get__tpex_emp88_constituents",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_emp88_constituents", "method": "GET"},
    )

    # 櫃買「勞工就業88指數」當日收盤指數
    get__tpex_emp88_change = PythonOperator(
        task_id="get__tpex_emp88_change",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_emp88_change", "method": "GET"},
    )

    # 櫃買「勞工就業88指數」歷史收盤指數
    get__tpex_emp88_reward_index = PythonOperator(
        task_id="get__tpex_emp88_reward_index",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_emp88_reward_index", "method": "GET"},
    )

    # 櫃買「富櫃200指數」當日成分股
    get__tpex200_constituents = PythonOperator(
        task_id="get__tpex200_constituents",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex200_constituents", "method": "GET"},
    )

    # 高殖利率指數歷史收盤指數
    get__tphd_index = PythonOperator(
        task_id="get__tphd_index",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tphd_index", "method": "GET"},
    )


    get__tpex_index >> get__tpex50_index >> get__tpex200_change >> get__tpcgi_constituents >> get__tpcgi_reward_index >> get__tpcgi_change >> get__tpex_index_consti >> get__tpex_reward_index >> get__tpex50_constituents >> get__tpex50_change >> get__tphd_constituents >> get__tphd_change >> get__tpci_constituents >> get__tpci_change >> get__tpci_reward_index >> get__tpex_emp88_constituents >> get__tpex_emp88_change >> get__tpex_emp88_reward_index >> get__tpex200_constituents >> get__tphd_index
    # 定義 task 執行順序（平行執行）
    # return [get__tpex_index, get__tpex50_index, get__tpex200_change, get__tpcgi_constituents, get__tpcgi_reward_index, get__tpcgi_change, get__tpex_index_consti, get__tpex_reward_index, get__tpex50_constituents, get__tpex50_change, get__tphd_constituents, get__tphd_change, get__tpci_constituents, get__tpci_change, get__tpci_reward_index, get__tpex_emp88_constituents, get__tpex_emp88_change, get__tpex_emp88_reward_index, get__tpex200_constituents, get__tphd_index]

# 實例化 DAG
dag = fetch_indices()

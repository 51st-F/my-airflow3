
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
    # collection = db_openapi_twse["broker_data"]

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
    dag_id="openapi_twse_broker_data",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["twse", "broker_data"],
)
def fetch_broker_data():
    # 定期定額前十名交易戶數證券統計
    get__etfreport_etfrank = PythonOperator(
        task_id="get__etfreport_etfrank",
        python_callable=call_twse_api,
        op_kwargs={"path": "/ETFReport/ETFRank", "method": "GET"},
    )

    # 開辦定期定額業務證券商名單
    get__brokerservice_secregdata = PythonOperator(
        task_id="get__brokerservice_secregdata",
        python_callable=call_twse_api,
        op_kwargs={"path": "/brokerService/secRegData", "method": "GET"},
    )

    # 證券商總公司基本資料
    get__brokerservice_brokerlist = PythonOperator(
        task_id="get__brokerservice_brokerlist",
        python_callable=call_twse_api,
        op_kwargs={"path": "/brokerService/brokerList", "method": "GET"},
    )

    # 券商業務別人員數
    get__opendata_t187ap01 = PythonOperator(
        task_id="get__opendata_t187ap01",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap01", "method": "GET"},
    )

    # 各券商每月月計表
    get__opendata_t187ap20 = PythonOperator(
        task_id="get__opendata_t187ap20",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap20", "method": "GET"},
    )

    # 各券商收支概況表資料
    get__opendata_t187ap21 = PythonOperator(
        task_id="get__opendata_t187ap21",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap21", "method": "GET"},
    )

    # 證券商基本資料
    get__opendata_t187ap18 = PythonOperator(
        task_id="get__opendata_t187ap18",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap18", "method": "GET"},
    )

    # 證券商營業員男女人數統計資料
    get__opendata_opendata_brk01 = PythonOperator(
        task_id="get__opendata_opendata_brk01",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/OpenData_BRK01", "method": "GET"},
    )

    # 證券商分公司基本資料
    get__opendata_opendata_brk02 = PythonOperator(
        task_id="get__opendata_opendata_brk02",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/OpenData_BRK02", "method": "GET"},
    )


    get__etfreport_etfrank >> get__brokerservice_secregdata >> get__brokerservice_brokerlist >> get__opendata_t187ap01 >> get__opendata_t187ap20 >> get__opendata_t187ap21 >> get__opendata_t187ap18 >> get__opendata_opendata_brk01 >> get__opendata_opendata_brk02
    # 定義 task 執行順序（平行執行）
    # return [get__etfreport_etfrank, get__brokerservice_secregdata, get__brokerservice_brokerlist, get__opendata_t187ap01, get__opendata_t187ap20, get__opendata_t187ap21, get__opendata_t187ap18, get__opendata_opendata_brk01, get__opendata_opendata_brk02]

# 實例化 DAG
dag = fetch_broker_data()

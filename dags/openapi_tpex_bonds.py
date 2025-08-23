
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
    # collection = db_openapi_tpex["bonds"]

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
    dag_id="openapi_tpex_bonds",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["tpex", "bonds"],
)
def fetch_bonds():
    # 可轉債資產交換ASO及ASW銀行承作餘額
    get__tpex_dpsp_monthly_cbmcs007 = PythonOperator(
        task_id="get__tpex_dpsp_monthly_cbmcs007",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_dpsp_monthly_CBmcs007", "method": "GET"},
    )

    # 國際債券當日盤中報價行情表(含寶島債)
    get__tpex_international_bond_quotes = PythonOperator(
        task_id="get__tpex_international_bond_quotes",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_international_bond_quotes", "method": "GET"},
    )

    # 國際債券當日盤中成交行情表(含寶島債)
    get__tpex_international_bond_trade = PythonOperator(
        task_id="get__tpex_international_bond_trade",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_international_bond_trade", "method": "GET"},
    )

    # 國際債券(一般投資人)
    get__tpex_international_bond_issue_investor = PythonOperator(
        task_id="get__tpex_international_bond_issue_investor",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_international_bond_issue_investor", "method": "GET"},
    )

    # 國際債券(僅售予專業投資人者)
    get__tpex_international_bond_issue_org = PythonOperator(
        task_id="get__tpex_international_bond_issue_org",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_international_bond_issue_org", "method": "GET"},
    )

    # 美元固定利率不可贖回國際債券理論價格
    get__bddos216utf = PythonOperator(
        task_id="get__bddos216utf",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/BDdos216UTF", "method": "GET"},
    )

    # 美元附息固定利率可贖回國際債券理論價格
    get__bddos215utf = PythonOperator(
        task_id="get__bddos215utf",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/BDdos215UTF", "method": "GET"},
    )

    # 美元零息可贖回國際債券理論價格
    get__bddos209utf = PythonOperator(
        task_id="get__bddos209utf",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/BDdos209UTF", "method": "GET"},
    )

    # 轉(交)換公司債買賣斷券商買賣日報表
    get__bond_cb_daily = PythonOperator(
        task_id="get__bond_cb_daily",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/bond_cb_daily", "method": "GET"},
    )

    # 公債發行資料下載
    get__bond_issbd1_data = PythonOperator(
        task_id="get__bond_issbd1_data",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/bond_ISSBD1_data", "method": "GET"},
    )

    # 外國金融債發行資料下載
    get__bond_issbd2_data = PythonOperator(
        task_id="get__bond_issbd2_data",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/bond_ISSBD2_data", "method": "GET"},
    )

    # 金融債發行資料下載
    get__bond_issbd3_data = PythonOperator(
        task_id="get__bond_issbd3_data",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/bond_ISSBD3_data", "method": "GET"},
    )

    # 普通債發行資料下載
    get__bond_issbd4_data = PythonOperator(
        task_id="get__bond_issbd4_data",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/bond_ISSBD4_data", "method": "GET"},
    )

    # 轉(交)換債發行資料下載
    get__bond_issbd5_data = PythonOperator(
        task_id="get__bond_issbd5_data",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/bond_ISSBD5_data", "method": "GET"},
    )

    # 海外轉換債發行資料下載
    get__bond_issbd6_data = PythonOperator(
        task_id="get__bond_issbd6_data",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/bond_ISSBD6_data", "method": "GET"},
    )

    # 附認股權公司債發行資料下載
    get__bond_issbd7_data = PythonOperator(
        task_id="get__bond_issbd7_data",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/bond_ISSBD7_data", "method": "GET"},
    )

    # 海外附認股權公司債發行資料下載
    get__bond_issbd8_data = PythonOperator(
        task_id="get__bond_issbd8_data",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/bond_ISSBD8_data", "method": "GET"},
    )

    # 海外普通債發行資料下載
    get__bond_issbd9_data = PythonOperator(
        task_id="get__bond_issbd9_data",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/bond_ISSBD9_data", "method": "GET"},
    )

    # 國際債券(寶島債券)-本國發行人及第一、二上市(櫃)公司之外國發行人發行資料下載
    get__bond_issbd10_data = PythonOperator(
        task_id="get__bond_issbd10_data",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/bond_ISSBD10_data", "method": "GET"},
    )

    # 國際債券(寶島債券)-外國發行人(在我國未公開發行股權商品者)發行資料下載
    get__bond_issbd11_data = PythonOperator(
        task_id="get__bond_issbd11_data",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/bond_ISSBD11_data", "method": "GET"},
    )


    get__tpex_dpsp_monthly_cbmcs007 >> get__tpex_international_bond_quotes >> get__tpex_international_bond_trade >> get__tpex_international_bond_issue_investor >> get__tpex_international_bond_issue_org >> get__bddos216utf >> get__bddos215utf >> get__bddos209utf >> get__bond_cb_daily >> get__bond_issbd1_data >> get__bond_issbd2_data >> get__bond_issbd3_data >> get__bond_issbd4_data >> get__bond_issbd5_data >> get__bond_issbd6_data >> get__bond_issbd7_data >> get__bond_issbd8_data >> get__bond_issbd9_data >> get__bond_issbd10_data >> get__bond_issbd11_data
    # 定義 task 執行順序（平行執行）
    # return [get__tpex_dpsp_monthly_cbmcs007, get__tpex_international_bond_quotes, get__tpex_international_bond_trade, get__tpex_international_bond_issue_investor, get__tpex_international_bond_issue_org, get__bddos216utf, get__bddos215utf, get__bddos209utf, get__bond_cb_daily, get__bond_issbd1_data, get__bond_issbd2_data, get__bond_issbd3_data, get__bond_issbd4_data, get__bond_issbd5_data, get__bond_issbd6_data, get__bond_issbd7_data, get__bond_issbd8_data, get__bond_issbd9_data, get__bond_issbd10_data, get__bond_issbd11_data]

# 實例化 DAG
dag = fetch_bonds()

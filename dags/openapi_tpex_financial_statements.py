
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
    # collection = db_openapi_tpex["financial_statements"]

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
    dag_id="openapi_tpex_financial_statements",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["tpex", "financial_statements"],
)
def fetch_financial_statements():
    # 上櫃公司資產負債表(金融業)
    get__mopsfin_t187ap07_o_basi = PythonOperator(
        task_id="get__mopsfin_t187ap07_o_basi",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap07_O_basi", "method": "GET"},
    )

    # 上櫃公司資產負債表(證券期貨業)
    get__mopsfin_t187ap07_o_bd = PythonOperator(
        task_id="get__mopsfin_t187ap07_o_bd",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap07_O_bd", "method": "GET"},
    )

    # 上櫃公司資產負債表(一般業)
    get__mopsfin_t187ap07_o_ci = PythonOperator(
        task_id="get__mopsfin_t187ap07_o_ci",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap07_O_ci", "method": "GET"},
    )

    # 上櫃公司資產負債表(金控業)
    get__mopsfin_t187ap07_o_fh = PythonOperator(
        task_id="get__mopsfin_t187ap07_o_fh",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap07_O_fh", "method": "GET"},
    )

    # 上櫃公司資產負債表(保險業)
    get__mopsfin_t187ap07_o_ins = PythonOperator(
        task_id="get__mopsfin_t187ap07_o_ins",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap07_O_ins", "method": "GET"},
    )

    # 上櫃公司資產負債表(異業)
    get__mopsfin_t187ap07_o_mim = PythonOperator(
        task_id="get__mopsfin_t187ap07_o_mim",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap07_O_mim", "method": "GET"},
    )

    # 上櫃公司綜合損益表(金融業)
    get__mopsfin_t187ap06_o_basi = PythonOperator(
        task_id="get__mopsfin_t187ap06_o_basi",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_O_basi", "method": "GET"},
    )

    # 上櫃公司綜合損益表(證券期貨業)
    get__mopsfin_t187ap06_o_bd = PythonOperator(
        task_id="get__mopsfin_t187ap06_o_bd",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_O_bd", "method": "GET"},
    )

    # 上櫃公司綜合損益表(一般業)
    get__mopsfin_t187ap06_o_ci = PythonOperator(
        task_id="get__mopsfin_t187ap06_o_ci",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_O_ci", "method": "GET"},
    )

    # 上櫃公司綜合損益表(金控業)
    get__mopsfin_t187ap06_o_fh = PythonOperator(
        task_id="get__mopsfin_t187ap06_o_fh",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_O_fh", "method": "GET"},
    )

    # 上櫃公司綜合損益表(保險業)
    get__mopsfin_t187ap06_o_ins = PythonOperator(
        task_id="get__mopsfin_t187ap06_o_ins",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_O_ins", "method": "GET"},
    )

    # 上櫃公司綜合損益表(異業)
    get__mopsfin_t187ap06_o_mim = PythonOperator(
        task_id="get__mopsfin_t187ap06_o_mim",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_O_mim", "method": "GET"},
    )

    # 上櫃公司財報資訊(金融業)
    get__mopsfin_t187ap06_o_basia = PythonOperator(
        task_id="get__mopsfin_t187ap06_o_basia",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_O_basiA", "method": "GET"},
    )

    # 上櫃公司財報資訊(證券期貨業)
    get__mopsfin_t187ap06_o_bda = PythonOperator(
        task_id="get__mopsfin_t187ap06_o_bda",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_O_bdA", "method": "GET"},
    )

    # 上櫃公司財報資訊( 一般業)
    get__mopsfin_t187ap06_o_cia = PythonOperator(
        task_id="get__mopsfin_t187ap06_o_cia",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_O_ciA", "method": "GET"},
    )

    # 上櫃公司財報資訊(金控業)
    get__mopsfin_t187ap06_o_fha = PythonOperator(
        task_id="get__mopsfin_t187ap06_o_fha",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_O_fhA", "method": "GET"},
    )

    # 上櫃公司財報資訊(保險業)
    get__mopsfin_t187ap06_o_insa = PythonOperator(
        task_id="get__mopsfin_t187ap06_o_insa",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_O_insA", "method": "GET"},
    )

    # 上櫃公司財報資訊(異業)
    get__mopsfin_t187ap06_o_mima = PythonOperator(
        task_id="get__mopsfin_t187ap06_o_mima",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_O_mimA", "method": "GET"},
    )

    # 興櫃公司資產負債表-證券期貨業
    get__mopsfin_t187ap07_u_bd = PythonOperator(
        task_id="get__mopsfin_t187ap07_u_bd",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap07_U_bd", "method": "GET"},
    )

    # 興櫃公司資產負債表-一般業
    get__mopsfin_t187ap07_u_ci = PythonOperator(
        task_id="get__mopsfin_t187ap07_u_ci",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap07_U_ci", "method": "GET"},
    )

    # 興櫃公司資產負債表-金控業
    get__mopsfin_t187ap07_u_fh = PythonOperator(
        task_id="get__mopsfin_t187ap07_u_fh",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap07_U_fh", "method": "GET"},
    )

    # 興櫃公司資產負債表-保險業
    get__mopsfin_t187ap07_u_ins = PythonOperator(
        task_id="get__mopsfin_t187ap07_u_ins",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap07_U_ins", "method": "GET"},
    )

    # 興櫃公司資產負債表-異業
    get__mopsfin_t187ap07_u_mim = PythonOperator(
        task_id="get__mopsfin_t187ap07_u_mim",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap07_U_mim", "method": "GET"},
    )

    # 興櫃公司綜合損益表-金融業
    get__mopsfin_t187ap06_u_basi = PythonOperator(
        task_id="get__mopsfin_t187ap06_u_basi",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_U_basi", "method": "GET"},
    )

    # 興櫃公司綜合損益表-證券期貨業
    get__mopsfin_t187ap06_u_bd = PythonOperator(
        task_id="get__mopsfin_t187ap06_u_bd",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_U_bd", "method": "GET"},
    )

    # 興櫃公司綜合損益表-一般業
    get__mopsfin_t187ap06_u_ci = PythonOperator(
        task_id="get__mopsfin_t187ap06_u_ci",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_U_ci", "method": "GET"},
    )

    # 興櫃公司綜合損益表-金控業
    get__mopsfin_t187ap06_u_fh = PythonOperator(
        task_id="get__mopsfin_t187ap06_u_fh",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_U_fh", "method": "GET"},
    )

    # 興櫃公司綜合損益表-保險業
    get__mopsfin_t187ap06_u_ins = PythonOperator(
        task_id="get__mopsfin_t187ap06_u_ins",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_U_ins", "method": "GET"},
    )

    # 興櫃公司綜合損益表-異業
    get__mopsfin_t187ap06_u_mim = PythonOperator(
        task_id="get__mopsfin_t187ap06_u_mim",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap06_U_mim", "method": "GET"},
    )

    # 興櫃公司資產負債表-金融業
    get__mopsfin_t187ap07_u_basi = PythonOperator(
        task_id="get__mopsfin_t187ap07_u_basi",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap07_U_basi", "method": "GET"},
    )

    # 上櫃公司截至各季綜合損益財測達成情形(簡式)
    get__mopsfin_t187ap15_o = PythonOperator(
        task_id="get__mopsfin_t187ap15_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap15_O", "method": "GET"},
    )

    # 上櫃公司當季綜合損益經會計師查核(核閱)數與當季預測數差異達百分之十以上者，或截至當季累計差異達百分之二十以上者(簡式)
    get__mopsfin_t187ap16_o = PythonOperator(
        task_id="get__mopsfin_t187ap16_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap16_O", "method": "GET"},
    )

    # 上櫃公司營益分析查詢彙總表(全體公司彙總報表)
    get__mopsfin_187ap17_o = PythonOperator(
        task_id="get__mopsfin_187ap17_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_187ap17_O", "method": "GET"},
    )


    get__mopsfin_t187ap07_o_basi >> get__mopsfin_t187ap07_o_bd >> get__mopsfin_t187ap07_o_ci >> get__mopsfin_t187ap07_o_fh >> get__mopsfin_t187ap07_o_ins >> get__mopsfin_t187ap07_o_mim >> get__mopsfin_t187ap06_o_basi >> get__mopsfin_t187ap06_o_bd >> get__mopsfin_t187ap06_o_ci >> get__mopsfin_t187ap06_o_fh >> get__mopsfin_t187ap06_o_ins >> get__mopsfin_t187ap06_o_mim >> get__mopsfin_t187ap06_o_basia >> get__mopsfin_t187ap06_o_bda >> get__mopsfin_t187ap06_o_cia >> get__mopsfin_t187ap06_o_fha >> get__mopsfin_t187ap06_o_insa >> get__mopsfin_t187ap06_o_mima >> get__mopsfin_t187ap07_u_bd >> get__mopsfin_t187ap07_u_ci >> get__mopsfin_t187ap07_u_fh >> get__mopsfin_t187ap07_u_ins >> get__mopsfin_t187ap07_u_mim >> get__mopsfin_t187ap06_u_basi >> get__mopsfin_t187ap06_u_bd >> get__mopsfin_t187ap06_u_ci >> get__mopsfin_t187ap06_u_fh >> get__mopsfin_t187ap06_u_ins >> get__mopsfin_t187ap06_u_mim >> get__mopsfin_t187ap07_u_basi >> get__mopsfin_t187ap15_o >> get__mopsfin_t187ap16_o >> get__mopsfin_187ap17_o
    # 定義 task 執行順序（平行執行）
    # return [get__mopsfin_t187ap07_o_basi, get__mopsfin_t187ap07_o_bd, get__mopsfin_t187ap07_o_ci, get__mopsfin_t187ap07_o_fh, get__mopsfin_t187ap07_o_ins, get__mopsfin_t187ap07_o_mim, get__mopsfin_t187ap06_o_basi, get__mopsfin_t187ap06_o_bd, get__mopsfin_t187ap06_o_ci, get__mopsfin_t187ap06_o_fh, get__mopsfin_t187ap06_o_ins, get__mopsfin_t187ap06_o_mim, get__mopsfin_t187ap06_o_basia, get__mopsfin_t187ap06_o_bda, get__mopsfin_t187ap06_o_cia, get__mopsfin_t187ap06_o_fha, get__mopsfin_t187ap06_o_insa, get__mopsfin_t187ap06_o_mima, get__mopsfin_t187ap07_u_bd, get__mopsfin_t187ap07_u_ci, get__mopsfin_t187ap07_u_fh, get__mopsfin_t187ap07_u_ins, get__mopsfin_t187ap07_u_mim, get__mopsfin_t187ap06_u_basi, get__mopsfin_t187ap06_u_bd, get__mopsfin_t187ap06_u_ci, get__mopsfin_t187ap06_u_fh, get__mopsfin_t187ap06_u_ins, get__mopsfin_t187ap06_u_mim, get__mopsfin_t187ap07_u_basi, get__mopsfin_t187ap15_o, get__mopsfin_t187ap16_o, get__mopsfin_187ap17_o]

# 實例化 DAG
dag = fetch_financial_statements()

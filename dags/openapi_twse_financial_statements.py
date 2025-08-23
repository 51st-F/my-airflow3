
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
    # collection = db_openapi_twse["financial_statements"]

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
    dag_id="openapi_twse_financial_statements",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["twse", "financial_statements"],
)
def fetch_financial_statements():
    # 公發公司資產負債表-一般業
    get__opendata_t187ap07_x_ci = PythonOperator(
        task_id="get__opendata_t187ap07_x_ci",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap07_X_ci", "method": "GET"},
    )

    # 公發公司資產負債表-異業
    get__opendata_t187ap07_x_mim = PythonOperator(
        task_id="get__opendata_t187ap07_x_mim",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap07_X_mim", "method": "GET"},
    )

    # 公發公司綜合損益表-金融業
    get__opendata_t187ap06_x_basi = PythonOperator(
        task_id="get__opendata_t187ap06_x_basi",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap06_X_basi", "method": "GET"},
    )

    # 公發公司綜合損益表-證券期貨業
    get__opendata_t187ap06_x_bd = PythonOperator(
        task_id="get__opendata_t187ap06_x_bd",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap06_X_bd", "method": "GET"},
    )

    # 公發公司綜合損益表-一般業
    get__opendata_t187ap06_x_ci = PythonOperator(
        task_id="get__opendata_t187ap06_x_ci",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap06_X_ci", "method": "GET"},
    )

    # 公發公司綜合損益表-金控業
    get__opendata_t187ap06_x_fh = PythonOperator(
        task_id="get__opendata_t187ap06_x_fh",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap06_X_fh", "method": "GET"},
    )

    # 公發公司綜合損益表-保險業
    get__opendata_t187ap06_x_ins = PythonOperator(
        task_id="get__opendata_t187ap06_x_ins",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap06_X_ins", "method": "GET"},
    )

    # 公發公司綜合損益表-異業
    get__opendata_t187ap06_x_mim = PythonOperator(
        task_id="get__opendata_t187ap06_x_mim",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap06_X_mim", "method": "GET"},
    )

    # 上市公司每月營業收入彙總表
    get__opendata_t187ap05_l = PythonOperator(
        task_id="get__opendata_t187ap05_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap05_L", "method": "GET"},
    )

    # 上市公司截至各季綜合損益財測達成情形(簡式)
    get__opendata_t187ap15_l = PythonOperator(
        task_id="get__opendata_t187ap15_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap15_L", "method": "GET"},
    )

    # 上市公司當季綜合損益經會計師查核(核閱)數與當季預測數差異達百分之十以上者，或截至當季累計差異達百分之二十以上者(簡式)
    get__opendata_t187ap16_l = PythonOperator(
        task_id="get__opendata_t187ap16_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap16_L", "method": "GET"},
    )

    # 上市公司營益分析查詢彙總表(全體公司彙總報表)
    get__opendata_t187ap17_l = PythonOperator(
        task_id="get__opendata_t187ap17_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap17_L", "method": "GET"},
    )

    # 上市公司財務報告經監察人承認情形
    get__opendata_t187ap31_l = PythonOperator(
        task_id="get__opendata_t187ap31_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap31_L", "method": "GET"},
    )

    # 上市公司綜合損益表(證券期貨業)
    get__opendata_t187ap06_l_bd = PythonOperator(
        task_id="get__opendata_t187ap06_l_bd",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap06_L_bd", "method": "GET"},
    )

    # 上市公司綜合損益表(一般業)
    get__opendata_t187ap06_l_ci = PythonOperator(
        task_id="get__opendata_t187ap06_l_ci",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap06_L_ci", "method": "GET"},
    )

    # 上市公司綜合損益表(金控業)
    get__opendata_t187ap06_l_fh = PythonOperator(
        task_id="get__opendata_t187ap06_l_fh",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap06_L_fh", "method": "GET"},
    )

    # 上市公司綜合損益表(保險業)
    get__opendata_t187ap06_l_ins = PythonOperator(
        task_id="get__opendata_t187ap06_l_ins",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap06_L_ins", "method": "GET"},
    )

    # 上市公司綜合損益表(異業)
    get__opendata_t187ap06_l_mim = PythonOperator(
        task_id="get__opendata_t187ap06_l_mim",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap06_L_mim", "method": "GET"},
    )

    # 上市公司資產負債表(證券期貨業)
    get__opendata_t187ap07_l_bd = PythonOperator(
        task_id="get__opendata_t187ap07_l_bd",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap07_L_bd", "method": "GET"},
    )

    # 上市公司資產負債表(一般業)
    get__opendata_t187ap07_l_ci = PythonOperator(
        task_id="get__opendata_t187ap07_l_ci",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap07_L_ci", "method": "GET"},
    )

    # 上市公司資產負債表(金控業)
    get__opendata_t187ap07_l_fh = PythonOperator(
        task_id="get__opendata_t187ap07_l_fh",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap07_L_fh", "method": "GET"},
    )

    # 上市公司資產負債表(保險業)
    get__opendata_t187ap07_l_ins = PythonOperator(
        task_id="get__opendata_t187ap07_l_ins",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap07_L_ins", "method": "GET"},
    )

    # 上市公司資產負債表(異業)
    get__opendata_t187ap07_l_mim = PythonOperator(
        task_id="get__opendata_t187ap07_l_mim",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap07_L_mim", "method": "GET"},
    )

    # 公發公司資產負債表-金融業
    get__opendata_t187ap07_x_basi = PythonOperator(
        task_id="get__opendata_t187ap07_x_basi",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap07_X_basi", "method": "GET"},
    )

    # 公發公司資產負債表-證券期貨業
    get__opendata_t187ap07_x_bd = PythonOperator(
        task_id="get__opendata_t187ap07_x_bd",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap07_X_bd", "method": "GET"},
    )

    # 公發公司資產負債表-金控業
    get__opendata_t187ap07_x_fh = PythonOperator(
        task_id="get__opendata_t187ap07_x_fh",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap07_X_fh", "method": "GET"},
    )

    # 公發公司資產負債表-保險業
    get__opendata_t187ap07_x_ins = PythonOperator(
        task_id="get__opendata_t187ap07_x_ins",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap07_X_ins", "method": "GET"},
    )

    # 公發公司董監事持股餘額明細
    get__opendata_t187ap11_p = PythonOperator(
        task_id="get__opendata_t187ap11_p",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap11_P", "method": "GET"},
    )

    # 上市公司綜合損益表(金融業)
    get__opendata_t187ap06_l_basi = PythonOperator(
        task_id="get__opendata_t187ap06_l_basi",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap06_L_basi", "method": "GET"},
    )

    # 上市公司資產負債表(金融業)
    get__opendata_t187ap07_l_basi = PythonOperator(
        task_id="get__opendata_t187ap07_l_basi",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap07_L_basi", "method": "GET"},
    )


    get__opendata_t187ap07_x_ci >> get__opendata_t187ap07_x_mim >> get__opendata_t187ap06_x_basi >> get__opendata_t187ap06_x_bd >> get__opendata_t187ap06_x_ci >> get__opendata_t187ap06_x_fh >> get__opendata_t187ap06_x_ins >> get__opendata_t187ap06_x_mim >> get__opendata_t187ap05_l >> get__opendata_t187ap15_l >> get__opendata_t187ap16_l >> get__opendata_t187ap17_l >> get__opendata_t187ap31_l >> get__opendata_t187ap06_l_bd >> get__opendata_t187ap06_l_ci >> get__opendata_t187ap06_l_fh >> get__opendata_t187ap06_l_ins >> get__opendata_t187ap06_l_mim >> get__opendata_t187ap07_l_bd >> get__opendata_t187ap07_l_ci >> get__opendata_t187ap07_l_fh >> get__opendata_t187ap07_l_ins >> get__opendata_t187ap07_l_mim >> get__opendata_t187ap07_x_basi >> get__opendata_t187ap07_x_bd >> get__opendata_t187ap07_x_fh >> get__opendata_t187ap07_x_ins >> get__opendata_t187ap11_p >> get__opendata_t187ap06_l_basi >> get__opendata_t187ap07_l_basi
    # 定義 task 執行順序（平行執行）
    # return [get__opendata_t187ap07_x_ci, get__opendata_t187ap07_x_mim, get__opendata_t187ap06_x_basi, get__opendata_t187ap06_x_bd, get__opendata_t187ap06_x_ci, get__opendata_t187ap06_x_fh, get__opendata_t187ap06_x_ins, get__opendata_t187ap06_x_mim, get__opendata_t187ap05_l, get__opendata_t187ap15_l, get__opendata_t187ap16_l, get__opendata_t187ap17_l, get__opendata_t187ap31_l, get__opendata_t187ap06_l_bd, get__opendata_t187ap06_l_ci, get__opendata_t187ap06_l_fh, get__opendata_t187ap06_l_ins, get__opendata_t187ap06_l_mim, get__opendata_t187ap07_l_bd, get__opendata_t187ap07_l_ci, get__opendata_t187ap07_l_fh, get__opendata_t187ap07_l_ins, get__opendata_t187ap07_l_mim, get__opendata_t187ap07_x_basi, get__opendata_t187ap07_x_bd, get__opendata_t187ap07_x_fh, get__opendata_t187ap07_x_ins, get__opendata_t187ap11_p, get__opendata_t187ap06_l_basi, get__opendata_t187ap07_l_basi]

# 實例化 DAG
dag = fetch_financial_statements()

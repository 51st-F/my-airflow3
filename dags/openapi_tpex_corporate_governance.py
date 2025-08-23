
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
    # collection = db_openapi_tpex["corporate_governance"]

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
    dag_id="openapi_tpex_corporate_governance",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["tpex", "corporate_governance"],
)
def fetch_corporate_governance():
    # 本國興櫃公司EPS排名
    get__tpex_esb_eps_rank = PythonOperator(
        task_id="get__tpex_esb_eps_rank",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_esb_eps_rank", "method": "GET"},
    )

    # 興櫃公司資本額排名
    get__tpex_esb_capitals_rank = PythonOperator(
        task_id="get__tpex_esb_capitals_rank",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_esb_capitals_rank", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-職業安全衛生
    get__t187ap46_o_21 = PythonOperator(
        task_id="get__t187ap46_o_21",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_21", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-功能性委員會
    get__t187ap46_o_9 = PythonOperator(
        task_id="get__t187ap46_o_9",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_9", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-氣候相關議題管理
    get__t187ap46_o_8 = PythonOperator(
        task_id="get__t187ap46_o_8",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_8", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-反競爭行為法律訴訟 
    get__t187ap46_o_20 = PythonOperator(
        task_id="get__t187ap46_o_20",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_20", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-風險管理政策 
    get__t187ap46_o_19 = PythonOperator(
        task_id="get__t187ap46_o_19",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_19", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-社區關係 
    get__t187ap46_o_15 = PythonOperator(
        task_id="get__t187ap46_o_15",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_15", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-供應鏈管理 
    get__t187ap46_o_13 = PythonOperator(
        task_id="get__t187ap46_o_13",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_13", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-食品安全
    get__t187ap46_o_12 = PythonOperator(
        task_id="get__t187ap46_o_12",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_12", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-產品品質與安全
    get__t187ap46_o_14 = PythonOperator(
        task_id="get__t187ap46_o_14",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_14", "method": "GET"},
    )

    # 上櫃公司召開股東常 (臨時) 會日期、地點及採用電子投票情形等資料彙總表
    get__t187ap41_o = PythonOperator(
        task_id="get__t187ap41_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap41_O", "method": "GET"},
    )

    # 興櫃公司每月營業收入彙總表
    get__t187ap05_r = PythonOperator(
        task_id="get__t187ap05_r",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap05_R", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-廢棄物管理
    get__t187ap46_o_4 = PythonOperator(
        task_id="get__t187ap46_o_4",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_4", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-能源管理
    get__t187ap46_o_2 = PythonOperator(
        task_id="get__t187ap46_o_2",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_2", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-投資人溝通
    get__t187ap46_o_7 = PythonOperator(
        task_id="get__t187ap46_o_7",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_7", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-溫室氣體排放
    get__t187ap46_o_1 = PythonOperator(
        task_id="get__t187ap46_o_1",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_1", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-董事會
    get__t187ap46_o_6 = PythonOperator(
        task_id="get__t187ap46_o_6",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_6", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-人力發展
    get__t187ap46_o_5 = PythonOperator(
        task_id="get__t187ap46_o_5",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_5", "method": "GET"},
    )

    # 上櫃公司企業ESG資訊揭露彙總資料-水資源管理
    get__t187ap46_o_3 = PythonOperator(
        task_id="get__t187ap46_o_3",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/t187ap46_O_3", "method": "GET"},
    )

    # 二十九大類股營收變化統計表
    get__mopsfin_t187ap05_oa = PythonOperator(
        task_id="get__mopsfin_t187ap05_oa",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap05_OA", "method": "GET"},
    )

    # 發行公司營收創新高一覽表(上櫃)
    get__mopsfin_t187ap05_ob = PythonOperator(
        task_id="get__mopsfin_t187ap05_ob",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap05_OB", "method": "GET"},
    )

    # 上櫃公司股東行使提案權情形彙總表
    get__mopsfin_t187ap35_o = PythonOperator(
        task_id="get__mopsfin_t187ap35_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap35_O", "method": "GET"},
    )

    # 上櫃公司公司治理之相關規程規則
    get__mopsfin_t187ap32_o = PythonOperator(
        task_id="get__mopsfin_t187ap32_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap32_O", "method": "GET"},
    )

    # 上櫃公司採累積投票制、全額連記法、候選人提名制選任董監事及當選資料彙總表
    get__mopsfin_t187ap34_o = PythonOperator(
        task_id="get__mopsfin_t187ap34_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap34_O", "method": "GET"},
    )

    # 上櫃公司董事長是否兼任總經理
    get__mopsfin_t187ap33_o = PythonOperator(
        task_id="get__mopsfin_t187ap33_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap33_O", "method": "GET"},
    )

    # 上櫃公司財務報告經監察人承認情形
    get__mopsfin_t187ap31_o = PythonOperator(
        task_id="get__mopsfin_t187ap31_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap31_O", "method": "GET"},
    )

    # 上櫃公司董事、監察人質權設定占董事及監察人實際持有股數彙總表
    get__mopsfin_t187ap09_o = PythonOperator(
        task_id="get__mopsfin_t187ap09_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap09_O", "method": "GET"},
    )

    # 上櫃公司董事、監察人持股不足法定成數連續達3個月以上彙總表
    get__mopsfin_t187ap10_o = PythonOperator(
        task_id="get__mopsfin_t187ap10_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap10_O", "method": "GET"},
    )

    # 上櫃公司經營權及營業範圍異(變)動專區-經營權異動公司
    get__mopsfin_t187ap24_o = PythonOperator(
        task_id="get__mopsfin_t187ap24_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap24_O", "method": "GET"},
    )

    # 上櫃公司經營權及營業範圍異(變)動專區-營業範圍重大變更公司
    get__mopsfin_t187ap25_o = PythonOperator(
        task_id="get__mopsfin_t187ap25_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap25_O", "method": "GET"},
    )

    # 上櫃股票基本資料
    get__mopsfin_t187ap03_o = PythonOperator(
        task_id="get__mopsfin_t187ap03_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap03_O", "method": "GET"},
    )

    # 興櫃公司基本資料
    get__mopsfin_t187ap03_r = PythonOperator(
        task_id="get__mopsfin_t187ap03_r",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap03_R", "method": "GET"},
    )

    # 上櫃公司每日重大訊息
    get__mopsfin_t187ap04_o = PythonOperator(
        task_id="get__mopsfin_t187ap04_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap04_O", "method": "GET"},
    )

    # 上櫃公司持股逾 10% 大股東名單
    get__mopsfin_t187ap02_o = PythonOperator(
        task_id="get__mopsfin_t187ap02_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap02_O", "method": "GET"},
    )

    # 上櫃公司經營權及營業範圍異(變)動專區-經營權異動且營業範圍重大變更停止買賣公司
    get__mopsfin_t187ap26_o = PythonOperator(
        task_id="get__mopsfin_t187ap26_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap26_O", "method": "GET"},
    )

    # 上櫃公司經營權及營業範圍異(變)動專區-經營權異動且營業範圍重大變更列為變更交易公司
    get__mopsfin_t187ap27_o = PythonOperator(
        task_id="get__mopsfin_t187ap27_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap27_O", "method": "GET"},
    )

    # 上櫃公司每月營業收入彙總表
    get__mopsfin_t187ap05_o = PythonOperator(
        task_id="get__mopsfin_t187ap05_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap05_O", "method": "GET"},
    )

    # 上櫃股利分派情形-董事會通過
    get__mopsfin_t187ap39_o = PythonOperator(
        task_id="get__mopsfin_t187ap39_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap39_O", "method": "GET"},
    )

    # 興櫃公司董監事持股餘額明細資料
    get__mopsfin_t187ap11_r = PythonOperator(
        task_id="get__mopsfin_t187ap11_r",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap11_R", "method": "GET"},
    )

    # 上櫃公司各產業EPS統計資訊
    get__mopsfin_t187ap14_o = PythonOperator(
        task_id="get__mopsfin_t187ap14_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap14_O", "method": "GET"},
    )

    # 上櫃公司董事、監察人持股不足法定成數彙總表
    get__mopsfin_t187ap08_o = PythonOperator(
        task_id="get__mopsfin_t187ap08_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap08_O", "method": "GET"},
    )

    # 上櫃公司董監事持股餘額明細資料
    get__mopsfin_t187ap11_o = PythonOperator(
        task_id="get__mopsfin_t187ap11_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap11_O", "method": "GET"},
    )

    # 上櫃公司每日內部人持股轉讓事前申報表-持股轉讓日報表
    get__mopsfin_t187ap12_o = PythonOperator(
        task_id="get__mopsfin_t187ap12_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap12_O", "method": "GET"},
    )

    # 上櫃公司每日內部人持股轉讓事前申報表-持股未轉讓日報表
    get__mopsfin_t187ap13_o = PythonOperator(
        task_id="get__mopsfin_t187ap13_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap13_O", "method": "GET"},
    )

    # 上櫃公司金管會證券期貨局裁罰案件專區
    get__mopsfin_t187ap22_o = PythonOperator(
        task_id="get__mopsfin_t187ap22_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap22_O", "method": "GET"},
    )

    # 上櫃公司獨立董監事兼任情形彙總表
    get__mopsfin_t187ap30_o = PythonOperator(
        task_id="get__mopsfin_t187ap30_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap30_O", "method": "GET"},
    )

    # 上櫃公司董事酬金相關資訊
    get__mopsfin_t187ap29_a_o = PythonOperator(
        task_id="get__mopsfin_t187ap29_a_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap29_A_O", "method": "GET"},
    )

    # 上櫃公司監察人酬金相關資訊
    get__mopsfin_t187ap29_b_o = PythonOperator(
        task_id="get__mopsfin_t187ap29_b_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap29_B_O", "method": "GET"},
    )

    # 上櫃公司合併報表董事酬金相關資訊
    get__mopsfin_t187ap29_c_o = PythonOperator(
        task_id="get__mopsfin_t187ap29_c_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap29_C_O", "method": "GET"},
    )

    # 上櫃公司合併報表監察人酬金相關資訊
    get__mopsfin_t187ap29_d_o = PythonOperator(
        task_id="get__mopsfin_t187ap29_d_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap29_D_O", "method": "GET"},
    )

    # 上櫃公司違反資訊申報、重大訊息及說明記者會規定專區
    get__mopsfin_t187ap23_o = PythonOperator(
        task_id="get__mopsfin_t187ap23_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap23_O", "method": "GET"},
    )


    get__tpex_esb_eps_rank >> get__tpex_esb_capitals_rank >> get__t187ap46_o_21 >> get__t187ap46_o_9 >> get__t187ap46_o_8 >> get__t187ap46_o_20 >> get__t187ap46_o_19 >> get__t187ap46_o_15 >> get__t187ap46_o_13 >> get__t187ap46_o_12 >> get__t187ap46_o_14 >> get__t187ap41_o >> get__t187ap05_r >> get__t187ap46_o_4 >> get__t187ap46_o_2 >> get__t187ap46_o_7 >> get__t187ap46_o_1 >> get__t187ap46_o_6 >> get__t187ap46_o_5 >> get__t187ap46_o_3 >> get__mopsfin_t187ap05_oa >> get__mopsfin_t187ap05_ob >> get__mopsfin_t187ap35_o >> get__mopsfin_t187ap32_o >> get__mopsfin_t187ap34_o >> get__mopsfin_t187ap33_o >> get__mopsfin_t187ap31_o >> get__mopsfin_t187ap09_o >> get__mopsfin_t187ap10_o >> get__mopsfin_t187ap24_o >> get__mopsfin_t187ap25_o >> get__mopsfin_t187ap03_o >> get__mopsfin_t187ap03_r >> get__mopsfin_t187ap04_o >> get__mopsfin_t187ap02_o >> get__mopsfin_t187ap26_o >> get__mopsfin_t187ap27_o >> get__mopsfin_t187ap05_o >> get__mopsfin_t187ap39_o >> get__mopsfin_t187ap11_r >> get__mopsfin_t187ap14_o >> get__mopsfin_t187ap08_o >> get__mopsfin_t187ap11_o >> get__mopsfin_t187ap12_o >> get__mopsfin_t187ap13_o >> get__mopsfin_t187ap22_o >> get__mopsfin_t187ap30_o >> get__mopsfin_t187ap29_a_o >> get__mopsfin_t187ap29_b_o >> get__mopsfin_t187ap29_c_o >> get__mopsfin_t187ap29_d_o >> get__mopsfin_t187ap23_o
    # 定義 task 執行順序（平行執行）
    # return [get__tpex_esb_eps_rank, get__tpex_esb_capitals_rank, get__t187ap46_o_21, get__t187ap46_o_9, get__t187ap46_o_8, get__t187ap46_o_20, get__t187ap46_o_19, get__t187ap46_o_15, get__t187ap46_o_13, get__t187ap46_o_12, get__t187ap46_o_14, get__t187ap41_o, get__t187ap05_r, get__t187ap46_o_4, get__t187ap46_o_2, get__t187ap46_o_7, get__t187ap46_o_1, get__t187ap46_o_6, get__t187ap46_o_5, get__t187ap46_o_3, get__mopsfin_t187ap05_oa, get__mopsfin_t187ap05_ob, get__mopsfin_t187ap35_o, get__mopsfin_t187ap32_o, get__mopsfin_t187ap34_o, get__mopsfin_t187ap33_o, get__mopsfin_t187ap31_o, get__mopsfin_t187ap09_o, get__mopsfin_t187ap10_o, get__mopsfin_t187ap24_o, get__mopsfin_t187ap25_o, get__mopsfin_t187ap03_o, get__mopsfin_t187ap03_r, get__mopsfin_t187ap04_o, get__mopsfin_t187ap02_o, get__mopsfin_t187ap26_o, get__mopsfin_t187ap27_o, get__mopsfin_t187ap05_o, get__mopsfin_t187ap39_o, get__mopsfin_t187ap11_r, get__mopsfin_t187ap14_o, get__mopsfin_t187ap08_o, get__mopsfin_t187ap11_o, get__mopsfin_t187ap12_o, get__mopsfin_t187ap13_o, get__mopsfin_t187ap22_o, get__mopsfin_t187ap30_o, get__mopsfin_t187ap29_a_o, get__mopsfin_t187ap29_b_o, get__mopsfin_t187ap29_c_o, get__mopsfin_t187ap29_d_o, get__mopsfin_t187ap23_o]

# 實例化 DAG
dag = fetch_corporate_governance()

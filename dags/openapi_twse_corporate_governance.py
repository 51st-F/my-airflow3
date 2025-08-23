
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
    # collection = db_openapi_twse["corporate_governance"]

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
    dag_id="openapi_twse_corporate_governance",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["twse", "corporate_governance"],
)
def fetch_corporate_governance():
    # 上市公司股利分派情形
    get__opendata_t187ap45_l = PythonOperator(
        task_id="get__opendata_t187ap45_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap45_L", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-反競爭行為法律訴訟
    get__opendata_t187ap46_l_20 = PythonOperator(
        task_id="get__opendata_t187ap46_l_20",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_20", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-風險管理政策
    get__opendata_t187ap46_l_19 = PythonOperator(
        task_id="get__opendata_t187ap46_l_19",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_19", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-持股及控制力
    get__opendata_t187ap46_l_18 = PythonOperator(
        task_id="get__opendata_t187ap46_l_18",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_18", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-普惠金融
    get__opendata_t187ap46_l_17 = PythonOperator(
        task_id="get__opendata_t187ap46_l_17",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_17", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-資訊安全
    get__opendata_t187ap46_l_16 = PythonOperator(
        task_id="get__opendata_t187ap46_l_16",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_16", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-社區關係
    get__opendata_t187ap46_l_15 = PythonOperator(
        task_id="get__opendata_t187ap46_l_15",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_15", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-產品品質與安全
    get__opendata_t187ap46_l_14 = PythonOperator(
        task_id="get__opendata_t187ap46_l_14",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_14", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-供應鏈管理
    get__opendata_t187ap46_l_13 = PythonOperator(
        task_id="get__opendata_t187ap46_l_13",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_13", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-食品安全
    get__opendata_t187ap46_l_12 = PythonOperator(
        task_id="get__opendata_t187ap46_l_12",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_12", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-產品生命週期
    get__opendata_t187ap46_l_11 = PythonOperator(
        task_id="get__opendata_t187ap46_l_11",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_11", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-燃料管理
    get__opendata_t187ap46_l_10 = PythonOperator(
        task_id="get__opendata_t187ap46_l_10",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_10", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-功能性委員會
    get__opendata_t187ap46_l_9 = PythonOperator(
        task_id="get__opendata_t187ap46_l_9",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_9", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-氣候相關議題管理
    get__opendata_t187ap46_l_8 = PythonOperator(
        task_id="get__opendata_t187ap46_l_8",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_8", "method": "GET"},
    )

    # 公開發行公司每月營業收入彙總表
    get__opendata_t187ap05_p = PythonOperator(
        task_id="get__opendata_t187ap05_p",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap05_P", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-投資人溝通
    get__opendata_t187ap46_l_7 = PythonOperator(
        task_id="get__opendata_t187ap46_l_7",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_7", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-董事會
    get__opendata_t187ap46_l_6 = PythonOperator(
        task_id="get__opendata_t187ap46_l_6",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_6", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-人力發展
    get__opendata_t187ap46_l_5 = PythonOperator(
        task_id="get__opendata_t187ap46_l_5",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_5", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-廢棄物管理
    get__opendata_t187ap46_l_4 = PythonOperator(
        task_id="get__opendata_t187ap46_l_4",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_4", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-水資源管理
    get__opendata_t187ap46_l_3 = PythonOperator(
        task_id="get__opendata_t187ap46_l_3",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_3", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-能源管理
    get__opendata_t187ap46_l_2 = PythonOperator(
        task_id="get__opendata_t187ap46_l_2",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_2", "method": "GET"},
    )

    # 上市公司企業ESG資訊揭露彙總資料-溫室氣體排放
    get__opendata_t187ap46_l_1 = PythonOperator(
        task_id="get__opendata_t187ap46_l_1",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap46_L_1", "method": "GET"},
    )

    # 外國公司向證交所申請第一上市之公司
    get__company_applylistingforeign = PythonOperator(
        task_id="get__company_applylistingforeign",
        python_callable=call_twse_api,
        op_kwargs={"path": "/company/applylistingForeign", "method": "GET"},
    )

    # 最近上市公司
    get__company_newlisting = PythonOperator(
        task_id="get__company_newlisting",
        python_callable=call_twse_api,
        op_kwargs={"path": "/company/newlisting", "method": "GET"},
    )

    # 終止上市公司
    get__company_suspendlistingcsvandhtml = PythonOperator(
        task_id="get__company_suspendlistingcsvandhtml",
        python_callable=call_twse_api,
        op_kwargs={"path": "/company/suspendListingCsvAndHtml", "method": "GET"},
    )

    # 申請上市之本國公司
    get__company_applylistinglocal = PythonOperator(
        task_id="get__company_applylistinglocal",
        python_callable=call_twse_api,
        op_kwargs={"path": "/company/applylistingLocal", "method": "GET"},
    )

    # 上市公司每日重大訊息
    get__opendata_t187ap04_l = PythonOperator(
        task_id="get__opendata_t187ap04_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap04_L", "method": "GET"},
    )

    # 上市公司基本資料
    get__opendata_t187ap03_l = PythonOperator(
        task_id="get__opendata_t187ap03_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap03_L", "method": "GET"},
    )

    # 上市公司持股逾 10% 大股東名單
    get__opendata_t187ap02_l = PythonOperator(
        task_id="get__opendata_t187ap02_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap02_L", "method": "GET"},
    )

    # 上市公司各產業EPS統計資訊
    get__opendata_t187ap14_l = PythonOperator(
        task_id="get__opendata_t187ap14_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap14_L", "method": "GET"},
    )

    # 上市公司董事、監察人持股不足法定成數彙總表
    get__opendata_t187ap08_l = PythonOperator(
        task_id="get__opendata_t187ap08_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap08_L", "method": "GET"},
    )

    # 上市公司董監事持股餘額明細資料
    get__opendata_t187ap11_l = PythonOperator(
        task_id="get__opendata_t187ap11_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap11_L", "method": "GET"},
    )

    # 上市公司每日內部人持股轉讓事前申報表-持股轉讓日報表
    get__opendata_t187ap12_l = PythonOperator(
        task_id="get__opendata_t187ap12_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap12_L", "method": "GET"},
    )

    # 上市公司每日內部人持股轉讓事前申報表-持股未轉讓日報表
    get__opendata_t187ap13_l = PythonOperator(
        task_id="get__opendata_t187ap13_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap13_L", "method": "GET"},
    )

    # 上市公司金管會證券期貨局裁罰案件專區
    get__opendata_t187ap22_l = PythonOperator(
        task_id="get__opendata_t187ap22_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap22_L", "method": "GET"},
    )

    # 上市公司獨立董監事兼任情形彙總表
    get__opendata_t187ap30_l = PythonOperator(
        task_id="get__opendata_t187ap30_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap30_L", "method": "GET"},
    )

    # 上市公司董事酬金相關資訊 
    get__opendata_t187ap29_a_l = PythonOperator(
        task_id="get__opendata_t187ap29_a_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap29_A_L", "method": "GET"},
    )

    # 上市公司監察人酬金相關資訊 
    get__opendata_t187ap29_b_l = PythonOperator(
        task_id="get__opendata_t187ap29_b_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap29_B_L", "method": "GET"},
    )

    # 上市公司合併報表董事酬金相關資訊 
    get__opendata_t187ap29_c_l = PythonOperator(
        task_id="get__opendata_t187ap29_c_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap29_C_L", "method": "GET"},
    )

    # 上市公司合併報表監察人酬金相關資訊 
    get__opendata_t187ap29_d_l = PythonOperator(
        task_id="get__opendata_t187ap29_d_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap29_D_L", "method": "GET"},
    )

    # 上市公司違反資訊申報、重大訊息及說明記者會規定專區
    get__opendata_t187ap23_l = PythonOperator(
        task_id="get__opendata_t187ap23_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap23_L", "method": "GET"},
    )

    # 上市公司 103 年應編製與申報 CSR 報告書名單
    get__static_20151104_csr103 = PythonOperator(
        task_id="get__static_20151104_csr103",
        python_callable=call_twse_api,
        op_kwargs={"path": "/static/20151104/CSR103", "method": "GET"},
    )

    # 公開發行公司基本資料
    get__opendata_t187ap03_p = PythonOperator(
        task_id="get__opendata_t187ap03_p",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap03_P", "method": "GET"},
    )

    # 集中市場公布處置股票
    get__announcement_punish = PythonOperator(
        task_id="get__announcement_punish",
        python_callable=call_twse_api,
        op_kwargs={"path": "/announcement/punish", "method": "GET"},
    )

    # 上市公司董事、監察人持股不足法定成數連續達3個月以上彙總表
    get__opendata_t187ap10_l = PythonOperator(
        task_id="get__opendata_t187ap10_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap10_L", "method": "GET"},
    )

    # 上市公司股東會公告-召集股東常(臨時)會公告資料彙總表(95年度起適用)
    get__opendata_t187ap38_l = PythonOperator(
        task_id="get__opendata_t187ap38_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap38_L", "method": "GET"},
    )

    # 上市公司經營權及營業範圍異(變)動專區-經營權異動公司
    get__opendata_t187ap24_l = PythonOperator(
        task_id="get__opendata_t187ap24_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap24_L", "method": "GET"},
    )

    # 上市公司經營權及營業範圍異(變)動專區-經營權異動且營業範圍重大變更停止買賣公司
    get__opendata_t187ap26_l = PythonOperator(
        task_id="get__opendata_t187ap26_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap26_L", "method": "GET"},
    )

    # 上市公司召開股東常 (臨時) 會日期、地點及採用電子投票情形等資料彙總表
    get__opendata_t187ap41_l = PythonOperator(
        task_id="get__opendata_t187ap41_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap41_L", "method": "GET"},
    )

    # 上市公司經營權及營業範圍異(變)動專區-營業範圍重大變更公司
    get__opendata_t187ap25_l = PythonOperator(
        task_id="get__opendata_t187ap25_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap25_L", "method": "GET"},
    )

    # 上市公司經營權及營業範圍異(變)動專區-經營權異動且營業範圍重大變更列為變更交易公司
    get__opendata_t187ap27_l = PythonOperator(
        task_id="get__opendata_t187ap27_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap27_L", "method": "GET"},
    )

    # 上市公司公司治理之相關規程規則
    get__opendata_t187ap32_l = PythonOperator(
        task_id="get__opendata_t187ap32_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap32_L", "method": "GET"},
    )

    # 上市公司董事長是否兼任總經理
    get__opendata_t187ap33_l = PythonOperator(
        task_id="get__opendata_t187ap33_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap33_L", "method": "GET"},
    )

    # 上市公司董事、監察人質權設定占董事及監察人實際持有股數彙總表
    get__opendata_t187ap09_l = PythonOperator(
        task_id="get__opendata_t187ap09_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap09_L", "method": "GET"},
    )

    # 上市公司採累積投票制、全額連記法、候選人提名制選任董監事及當選資料彙總表
    get__opendata_t187ap34_l = PythonOperator(
        task_id="get__opendata_t187ap34_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap34_L", "method": "GET"},
    )

    # 上市公司股東行使提案權情形彙總表
    get__opendata_t187ap35_l = PythonOperator(
        task_id="get__opendata_t187ap35_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap35_L", "method": "GET"},
    )


    get__opendata_t187ap45_l >> get__opendata_t187ap46_l_20 >> get__opendata_t187ap46_l_19 >> get__opendata_t187ap46_l_18 >> get__opendata_t187ap46_l_17 >> get__opendata_t187ap46_l_16 >> get__opendata_t187ap46_l_15 >> get__opendata_t187ap46_l_14 >> get__opendata_t187ap46_l_13 >> get__opendata_t187ap46_l_12 >> get__opendata_t187ap46_l_11 >> get__opendata_t187ap46_l_10 >> get__opendata_t187ap46_l_9 >> get__opendata_t187ap46_l_8 >> get__opendata_t187ap05_p >> get__opendata_t187ap46_l_7 >> get__opendata_t187ap46_l_6 >> get__opendata_t187ap46_l_5 >> get__opendata_t187ap46_l_4 >> get__opendata_t187ap46_l_3 >> get__opendata_t187ap46_l_2 >> get__opendata_t187ap46_l_1 >> get__company_applylistingforeign >> get__company_newlisting >> get__company_suspendlistingcsvandhtml >> get__company_applylistinglocal >> get__opendata_t187ap04_l >> get__opendata_t187ap03_l >> get__opendata_t187ap02_l >> get__opendata_t187ap14_l >> get__opendata_t187ap08_l >> get__opendata_t187ap11_l >> get__opendata_t187ap12_l >> get__opendata_t187ap13_l >> get__opendata_t187ap22_l >> get__opendata_t187ap30_l >> get__opendata_t187ap29_a_l >> get__opendata_t187ap29_b_l >> get__opendata_t187ap29_c_l >> get__opendata_t187ap29_d_l >> get__opendata_t187ap23_l >> get__static_20151104_csr103 >> get__opendata_t187ap03_p >> get__announcement_punish >> get__opendata_t187ap10_l >> get__opendata_t187ap38_l >> get__opendata_t187ap24_l >> get__opendata_t187ap26_l >> get__opendata_t187ap41_l >> get__opendata_t187ap25_l >> get__opendata_t187ap27_l >> get__opendata_t187ap32_l >> get__opendata_t187ap33_l >> get__opendata_t187ap09_l >> get__opendata_t187ap34_l >> get__opendata_t187ap35_l
    # 定義 task 執行順序（平行執行）
    # return [get__opendata_t187ap45_l, get__opendata_t187ap46_l_20, get__opendata_t187ap46_l_19, get__opendata_t187ap46_l_18, get__opendata_t187ap46_l_17, get__opendata_t187ap46_l_16, get__opendata_t187ap46_l_15, get__opendata_t187ap46_l_14, get__opendata_t187ap46_l_13, get__opendata_t187ap46_l_12, get__opendata_t187ap46_l_11, get__opendata_t187ap46_l_10, get__opendata_t187ap46_l_9, get__opendata_t187ap46_l_8, get__opendata_t187ap05_p, get__opendata_t187ap46_l_7, get__opendata_t187ap46_l_6, get__opendata_t187ap46_l_5, get__opendata_t187ap46_l_4, get__opendata_t187ap46_l_3, get__opendata_t187ap46_l_2, get__opendata_t187ap46_l_1, get__company_applylistingforeign, get__company_newlisting, get__company_suspendlistingcsvandhtml, get__company_applylistinglocal, get__opendata_t187ap04_l, get__opendata_t187ap03_l, get__opendata_t187ap02_l, get__opendata_t187ap14_l, get__opendata_t187ap08_l, get__opendata_t187ap11_l, get__opendata_t187ap12_l, get__opendata_t187ap13_l, get__opendata_t187ap22_l, get__opendata_t187ap30_l, get__opendata_t187ap29_a_l, get__opendata_t187ap29_b_l, get__opendata_t187ap29_c_l, get__opendata_t187ap29_d_l, get__opendata_t187ap23_l, get__static_20151104_csr103, get__opendata_t187ap03_p, get__announcement_punish, get__opendata_t187ap10_l, get__opendata_t187ap38_l, get__opendata_t187ap24_l, get__opendata_t187ap26_l, get__opendata_t187ap41_l, get__opendata_t187ap25_l, get__opendata_t187ap27_l, get__opendata_t187ap32_l, get__opendata_t187ap33_l, get__opendata_t187ap09_l, get__opendata_t187ap34_l, get__opendata_t187ap35_l]

# 實例化 DAG
dag = fetch_corporate_governance()

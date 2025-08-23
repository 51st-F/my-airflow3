
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
    # collection = db_openapi_twse["securities_trading"]

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
    dag_id="openapi_twse_securities_trading",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["twse", "securities_trading"],
)
def fetch_securities_trading():
    # 上市個股日本益比、殖利率及股價淨值比（依代碼查詢）
    get__exchangereport_bwibbu_all = PythonOperator(
        task_id="get__exchangereport_bwibbu_all",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/BWIBBU_ALL", "method": "GET"},
    )

    # 上市個股日收盤價及月平均價
    get__exchangereport_stock_day_avg_all = PythonOperator(
        task_id="get__exchangereport_stock_day_avg_all",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/STOCK_DAY_AVG_ALL", "method": "GET"},
    )

    # 上市個股日成交資訊
    get__exchangereport_stock_day_all = PythonOperator(
        task_id="get__exchangereport_stock_day_all",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/STOCK_DAY_ALL", "method": "GET"},
    )

    # 上市個股月成交資訊
    get__exchangereport_fmsrfk_all = PythonOperator(
        task_id="get__exchangereport_fmsrfk_all",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/FMSRFK_ALL", "method": "GET"},
    )

    # 上市個股年成交資訊
    get__exchangereport_fmnptk_all = PythonOperator(
        task_id="get__exchangereport_fmnptk_all",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/FMNPTK_ALL", "method": "GET"},
    )

    # 每日收盤行情-大盤統計資訊
    get__exchangereport_mi_index = PythonOperator(
        task_id="get__exchangereport_mi_index",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/MI_INDEX", "method": "GET"},
    )

    # 集中市場外資及陸資投資類股持股比率表
    get__fund_mi_qfiis_cat = PythonOperator(
        task_id="get__fund_mi_qfiis_cat",
        python_callable=call_twse_api,
        op_kwargs={"path": "/fund/MI_QFIIS_cat", "method": "GET"},
    )

    # 集中市場外資及陸資持股前 20 名彙總表
    get__fund_mi_qfiis_sort_20 = PythonOperator(
        task_id="get__fund_mi_qfiis_sort_20",
        python_callable=call_twse_api,
        op_kwargs={"path": "/fund/MI_QFIIS_sort_20", "method": "GET"},
    )

    # 上市個股首五日無漲跌幅
    get__exchangereport_twt88u = PythonOperator(
        task_id="get__exchangereport_twt88u",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/TWT88U", "method": "GET"},
    )

    # 投資理財節目異常推介個股
    get__announcement_bfzfzu_t = PythonOperator(
        task_id="get__announcement_bfzfzu_t",
        python_callable=call_twse_api,
        op_kwargs={"path": "/Announcement/BFZFZU_T", "method": "GET"},
    )

    # 上市股票每日當日沖銷交易標的及統計
    get__exchangereport_twtb4u = PythonOperator(
        task_id="get__exchangereport_twtb4u",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/TWTB4U", "method": "GET"},
    )

    # 集中市場暫停先賣後買當日沖銷交易標的預告表
    get__exchangereport_twtbau1 = PythonOperator(
        task_id="get__exchangereport_twtbau1",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/TWTBAU1", "method": "GET"},
    )

    # 集中市場暫停先賣後買當日沖銷交易歷史查詢
    get__exchangereport_twtbau2 = PythonOperator(
        task_id="get__exchangereport_twtbau2",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/TWTBAU2", "method": "GET"},
    )

    # 每 5 秒委託成交統計
    get__exchangereport_mi_5mins = PythonOperator(
        task_id="get__exchangereport_mi_5mins",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/MI_5MINS", "method": "GET"},
    )

    # 集中市場每日市場成交資訊
    get__exchangereport_fmtqik = PythonOperator(
        task_id="get__exchangereport_fmtqik",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/FMTQIK", "method": "GET"},
    )

    # 集中市場每日成交量前二十名證券
    get__exchangereport_mi_index20 = PythonOperator(
        task_id="get__exchangereport_mi_index20",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/MI_INDEX20", "method": "GET"},
    )

    # 集中市場零股交易行情單
    get__exchangereport_twt53u = PythonOperator(
        task_id="get__exchangereport_twt53u",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/TWT53U", "method": "GET"},
    )

    # 集中市場暫停交易證券
    get__exchangereport_twtawu = PythonOperator(
        task_id="get__exchangereport_twtawu",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/TWTAWU", "method": "GET"},
    )

    # 集中市場盤後定價交易
    get__exchangereport_bft41u = PythonOperator(
        task_id="get__exchangereport_bft41u",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/BFT41U", "method": "GET"},
    )

    # 集中市場停資停券預告表
    get__exchangereport_bfi84u = PythonOperator(
        task_id="get__exchangereport_bfi84u",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/BFI84U", "method": "GET"},
    )

    # 集中市場融資融券餘額
    get__exchangereport_mi_margn = PythonOperator(
        task_id="get__exchangereport_mi_margn",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/MI_MARGN", "method": "GET"},
    )

    # 集中市場鉅額交易日成交量值統計
    get__block_bfiauu_d = PythonOperator(
        task_id="get__block_bfiauu_d",
        python_callable=call_twse_api,
        op_kwargs={"path": "/block/BFIAUU_d", "method": "GET"},
    )

    # 集中市場鉅額交易月成交量值統計
    get__block_bfiauu_m = PythonOperator(
        task_id="get__block_bfiauu_m",
        python_callable=call_twse_api,
        op_kwargs={"path": "/block/BFIAUU_m", "method": "GET"},
    )

    # 集中市場鉅額交易年成交量值統計
    get__block_bfiauu_y = PythonOperator(
        task_id="get__block_bfiauu_y",
        python_callable=call_twse_api,
        op_kwargs={"path": "/block/BFIAUU_y", "method": "GET"},
    )

    # 每日第一上市外國股票成交量值
    get__exchangereport_stock_first = PythonOperator(
        task_id="get__exchangereport_stock_first",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/STOCK_FIRST", "method": "GET"},
    )

    # 集中市場證券變更交易
    get__exchangereport_twt85u = PythonOperator(
        task_id="get__exchangereport_twt85u",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/TWT85U", "method": "GET"},
    )

    # 有價證券集中交易市場開（休）市日期
    get__holidayschedule_holidayschedule = PythonOperator(
        task_id="get__holidayschedule_holidayschedule",
        python_callable=call_twse_api,
        op_kwargs={"path": "/holidaySchedule/holidaySchedule", "method": "GET"},
    )

    # 上市個股日本益比、殖利率及股價淨值比（依日期查詢）
    get__exchangereport_bwibbu_d = PythonOperator(
        task_id="get__exchangereport_bwibbu_d",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/BWIBBU_d", "method": "GET"},
    )

    # 上市上櫃股票當日可借券賣出股數
    get__sbl_twt96u = PythonOperator(
        task_id="get__sbl_twt96u",
        python_callable=call_twse_api,
        op_kwargs={"path": "/SBL/TWT96U", "method": "GET"},
    )

    # 上市個股股價升降幅度
    get__exchangereport_twt84u = PythonOperator(
        task_id="get__exchangereport_twt84u",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/TWT84U", "method": "GET"},
    )

    # 集中市場漲跌證券數統計表
    get__opendata_twtazu_od = PythonOperator(
        task_id="get__opendata_twtazu_od",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/twtazu_od", "method": "GET"},
    )

    # 電子式交易統計資訊
    get__opendata_t187ap19 = PythonOperator(
        task_id="get__opendata_t187ap19",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap19", "method": "GET"},
    )

    # 上市權證基本資料彙總表
    get__opendata_t187ap37_l = PythonOperator(
        task_id="get__opendata_t187ap37_l",
        python_callable=call_twse_api,
        op_kwargs={"path": "/opendata/t187ap37_L", "method": "GET"},
    )

    # 集中市場公布注意累計次數異常資訊
    get__announcement_notetrans = PythonOperator(
        task_id="get__announcement_notetrans",
        python_callable=call_twse_api,
        op_kwargs={"path": "/announcement/notetrans", "method": "GET"},
    )

    # 集中市場當日公布注意股票
    get__announcement_notice = PythonOperator(
        task_id="get__announcement_notice",
        python_callable=call_twse_api,
        op_kwargs={"path": "/announcement/notice", "method": "GET"},
    )

    # 上市股票除權除息預告表
    get__exchangereport_twt48u_all = PythonOperator(
        task_id="get__exchangereport_twt48u_all",
        python_callable=call_twse_api,
        op_kwargs={"path": "/exchangeReport/TWT48U_ALL", "method": "GET"},
    )


    get__exchangereport_bwibbu_all >> get__exchangereport_stock_day_avg_all >> get__exchangereport_stock_day_all >> get__exchangereport_fmsrfk_all >> get__exchangereport_fmnptk_all >> get__exchangereport_mi_index >> get__fund_mi_qfiis_cat >> get__fund_mi_qfiis_sort_20 >> get__exchangereport_twt88u >> get__announcement_bfzfzu_t >> get__exchangereport_twtb4u >> get__exchangereport_twtbau1 >> get__exchangereport_twtbau2 >> get__exchangereport_mi_5mins >> get__exchangereport_fmtqik >> get__exchangereport_mi_index20 >> get__exchangereport_twt53u >> get__exchangereport_twtawu >> get__exchangereport_bft41u >> get__exchangereport_bfi84u >> get__exchangereport_mi_margn >> get__block_bfiauu_d >> get__block_bfiauu_m >> get__block_bfiauu_y >> get__exchangereport_stock_first >> get__exchangereport_twt85u >> get__holidayschedule_holidayschedule >> get__exchangereport_bwibbu_d >> get__sbl_twt96u >> get__exchangereport_twt84u >> get__opendata_twtazu_od >> get__opendata_t187ap19 >> get__opendata_t187ap37_l >> get__announcement_notetrans >> get__announcement_notice >> get__exchangereport_twt48u_all
    # 定義 task 執行順序（平行執行）
    # return [get__exchangereport_bwibbu_all, get__exchangereport_stock_day_avg_all, get__exchangereport_stock_day_all, get__exchangereport_fmsrfk_all, get__exchangereport_fmnptk_all, get__exchangereport_mi_index, get__fund_mi_qfiis_cat, get__fund_mi_qfiis_sort_20, get__exchangereport_twt88u, get__announcement_bfzfzu_t, get__exchangereport_twtb4u, get__exchangereport_twtbau1, get__exchangereport_twtbau2, get__exchangereport_mi_5mins, get__exchangereport_fmtqik, get__exchangereport_mi_index20, get__exchangereport_twt53u, get__exchangereport_twtawu, get__exchangereport_bft41u, get__exchangereport_bfi84u, get__exchangereport_mi_margn, get__block_bfiauu_d, get__block_bfiauu_m, get__block_bfiauu_y, get__exchangereport_stock_first, get__exchangereport_twt85u, get__holidayschedule_holidayschedule, get__exchangereport_bwibbu_d, get__sbl_twt96u, get__exchangereport_twt84u, get__opendata_twtazu_od, get__opendata_t187ap19, get__opendata_t187ap37_l, get__announcement_notetrans, get__announcement_notice, get__exchangereport_twt48u_all]

# 實例化 DAG
dag = fetch_securities_trading()

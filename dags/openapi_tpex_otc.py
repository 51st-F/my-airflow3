
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
    # collection = db_openapi_tpex["otc"]

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
    dag_id="openapi_tpex_otc",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["tpex", "otc"],
)
def fetch_otc():
    # 上櫃股票現股當沖交易標的資訊
    get__tpex_securities = PythonOperator(
        task_id="get__tpex_securities",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_securities", "method": "GET"},
    )

    # 上櫃當日公布暫停/恢復交易股票
    get__tpex_spendi_today = PythonOperator(
        task_id="get__tpex_spendi_today",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_spendi_today", "method": "GET"},
    )

    # 上櫃歷史公布暫停/恢復交易股票
    get__tpex_spendi_history = PythonOperator(
        task_id="get__tpex_spendi_history",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_spendi_history", "method": "GET"},
    )

    # 申請上櫃公司
    get__tpex_esb_applicant_companies = PythonOperator(
        task_id="get__tpex_esb_applicant_companies",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_esb_applicant_companies", "method": "GET"},
    )

    # 上櫃公布注意股票資訊
    get__tpex_trading_warning_information = PythonOperator(
        task_id="get__tpex_trading_warning_information",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_trading_warning_information", "method": "GET"},
    )

    # 上櫃處置有價證券資訊
    get__tpex_disposal_information = PythonOperator(
        task_id="get__tpex_disposal_information",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_disposal_information", "method": "GET"},
    )

    # 上櫃公布注意累計次數異常資訊
    get__tpex_trading_warning_note = PythonOperator(
        task_id="get__tpex_trading_warning_note",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_trading_warning_note", "method": "GET"},
    )

    # 上櫃股票市場現況
    get__tpex_mainborad_highlight = PythonOperator(
        task_id="get__tpex_mainborad_highlight",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_mainborad_highlight", "method": "GET"},
    )

    # 上櫃股票行情
    get__tpex_mainboard_daily_close_quotes = PythonOperator(
        task_id="get__tpex_mainboard_daily_close_quotes",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_mainboard_daily_close_quotes", "method": "GET"},
    )

    # 上櫃股票收盤行情
    get__tpex_mainboard_quotes = PythonOperator(
        task_id="get__tpex_mainboard_quotes",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_mainboard_quotes", "method": "GET"},
    )

    # 上櫃股票個股本益比、殖利率、股價淨值比
    get__tpex_mainboard_peratio_analysis = PythonOperator(
        task_id="get__tpex_mainboard_peratio_analysis",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_mainboard_peratio_analysis", "method": "GET"},
    )

    # 上櫃股票融資融券餘額
    get__tpex_mainboard_margin_balance = PythonOperator(
        task_id="get__tpex_mainboard_margin_balance",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_mainboard_margin_balance", "method": "GET"},
    )

    # 上櫃股票現股當沖交易統計資訊
    get__tpex_intraday_trading_statistics = PythonOperator(
        task_id="get__tpex_intraday_trading_statistics",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_intraday_trading_statistics", "method": "GET"},
    )

    # 上櫃股票熱門股證券商進出排行
    get__tpex_active_broker_volume = PythonOperator(
        task_id="get__tpex_active_broker_volume",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_active_broker_volume", "method": "GET"},
    )

    # 上櫃股票融券借券賣出餘額
    get__tpex_margin_sbl = PythonOperator(
        task_id="get__tpex_margin_sbl",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_margin_sbl", "method": "GET"},
    )

    # 上櫃股票除權除息計算結果表
    get__tpex_exright_daily = PythonOperator(
        task_id="get__tpex_exright_daily",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_exright_daily", "method": "GET"},
    )

    # 上櫃股票除權除息預告表
    get__tpex_exright_prepost = PythonOperator(
        task_id="get__tpex_exright_prepost",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_exright_prepost", "method": "GET"},
    )

    # 上櫃股票變更交易、分盤交易、管理股票與停止交易資訊
    get__tpex_cmode = PythonOperator(
        task_id="get__tpex_cmode",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_cmode", "method": "GET"},
    )

    # 上櫃股票零股交易資訊
    get__tpex_odd_stock = PythonOperator(
        task_id="get__tpex_odd_stock",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_odd_stock", "method": "GET"},
    )

    # 上櫃股票盤後定價行情
    get__tpex_off_market = PythonOperator(
        task_id="get__tpex_off_market",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_off_market", "method": "GET"},
    )

    # 上櫃融資融券暫停融券賣出預告表
    get__tpex_margin_trading_term = PythonOperator(
        task_id="get__tpex_margin_trading_term",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_margin_trading_term", "method": "GET"},
    )

    # 上櫃融資融券調整成數
    get__tpex_margin_trading_adjust = PythonOperator(
        task_id="get__tpex_margin_trading_adjust",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_margin_trading_adjust", "method": "GET"},
    )

    # 上櫃融資融券標借
    get__tpex_margin_trading_lend = PythonOperator(
        task_id="get__tpex_margin_trading_lend",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_margin_trading_lend", "method": "GET"},
    )

    # 上櫃信用交易餘額概況表
    get__tpex_margin_trading_marginspot = PythonOperator(
        task_id="get__tpex_margin_trading_marginspot",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_margin_trading_marginspot", "method": "GET"},
    )

    # 上櫃平盤下得融(借)券賣出之證券名單
    get__tpex_margin_trading_margin_mark = PythonOperator(
        task_id="get__tpex_margin_trading_margin_mark",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_margin_trading_margin_mark", "method": "GET"},
    )

    # 上櫃融資融券使用率報表
    get__tpex_margin_trading_margin_used = PythonOperator(
        task_id="get__tpex_margin_trading_margin_used",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_margin_trading_margin_used", "method": "GET"},
    )

    # 上櫃融資融券增減排行表
    get__tpex_margin_trading_short_sell = PythonOperator(
        task_id="get__tpex_margin_trading_short_sell",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_margin_trading_short_sell", "method": "GET"},
    )

    # 上櫃僑外資及陸資持股比例排行表
    get__tpex_3insti_qfii = PythonOperator(
        task_id="get__tpex_3insti_qfii",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_3insti_qfii", "method": "GET"},
    )

    # 上櫃各類股僑外資及陸資持股比例表
    get__tpex_3insti_qfii_industry = PythonOperator(
        task_id="get__tpex_3insti_qfii_industry",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_3insti_qfii_industry", "method": "GET"},
    )

    # 上櫃股票三大法人買賣明細資訊
    get__tpex_3insti_daily_trading = PythonOperator(
        task_id="get__tpex_3insti_daily_trading",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_3insti_daily_trading", "method": "GET"},
    )

    # 上櫃股票自營商買賣超彙總表
    get__tpex_3insti_dealer_trading = PythonOperator(
        task_id="get__tpex_3insti_dealer_trading",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_3insti_dealer_trading", "method": "GET"},
    )

    # 上櫃漲跌停未成交資訊
    get__tpex_ceil_non_trading = PythonOperator(
        task_id="get__tpex_ceil_non_trading",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_ceil_non_trading", "method": "GET"},
    )

    # 上櫃日成交量值指數
    get__tpex_daily_trading_index = PythonOperator(
        task_id="get__tpex_daily_trading_index",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_daily_trading_index", "method": "GET"},
    )

    # 上櫃當日融券賣出與借券賣出成交量值
    get__tpex_short_sell = PythonOperator(
        task_id="get__tpex_short_sell",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_short_sell", "method": "GET"},
    )

    # 上櫃各券商當日營業金額統計表
    get__tpex_daily_broker1 = PythonOperator(
        task_id="get__tpex_daily_broker1",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_daily_broker1", "method": "GET"},
    )

    # 上櫃盤中個股成交金額排行
    get__tpex_active_dollar_volume = PythonOperator(
        task_id="get__tpex_active_dollar_volume",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_active_dollar_volume", "method": "GET"},
    )

    # 上櫃盤中個股漲幅排行
    get__tpex_active_advanced = PythonOperator(
        task_id="get__tpex_active_advanced",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_active_advanced", "method": "GET"},
    )

    # 上櫃盤中個股跌幅排行
    get__tpex_active_declined = PythonOperator(
        task_id="get__tpex_active_declined",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_active_declined", "method": "GET"},
    )

    # 上櫃應付現股當日沖銷券差借券費率
    get__tpex_intraday_fee = PythonOperator(
        task_id="get__tpex_intraday_fee",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_intraday_fee", "method": "GET"},
    )

    # 上櫃暫停先賣後買當日沖銷交易標的預告表
    get__tpex_intraday_trading_pre = PythonOperator(
        task_id="get__tpex_intraday_trading_pre",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_intraday_trading_pre", "method": "GET"},
    )

    # 上櫃暫停先賣後買當日沖銷交易歷史查詢
    get__tpex_intraday_trading_his = PythonOperator(
        task_id="get__tpex_intraday_trading_his",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_intraday_trading_his", "method": "GET"},
    )

    # 上櫃歷史個股市值排行
    get__tpex_daily_market_value = PythonOperator(
        task_id="get__tpex_daily_market_value",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_daily_market_value", "method": "GET"},
    )

    # 上櫃歷史個股週轉率排行
    get__tpex_daily_turnover = PythonOperator(
        task_id="get__tpex_daily_turnover",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_daily_turnover", "method": "GET"},
    )

    # 上櫃歷史個股日均量排行
    get__tpex_trading_volumes_avg = PythonOperator(
        task_id="get__tpex_trading_volumes_avg",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_trading_volumes_avg", "method": "GET"},
    )

    # 上櫃歷史個股日均值排行
    get__tpex_trading_amount_avg = PythonOperator(
        task_id="get__tpex_trading_amount_avg",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_trading_amount_avg", "method": "GET"},
    )

    # 上櫃歷史類股成交價量比重
    get__tpex_trading_volume_ratio = PythonOperator(
        task_id="get__tpex_trading_volume_ratio",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_trading_volume_ratio", "method": "GET"},
    )

    # 上櫃歷史個股本益比排行
    get__tpex_pe_ratio_top10 = PythonOperator(
        task_id="get__tpex_pe_ratio_top10",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_pe_ratio_top10", "method": "GET"},
    )

    # 上櫃鉅額交易日成交資訊
    get__tpex_daily_qutoes_block = PythonOperator(
        task_id="get__tpex_daily_qutoes_block",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_daily_qutoes_block", "method": "GET"},
    )

    # 上櫃個股單一證券鉅額交易日成交資訊
    get__tpex_daily_trading_block = PythonOperator(
        task_id="get__tpex_daily_trading_block",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_daily_trading_block", "method": "GET"},
    )

    # 上櫃鉅額交易日成交量值統計
    get__tpex_daily_trading_summary_odd = PythonOperator(
        task_id="get__tpex_daily_trading_summary_odd",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_daily_trading_summary_odd", "method": "GET"},
    )

    # 上櫃鉅額交易月成交量值統計
    get__tpex_monthly_trading_summary_block = PythonOperator(
        task_id="get__tpex_monthly_trading_summary_block",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_monthly_trading_summary_block", "method": "GET"},
    )

    # 上櫃鉅額交易年成交量值統計
    get__tpex_yearly_trading_summary_block = PythonOperator(
        task_id="get__tpex_yearly_trading_summary_block",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_yearly_trading_summary_block", "method": "GET"},
    )

    # 上櫃歷史個股成交量排行
    get__tpex_volume_rank = PythonOperator(
        task_id="get__tpex_volume_rank",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_volume_rank", "method": "GET"},
    )

    # 上櫃歷史個股成交值排行
    get__tpex_amount_rank = PythonOperator(
        task_id="get__tpex_amount_rank",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_amount_rank", "method": "GET"},
    )

    # 上櫃股票等價系統成交分價表
    get__tpex_prvol = PythonOperator(
        task_id="get__tpex_prvol",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_prvol", "method": "GET"},
    )

    # 上櫃股票三大法人買賣金額彙總表
    get__tpex_3insti_summary = PythonOperator(
        task_id="get__tpex_3insti_summary",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_3insti_summary", "method": "GET"},
    )

    # 上櫃股票投信買賣超彙總表
    get__tpex_3insti_trading = PythonOperator(
        task_id="get__tpex_3insti_trading",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_3insti_trading", "method": "GET"},
    )

    # 鉅額交易歷史成交資訊
    get__tpex_daily_trade_block_day = PythonOperator(
        task_id="get__tpex_daily_trade_block_day",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_daily_trade_block_day", "method": "GET"},
    )

    # 上櫃每日暫緩開盤股票
    get__tpex_delayed_stock_open = PythonOperator(
        task_id="get__tpex_delayed_stock_open",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_delayed_stock_open", "method": "GET"},
    )

    # 上櫃每日暫緩收盤股票
    get__tpex_delayed_stock_close = PythonOperator(
        task_id="get__tpex_delayed_stock_close",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_delayed_stock_close", "method": "GET"},
    )

    # 上櫃首五日無漲跌幅資訊
    get__tpex_ipo_no_limit = PythonOperator(
        task_id="get__tpex_ipo_no_limit",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_ipo_no_limit", "method": "GET"},
    )

    # 上櫃股票外資及陸資買賣超彙總表
    get__tpex_3insti_qfii_trading = PythonOperator(
        task_id="get__tpex_3insti_qfii_trading",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/tpex_3insti_qfii_trading", "method": "GET"},
    )

    # 電子式交易統計資訊(上櫃)
    get__mopsfin_t187ap19_o = PythonOperator(
        task_id="get__mopsfin_t187ap19_o",
        python_callable=call_tpex_api,
        op_kwargs={"path": "/mopsfin_t187ap19_O", "method": "GET"},
    )


    get__tpex_securities >> get__tpex_spendi_today >> get__tpex_spendi_history >> get__tpex_esb_applicant_companies >> get__tpex_trading_warning_information >> get__tpex_disposal_information >> get__tpex_trading_warning_note >> get__tpex_mainborad_highlight >> get__tpex_mainboard_daily_close_quotes >> get__tpex_mainboard_quotes >> get__tpex_mainboard_peratio_analysis >> get__tpex_mainboard_margin_balance >> get__tpex_intraday_trading_statistics >> get__tpex_active_broker_volume >> get__tpex_margin_sbl >> get__tpex_exright_daily >> get__tpex_exright_prepost >> get__tpex_cmode >> get__tpex_odd_stock >> get__tpex_off_market >> get__tpex_margin_trading_term >> get__tpex_margin_trading_adjust >> get__tpex_margin_trading_lend >> get__tpex_margin_trading_marginspot >> get__tpex_margin_trading_margin_mark >> get__tpex_margin_trading_margin_used >> get__tpex_margin_trading_short_sell >> get__tpex_3insti_qfii >> get__tpex_3insti_qfii_industry >> get__tpex_3insti_daily_trading >> get__tpex_3insti_dealer_trading >> get__tpex_ceil_non_trading >> get__tpex_daily_trading_index >> get__tpex_short_sell >> get__tpex_daily_broker1 >> get__tpex_active_dollar_volume >> get__tpex_active_advanced >> get__tpex_active_declined >> get__tpex_intraday_fee >> get__tpex_intraday_trading_pre >> get__tpex_intraday_trading_his >> get__tpex_daily_market_value >> get__tpex_daily_turnover >> get__tpex_trading_volumes_avg >> get__tpex_trading_amount_avg >> get__tpex_trading_volume_ratio >> get__tpex_pe_ratio_top10 >> get__tpex_daily_qutoes_block >> get__tpex_daily_trading_block >> get__tpex_daily_trading_summary_odd >> get__tpex_monthly_trading_summary_block >> get__tpex_yearly_trading_summary_block >> get__tpex_volume_rank >> get__tpex_amount_rank >> get__tpex_prvol >> get__tpex_3insti_summary >> get__tpex_3insti_trading >> get__tpex_daily_trade_block_day >> get__tpex_delayed_stock_open >> get__tpex_delayed_stock_close >> get__tpex_ipo_no_limit >> get__tpex_3insti_qfii_trading >> get__mopsfin_t187ap19_o
    # 定義 task 執行順序（平行執行）
    # return [get__tpex_securities, get__tpex_spendi_today, get__tpex_spendi_history, get__tpex_esb_applicant_companies, get__tpex_trading_warning_information, get__tpex_disposal_information, get__tpex_trading_warning_note, get__tpex_mainborad_highlight, get__tpex_mainboard_daily_close_quotes, get__tpex_mainboard_quotes, get__tpex_mainboard_peratio_analysis, get__tpex_mainboard_margin_balance, get__tpex_intraday_trading_statistics, get__tpex_active_broker_volume, get__tpex_margin_sbl, get__tpex_exright_daily, get__tpex_exright_prepost, get__tpex_cmode, get__tpex_odd_stock, get__tpex_off_market, get__tpex_margin_trading_term, get__tpex_margin_trading_adjust, get__tpex_margin_trading_lend, get__tpex_margin_trading_marginspot, get__tpex_margin_trading_margin_mark, get__tpex_margin_trading_margin_used, get__tpex_margin_trading_short_sell, get__tpex_3insti_qfii, get__tpex_3insti_qfii_industry, get__tpex_3insti_daily_trading, get__tpex_3insti_dealer_trading, get__tpex_ceil_non_trading, get__tpex_daily_trading_index, get__tpex_short_sell, get__tpex_daily_broker1, get__tpex_active_dollar_volume, get__tpex_active_advanced, get__tpex_active_declined, get__tpex_intraday_fee, get__tpex_intraday_trading_pre, get__tpex_intraday_trading_his, get__tpex_daily_market_value, get__tpex_daily_turnover, get__tpex_trading_volumes_avg, get__tpex_trading_amount_avg, get__tpex_trading_volume_ratio, get__tpex_pe_ratio_top10, get__tpex_daily_qutoes_block, get__tpex_daily_trading_block, get__tpex_daily_trading_summary_odd, get__tpex_monthly_trading_summary_block, get__tpex_yearly_trading_summary_block, get__tpex_volume_rank, get__tpex_amount_rank, get__tpex_prvol, get__tpex_3insti_summary, get__tpex_3insti_trading, get__tpex_daily_trade_block_day, get__tpex_delayed_stock_open, get__tpex_delayed_stock_close, get__tpex_ipo_no_limit, get__tpex_3insti_qfii_trading, get__mopsfin_t187ap19_o]

# 實例化 DAG
dag = fetch_otc()

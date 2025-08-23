
from datetime import datetime, timedelta
import requests
import json

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

from utils.bot_telegram import send_task_telegram_notification
from utils.database import db_openapi_taifex

def call_taifex_api(path: str, method: str = "GET", **kwargs):
    # 取得 task instance，抓取 execution context 中資訊
    execution_date = kwargs.get("execution_date")

    # 呼叫 API
    url = f"https://openapi.taifex.com.tw/v1{path}"
    response = requests.request(method, url)
    response.raise_for_status()
    data = response.json()

    # 印出部分資料（方便除錯）
    print("data lens", len(data))
    print(json.dumps(data[:25], indent=2, ensure_ascii=False) if isinstance(data, list) else str(data))

    # 儲存到 MongoDB
    # collection = db_openapi_taifex["APIs"]

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
    dag_id="openapi_taifex_APIs",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    },
    tags=["taifex", "APIs"],
)
def fetch_APIs():
    # 店頭集中結算會員名冊
    get__ccp_cmlists = PythonOperator(
        task_id="get__ccp_cmlists",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/CCP_CMLists", "method": "GET"},
    )

    # 盤後交易時段指定豁免及非豁免代為沖銷商品
    get__productsexemptedah = PythonOperator(
        task_id="get__productsexemptedah",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/productsExemptedAH", "method": "GET"},
    )

    # 股票期貨/選擇權調整型契約資訊內容
    get__ssfadjustedinfo = PythonOperator(
        task_id="get__ssfadjustedinfo",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SSFAdjustedInfo", "method": "GET"},
    )

    # 股票期貨調整開盤參考價(盤後交易時段)
    get__ssfrefferedopeningpriceah = PythonOperator(
        task_id="get__ssfrefferedopeningpriceah",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SSFRefferedOpeningPriceAh", "method": "GET"},
    )

    # 股票期貨/選擇權調整開盤參考價
    get__ssfrefferedopeningprice = PythonOperator(
        task_id="get__ssfrefferedopeningprice",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SSFRefferedOpeningPrice", "method": "GET"},
    )

    # 股票期貨/選擇權契約調整一覽事項
    get__contractadj = PythonOperator(
        task_id="get__contractadj",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/ContractAdj", "method": "GET"},
    )

    # 期貨每日交易行情
    get__dailymarketreportfut = PythonOperator(
        task_id="get__dailymarketreportfut",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/DailyMarketReportFut", "method": "GET"},
    )

    # 選擇權每日交易行情
    get__dailymarketreportopt = PythonOperator(
        task_id="get__dailymarketreportopt",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/DailyMarketReportOpt", "method": "GET"},
    )

    # 選擇權每日Delta值
    get__dailyoptionsdelta = PythonOperator(
        task_id="get__dailyoptionsdelta",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/DailyOptionsDelta", "method": "GET"},
    )

    # 臺指選擇權Put/Call比
    get__putcallratio = PythonOperator(
        task_id="get__putcallratio",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/PutCallRatio", "method": "GET"},
    )

    # 鉅額交易逐筆撮合之單式委託成交
    get__blocktradecontinuousmatchingsingleorder = PythonOperator(
        task_id="get__blocktradecontinuousmatchingsingleorder",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/BlockTradeContinuousMatchingSingleOrder", "method": "GET"},
    )

    # 鉅額交易逐筆撮合之組合式委託成交
    get__blocktradecontinuousmatchingcombinationorder = PythonOperator(
        task_id="get__blocktradecontinuousmatchingcombinationorder",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/BlockTradeContinuousMatchingCombinationOrder", "method": "GET"},
    )

    # 鉅額交易議價申報成交
    get__blocktradenegotiation = PythonOperator(
        task_id="get__blocktradenegotiation",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/BlockTradeNegotiation", "method": "GET"},
    )

    # 鉅額交易各商品成交資訊
    get__blocktrade = PythonOperator(
        task_id="get__blocktrade",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/BlockTrade", "method": "GET"},
    )

    # 三大法人-總表-依日期
    get__marketdataofmajorinstitutionaltradersgeneralbythedate = PythonOperator(
        task_id="get__marketdataofmajorinstitutionaltradersgeneralbythedate",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketDataOfMajorInstitutionalTradersGeneralBytheDate", "method": "GET"},
    )

    # 三大法人-總表-依週別
    get__marketdataofmajorinstitutionaltradersgeneralbytheweek = PythonOperator(
        task_id="get__marketdataofmajorinstitutionaltradersgeneralbytheweek",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketDataOfMajorInstitutionalTradersGeneralBytheWeek", "method": "GET"},
    )

    # 三大法人-區分期貨與選擇權二類-依日期
    get__marketdataofmajorinstitutionaltradersdividedbyfuturesandoptionsbythedate = PythonOperator(
        task_id="get__marketdataofmajorinstitutionaltradersdividedbyfuturesandoptionsbythedate",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketDataOfMajorInstitutionalTradersDividedByFuturesAndOptionsBytheDate", "method": "GET"},
    )

    # 三大法人-區分期貨與選擇權二類-依週別
    get__marketdataofmajorinstitutionaltradersdividedbyfuturesandoptionsbytheweek = PythonOperator(
        task_id="get__marketdataofmajorinstitutionaltradersdividedbyfuturesandoptionsbytheweek",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketDataOfMajorInstitutionalTradersDividedByFuturesAndOptionsBytheWeek", "method": "GET"},
    )

    # 三大法人-區分各期貨契約-依日期
    get__marketdataofmajorinstitutionaltradersdetailsoffuturescontractsbythedate = PythonOperator(
        task_id="get__marketdataofmajorinstitutionaltradersdetailsoffuturescontractsbythedate",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate", "method": "GET"},
    )

    # 三大法人-區分各期貨契約-依週別
    get__marketdataofmajorinstitutionaltradersdetailsoffuturescontractsbytheweek = PythonOperator(
        task_id="get__marketdataofmajorinstitutionaltradersdetailsoffuturescontractsbytheweek",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheWeek", "method": "GET"},
    )

    # 三大法人-區分各選擇權契約-依日期
    get__marketdataofmajorinstitutionaltradersdetailsofoptionscontractsbythedate = PythonOperator(
        task_id="get__marketdataofmajorinstitutionaltradersdetailsofoptionscontractsbythedate",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketDataOfMajorInstitutionalTradersDetailsOfOptionsContractsBytheDate", "method": "GET"},
    )

    # 三大法人-區分各選擇權契約-依週別
    get__marketdataofmajorinstitutionaltradersdetailsofoptionscontractsbytheweek = PythonOperator(
        task_id="get__marketdataofmajorinstitutionaltradersdetailsofoptionscontractsbytheweek",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketDataOfMajorInstitutionalTradersDetailsOfOptionsContractsBytheWeek", "method": "GET"},
    )

    # 三大法人-選擇權買賣權分計-依日期
    get__marketdataofmajorinstitutionaltradersdetailsofcallsandputsbythedate = PythonOperator(
        task_id="get__marketdataofmajorinstitutionaltradersdetailsofcallsandputsbythedate",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketDataOfMajorInstitutionalTradersDetailsOfCallsAndPutsBytheDate", "method": "GET"},
    )

    # 三大法人-選擇權買賣權分計-依週別
    get__marketdataofmajorinstitutionaltradersdetailsofcallsandputsbytheweek = PythonOperator(
        task_id="get__marketdataofmajorinstitutionaltradersdetailsofcallsandputsbytheweek",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketDataOfMajorInstitutionalTradersDetailsOfCallsAndPutsBytheWeek", "method": "GET"},
    )

    # 期貨大額交易人未沖銷部位資料
    get__openinterestoflargetradersfutures = PythonOperator(
        task_id="get__openinterestoflargetradersfutures",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/OpenInterestOfLargeTradersFutures", "method": "GET"},
    )

    # 選擇權大額交易人未沖銷部位資料
    get__openinterestoflargetradersoptions = PythonOperator(
        task_id="get__openinterestoflargetradersoptions",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/OpenInterestOfLargeTradersOptions", "method": "GET"},
    )

    # 每日外幣參考匯率
    get__dailyforeignexchangerates = PythonOperator(
        task_id="get__dailyforeignexchangerates",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/DailyForeignExchangeRates", "method": "GET"},
    )

    # 個股類全市場部位限制
    get__totalmarketpositionlimitforsinglestockfuturesandequityoptions = PythonOperator(
        task_id="get__totalmarketpositionlimitforsinglestockfuturesandequityoptions",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/TotalMarketPositionLimitForSingleStockFuturesAndEquityOptions", "method": "GET"},
    )

    # 股票期貨契約調整開盤參考價
    get__singlestockfuturescontractreferredopeningprice = PythonOperator(
        task_id="get__singlestockfuturescontractreferredopeningprice",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SingleStockFuturesContractReferredOpeningPrice", "method": "GET"},
    )

    # 期交所期貨暨選擇權商品相關費用表
    get__futuresandoptionsfeeschedule = PythonOperator(
        task_id="get__futuresandoptionsfeeschedule",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FuturesAndOptionsFeeSchedule", "method": "GET"},
    )

    # 最後結算價
    get__finalsettlementprice = PythonOperator(
        task_id="get__finalsettlementprice",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FinalSettlementPrice", "method": "GET"},
    )

    # 到期契約履約交割
    get__settledpositionsofcontractsonexpirationdate = PythonOperator(
        task_id="get__settledpositionsofcontractsonexpirationdate",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SettledPositionsOfContractsOnExpirationDate", "method": "GET"},
    )

    # 保證金一覽表-股價指數類
    get__indexfuturesandoptionsmargining = PythonOperator(
        task_id="get__indexfuturesandoptionsmargining",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/IndexFuturesAndOptionsMargining", "method": "GET"},
    )

    # 保證金一覽表-利率類
    get__interestratefuturesmargining = PythonOperator(
        task_id="get__interestratefuturesmargining",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/InterestRateFuturesMargining", "method": "GET"},
    )

    # 保證金一覽表-商品類
    get__goldfuturesandoptionsmargining = PythonOperator(
        task_id="get__goldfuturesandoptionsmargining",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/GoldFuturesAndOptionsMargining", "method": "GET"},
    )

    # 保證金一覽表-股票類
    get__singlestockfuturesmargining = PythonOperator(
        task_id="get__singlestockfuturesmargining",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SingleStockFuturesMargining", "method": "GET"},
    )

    # 保證金一覽表-股票類(ETF)
    get__singlestockfuturesetfmargining = PythonOperator(
        task_id="get__singlestockfuturesetfmargining",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SingleStockFuturesETFMargining", "method": "GET"},
    )

    # 有價證券保證金之可抵繳標的-公債
    get__acceptablecollateralgovernmentbonds = PythonOperator(
        task_id="get__acceptablecollateralgovernmentbonds",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AcceptableCollateralGovernmentBonds", "method": "GET"},
    )

    # 有價證券保證金之可抵繳標的-國際債
    get__acceptablecollateralinternationalbonds = PythonOperator(
        task_id="get__acceptablecollateralinternationalbonds",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AcceptableCollateralInternationalBonds", "method": "GET"},
    )

    # 有價證券保證金之可抵繳標的-股票(含ETF)
    get__acceptablecollateralstock = PythonOperator(
        task_id="get__acceptablecollateralstock",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AcceptableCollateralStock", "method": "GET"},
    )

    # 期貨商每股淨值明細表
    get__netvaluepersharestatement = PythonOperator(
        task_id="get__netvaluepersharestatement",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/NetValuePerShareStatement", "method": "GET"},
    )

    # 期貨商稅前累計損益明細表
    get__accumulatedincomestatementbeforetaxstatement = PythonOperator(
        task_id="get__accumulatedincomestatementbeforetaxstatement",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AccumulatedIncomeStatementBeforeTaxStatement", "method": "GET"},
    )

    # 期貨商交易量日報表－期貨
    get__daily_fut = PythonOperator(
        task_id="get__daily_fut",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/Daily_FUT", "method": "GET"},
    )

    # 期貨商交易量週報表－期貨
    get__weekly_fut = PythonOperator(
        task_id="get__weekly_fut",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/Weekly_FUT", "method": "GET"},
    )

    # 期貨商交易量月報表－期貨
    get__monthly_fut = PythonOperator(
        task_id="get__monthly_fut",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/Monthly_FUT", "method": "GET"},
    )

    # 期貨商交易量年報表－期貨
    get__yearly_fut = PythonOperator(
        task_id="get__yearly_fut",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/Yearly_FUT", "method": "GET"},
    )

    # 期貨商交易量日報表－選擇權
    get__daily_opt = PythonOperator(
        task_id="get__daily_opt",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/Daily_OPT", "method": "GET"},
    )

    # 期貨商交易量週報表－選擇權
    get__weekly_opt = PythonOperator(
        task_id="get__weekly_opt",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/Weekly_OPT", "method": "GET"},
    )

    # 期貨商交易量月報表－選擇權
    get__monthly_opt = PythonOperator(
        task_id="get__monthly_opt",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/Monthly_OPT", "method": "GET"},
    )

    # 期貨商交易量年報表－選擇權
    get__yearly_opt = PythonOperator(
        task_id="get__yearly_opt",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/Yearly_OPT", "method": "GET"},
    )

    # 保管銀行名冊
    get__custodianbanklists = PythonOperator(
        task_id="get__custodianbanklists",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/CustodianBankLists", "method": "GET"},
    )

    # 結算會員名冊
    get__cmlists = PythonOperator(
        task_id="get__cmlists",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/CMLists", "method": "GET"},
    )

    # 結算銀行名冊
    get__clearingbanklists = PythonOperator(
        task_id="get__clearingbanklists",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/ClearingBankLists", "method": "GET"},
    )

    # 期貨商總公司名冊
    get__fcmlists = PythonOperator(
        task_id="get__fcmlists",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FCMLists", "method": "GET"},
    )

    # 期貨商分公司名冊
    get__fcmbranchlists = PythonOperator(
        task_id="get__fcmbranchlists",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FCMBranchLists", "method": "GET"},
    )

    # 期貨商交易量週到期選擇權日報表
    get__daily_opt_w = PythonOperator(
        task_id="get__daily_opt_w",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/Daily_OPT_W", "method": "GET"},
    )

    # 交易人部位限制-非個股類
    get__positionlimitnonequity = PythonOperator(
        task_id="get__positionlimitnonequity",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/PositionLimitNonEquity", "method": "GET"},
    )

    # 交易人部位限制-個股類
    get__positionlimitequity = PythonOperator(
        task_id="get__positionlimitequity",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/PositionLimitEquity", "method": "GET"},
    )

    # 每日期貨每筆成交資料
    get__timeandsalesdata = PythonOperator(
        task_id="get__timeandsalesdata",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/TimeAndSalesData", "method": "GET"},
    )

    # 每日期貨價差委託成交概況表
    get__dailyvolumereportoncalendarspreadorders = PythonOperator(
        task_id="get__dailyvolumereportoncalendarspreadorders",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/DailyVolumeReportOnCalendarSpreadOrders", "method": "GET"},
    )

    # 每日期貨價差每筆成交資料
    get__timeandsalesdataoncalendarspreadorders = PythonOperator(
        task_id="get__timeandsalesdataoncalendarspreadorders",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/TimeAndSalesDataOnCalendarSpreadOrders", "method": "GET"},
    )

    # 每日選擇權每筆成交資料
    get__optionstimeandsalesdata = PythonOperator(
        task_id="get__optionstimeandsalesdata",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/OptionsTimeAndSalesData", "method": "GET"},
    )

    # 期貨商品造市者清單
    get__marketmakerlistsfut = PythonOperator(
        task_id="get__marketmakerlistsfut",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketMakerListsFut", "method": "GET"},
    )

    # 選擇權商品造市者清單
    get__marketmakerlistsopt = PythonOperator(
        task_id="get__marketmakerlistsopt",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketMakerListsOpt", "method": "GET"},
    )

    # 各商品年成交量統計表
    get__annualtradingvolume = PythonOperator(
        task_id="get__annualtradingvolume",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AnnualTradingVolume", "method": "GET"},
    )

    # 股票期貨交易標的
    get__ssflists = PythonOperator(
        task_id="get__ssflists",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SSFLists", "method": "GET"},
    )

    # 股票選擇權交易標的
    get__ssolists = PythonOperator(
        task_id="get__ssolists",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SSOLists", "method": "GET"},
    )

    # 鉅額交易逐筆撮合之單式委託成交-期貨商品
    get__btcontinuousmatchingsingleorderfutures = PythonOperator(
        task_id="get__btcontinuousmatchingsingleorderfutures",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/BTContinuousMatchingSingleOrderFutures", "method": "GET"},
    )

    # 鉅額交易逐筆撮合之單式委託成交-選擇權商品
    get__btcontinuousmatchingsingleorderoptions = PythonOperator(
        task_id="get__btcontinuousmatchingsingleorderoptions",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/BTContinuousMatchingSingleOrderOptions", "method": "GET"},
    )

    # 鉅額交易各商品成交資訊-期貨商品
    get__btdailytradeinformationfutures = PythonOperator(
        task_id="get__btdailytradeinformationfutures",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/BTDailyTradeInformationFutures", "method": "GET"},
    )

    # 鉅額交易各商品成交資訊-選擇權商品
    get__btdailytradeinformationoptions = PythonOperator(
        task_id="get__btdailytradeinformationoptions",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/BTDailyTradeInformationOptions", "method": "GET"},
    )

    # 鉅額交易成交量統計-期貨商品
    get__dailysummaryofblocktradefutures = PythonOperator(
        task_id="get__dailysummaryofblocktradefutures",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/DailySummaryOfBlockTradeFutures", "method": "GET"},
    )

    # 鉅額交易成交量統計-選擇權商品
    get__dailysummaryofblocktradeoptions = PythonOperator(
        task_id="get__dailysummaryofblocktradeoptions",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/DailySummaryOfBlockTradeOptions", "method": "GET"},
    )

    # 最後結算價-期貨商品
    get__finalsettlementpricefutures = PythonOperator(
        task_id="get__finalsettlementpricefutures",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FinalSettlementPriceFutures", "method": "GET"},
    )

    # 最後結算價-選擇權商品
    get__finalsettlementpriceoptions = PythonOperator(
        task_id="get__finalsettlementpriceoptions",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FinalSettlementPriceOptions", "method": "GET"},
    )

    # 到期契約履約交割-期貨商品
    get__settledpositionsfutures = PythonOperator(
        task_id="get__settledpositionsfutures",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SettledPositionsFutures", "method": "GET"},
    )

    # 到期契約履約交割-選擇權商品
    get__settledpositionsoptions = PythonOperator(
        task_id="get__settledpositionsoptions",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SettledPositionsOptions", "method": "GET"},
    )

    # 兼營期貨商每股淨值明細表
    get__netvaluepershares = PythonOperator(
        task_id="get__netvaluepershares",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/NetValuePerShareS", "method": "GET"},
    )

    # 專營期貨商每股淨值明細表
    get__netvaluepersharef = PythonOperator(
        task_id="get__netvaluepersharef",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/NetValuePerShareF", "method": "GET"},
    )

    # 複委託期貨商每股淨值明細表
    get__netvaluepersharer = PythonOperator(
        task_id="get__netvaluepersharer",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/NetValuePerShareR", "method": "GET"},
    )

    # 兼營期貨商稅前累計損益明細表
    get__accumulatedincomestates = PythonOperator(
        task_id="get__accumulatedincomestates",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AccumulatedIncomeStateS", "method": "GET"},
    )

    # 專營期貨商稅前累計損益明細表
    get__accumulatedincomestatef = PythonOperator(
        task_id="get__accumulatedincomestatef",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AccumulatedIncomeStateF", "method": "GET"},
    )

    # 複委託期貨商稅前累計損益明細表
    get__accumulatedincomestater = PythonOperator(
        task_id="get__accumulatedincomestater",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AccumulatedIncomeStateR", "method": "GET"},
    )

    # 兼營期貨商稅前損益排序表
    get__netincomerankings = PythonOperator(
        task_id="get__netincomerankings",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/NetIncomeRankingS", "method": "GET"},
    )

    # 專營期貨商稅前損益排序表
    get__netincomerankingf = PythonOperator(
        task_id="get__netincomerankingf",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/NetIncomeRankingF", "method": "GET"},
    )

    # 複委託期貨商稅前損益排序表
    get__netincomerankingr = PythonOperator(
        task_id="get__netincomerankingr",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/NetIncomeRankingR", "method": "GET"},
    )

    # 兼營期貨商稅前累計損益彙總表
    get__incomestatements = PythonOperator(
        task_id="get__incomestatements",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/IncomeStatementS", "method": "GET"},
    )

    # 專營期貨商稅前累計損益彙總表
    get__incomestatementf = PythonOperator(
        task_id="get__incomestatementf",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/IncomeStatementF", "method": "GET"},
    )

    # 複委託期貨商稅前累計損益彙總表
    get__incomestatementr = PythonOperator(
        task_id="get__incomestatementr",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/IncomeStatementR", "method": "GET"},
    )

    # 兼營期貨商每股稅前盈餘統計表
    get__epsbeforetaxs = PythonOperator(
        task_id="get__epsbeforetaxs",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/EPSBeforeTaxS", "method": "GET"},
    )

    # 專營期貨商每股稅前盈餘統計表
    get__epsbeforetaxf = PythonOperator(
        task_id="get__epsbeforetaxf",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/EPSBeforeTaxF", "method": "GET"},
    )

    # 複委託期貨商每股稅前盈餘統計表
    get__epsbeforetaxr = PythonOperator(
        task_id="get__epsbeforetaxr",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/EPSBeforeTaxR", "method": "GET"},
    )

    # 市場參與者統計
    get__marketparticipants = PythonOperator(
        task_id="get__marketparticipants",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MarketParticipants", "method": "GET"},
    )

    # 期貨各類交易人各商品交易量統計表
    get__monthlytradingstatisticsfutures = PythonOperator(
        task_id="get__monthlytradingstatisticsfutures",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MonthlyTradingStatisticsFutures", "method": "GET"},
    )

    # 選擇權各類交易人各商品交易量統計表
    get__monthlytradingstatisticsoptions = PythonOperator(
        task_id="get__monthlytradingstatisticsoptions",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/MonthlyTradingStatisticsOptions", "method": "GET"},
    )

    # 最後結算價-股票期貨
    get__finalsettlementpricessf = PythonOperator(
        task_id="get__finalsettlementpricessf",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FinalSettlementPriceSSF", "method": "GET"},
    )

    # 最後結算價-股票選擇權
    get__finalsettlementpricesso = PythonOperator(
        task_id="get__finalsettlementpricesso",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FinalSettlementPriceSSO", "method": "GET"},
    )

    # 最後結算價-股價指數類（指數期貨）
    get__finalsettlementpriceindexfutures = PythonOperator(
        task_id="get__finalsettlementpriceindexfutures",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FinalSettlementPriceIndexFutures", "method": "GET"},
    )

    # 最後結算價-股價指數類（指數選擇權）
    get__finalsettlementpriceindexoptions = PythonOperator(
        task_id="get__finalsettlementpriceindexoptions",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FinalSettlementPriceIndexOptions", "method": "GET"},
    )

    # 最後結算價-利率類
    get__finalsettlementpriceir = PythonOperator(
        task_id="get__finalsettlementpriceir",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FinalSettlementPriceIR", "method": "GET"},
    )

    # 最後結算價-商品類
    get__finalsettlementpricegold = PythonOperator(
        task_id="get__finalsettlementpricegold",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FinalSettlementPriceGold", "method": "GET"},
    )

    # 最後結算價-匯率類
    get__finalsettlementpricefx = PythonOperator(
        task_id="get__finalsettlementpricefx",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FinalSettlementPriceFx", "method": "GET"},
    )

    # 到期契約履約交割-股價指數類（指數期貨）
    get__settledpositionsindexfutures = PythonOperator(
        task_id="get__settledpositionsindexfutures",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SettledPositionsIndexFutures", "method": "GET"},
    )

    # 到期契約履約交割-股價指數類（指數選擇權）
    get__settledpositionsindexoptions = PythonOperator(
        task_id="get__settledpositionsindexoptions",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SettledPositionsIndexOptions", "method": "GET"},
    )

    # 到期契約履約交割-股票類（股票期貨）
    get__settledpositionsssf = PythonOperator(
        task_id="get__settledpositionsssf",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SettledPositionsSSF", "method": "GET"},
    )

    # 到期契約履約交割-股票類（股票選擇權）
    get__settledpositionssso = PythonOperator(
        task_id="get__settledpositionssso",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SettledPositionsSSO", "method": "GET"},
    )

    # 到期契約履約交割-利率類
    get__settledpositionsir = PythonOperator(
        task_id="get__settledpositionsir",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SettledPositionsIR", "method": "GET"},
    )

    # 到期契約履約交割-商品類
    get__settledpositionsgold = PythonOperator(
        task_id="get__settledpositionsgold",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SettledPositionsGold", "method": "GET"},
    )

    # 到期契約履約交割-匯率類
    get__settledpositionsfx = PythonOperator(
        task_id="get__settledpositionsfx",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SettledPositionsFX", "method": "GET"},
    )

    # 商品年成交量統計-股價指數類（指數期貨）
    get__annualtradingindexfutures = PythonOperator(
        task_id="get__annualtradingindexfutures",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AnnualTradingIndexFutures", "method": "GET"},
    )

    # 商品年成交量統計-股價指數類（指數選擇權）
    get__annualtradingindexoptions = PythonOperator(
        task_id="get__annualtradingindexoptions",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AnnualTradingIndexOptions", "method": "GET"},
    )

    # 商品年成交量統計-股票類（股票期貨）
    get__annualtradingssf = PythonOperator(
        task_id="get__annualtradingssf",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AnnualTradingSSF", "method": "GET"},
    )

    # 商品年成交量統計-股票類（股票選擇權）
    get__annualtradingsso = PythonOperator(
        task_id="get__annualtradingsso",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AnnualTradingSSO", "method": "GET"},
    )

    # 商品年成交量統計-利率類
    get__annualtradingir = PythonOperator(
        task_id="get__annualtradingir",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AnnualTradingIR", "method": "GET"},
    )

    # 商品年成交量統計-商品類
    get__annualtradinggold = PythonOperator(
        task_id="get__annualtradinggold",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AnnualTradingGold", "method": "GET"},
    )

    # 每月市場電子式交易下單統計（網路與DMA下單）
    get__etradeqty = PythonOperator(
        task_id="get__etradeqty",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/eTradeQty", "method": "GET"},
    )

    # 每日股票期貨交易量前十大統計表
    get__stftop10 = PythonOperator(
        task_id="get__stftop10",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/STFTop10", "method": "GET"},
    )

    # 商品年成交量統計-匯率類
    get__annualtradingfx = PythonOperator(
        task_id="get__annualtradingfx",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AnnualTradingFX", "method": "GET"},
    )

    # 到期契約履約交割-匯率期貨類
    get__settledpositionsfxfutures = PythonOperator(
        task_id="get__settledpositionsfxfutures",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/SettledPositionsFXFutures", "method": "GET"},
    )

    # 保證金一覽表-匯率類
    get__fxfuturesandoptionsmargining = PythonOperator(
        task_id="get__fxfuturesandoptionsmargining",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/FXFuturesAndOptionsMargining", "method": "GET"},
    )

    # 有價證券保證金之可抵繳標的增刪紀錄
    get__acceptablecollaterallogstock = PythonOperator(
        task_id="get__acceptablecollaterallogstock",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/AcceptableCollateralLogStock", "method": "GET"},
    )

    # 每日股價指數類選擇權未平倉量增減
    get__va01 = PythonOperator(
        task_id="get__va01",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va01", "method": "GET"},
    )

    # 每日個股選擇權未平倉量增減(區分股票與ETF)
    get__va02 = PythonOperator(
        task_id="get__va02",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va02", "method": "GET"},
    )

    # 每日商品類選擇權未平倉量增減
    get__va03 = PythonOperator(
        task_id="get__va03",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va03", "method": "GET"},
    )

    # 股價指數類期貨商品每月平均成交金額
    get__va04 = PythonOperator(
        task_id="get__va04",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va04", "method": "GET"},
    )

    # 個股期貨商品每月平均成交金額(區分股票與ETF)
    get__va05 = PythonOperator(
        task_id="get__va05",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va05", "method": "GET"},
    )

    # 利率期貨商品每月平均成交金額
    get__va06 = PythonOperator(
        task_id="get__va06",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va06", "method": "GET"},
    )

    # 股價指數類選擇權商品每月平均成交金額(權利金)
    get__va07 = PythonOperator(
        task_id="get__va07",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va07", "method": "GET"},
    )

    # 個股選擇權商品每月平均成交金額(權利金-區分股票與ETF)
    get__va08 = PythonOperator(
        task_id="get__va08",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va08", "method": "GET"},
    )

    # 商品類期貨每月平均成交金額
    get__va09 = PythonOperator(
        task_id="get__va09",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va09", "method": "GET"},
    )

    # 商品類選擇權每月平均成交金額(權利金)
    get__va10 = PythonOperator(
        task_id="get__va10",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va10", "method": "GET"},
    )

    # 匯率類期貨每月平均成交金額
    get__va11 = PythonOperator(
        task_id="get__va11",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va11", "method": "GET"},
    )

    # 每日個股期貨交易量統計表（區分股票與ETF）
    get__va12 = PythonOperator(
        task_id="get__va12",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va12", "method": "GET"},
    )

    # 每月個股期貨交易量統計表（區分股票與ETF）
    get__va13 = PythonOperator(
        task_id="get__va13",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va13", "method": "GET"},
    )

    # 每年個股期貨交易量統計表（區分股票與ETF）
    get__va14 = PythonOperator(
        task_id="get__va14",
        python_callable=call_taifex_api,
        op_kwargs={"path": "/va14", "method": "GET"},
    )


    get__ccp_cmlists >> get__productsexemptedah >> get__ssfadjustedinfo >> get__ssfrefferedopeningpriceah >> get__ssfrefferedopeningprice >> get__contractadj >> get__dailymarketreportfut >> get__dailymarketreportopt >> get__dailyoptionsdelta >> get__putcallratio >> get__blocktradecontinuousmatchingsingleorder >> get__blocktradecontinuousmatchingcombinationorder >> get__blocktradenegotiation >> get__blocktrade >> get__marketdataofmajorinstitutionaltradersgeneralbythedate >> get__marketdataofmajorinstitutionaltradersgeneralbytheweek >> get__marketdataofmajorinstitutionaltradersdividedbyfuturesandoptionsbythedate >> get__marketdataofmajorinstitutionaltradersdividedbyfuturesandoptionsbytheweek >> get__marketdataofmajorinstitutionaltradersdetailsoffuturescontractsbythedate >> get__marketdataofmajorinstitutionaltradersdetailsoffuturescontractsbytheweek >> get__marketdataofmajorinstitutionaltradersdetailsofoptionscontractsbythedate >> get__marketdataofmajorinstitutionaltradersdetailsofoptionscontractsbytheweek >> get__marketdataofmajorinstitutionaltradersdetailsofcallsandputsbythedate >> get__marketdataofmajorinstitutionaltradersdetailsofcallsandputsbytheweek >> get__openinterestoflargetradersfutures >> get__openinterestoflargetradersoptions >> get__dailyforeignexchangerates >> get__totalmarketpositionlimitforsinglestockfuturesandequityoptions >> get__singlestockfuturescontractreferredopeningprice >> get__futuresandoptionsfeeschedule >> get__finalsettlementprice >> get__settledpositionsofcontractsonexpirationdate >> get__indexfuturesandoptionsmargining >> get__interestratefuturesmargining >> get__goldfuturesandoptionsmargining >> get__singlestockfuturesmargining >> get__singlestockfuturesetfmargining >> get__acceptablecollateralgovernmentbonds >> get__acceptablecollateralinternationalbonds >> get__acceptablecollateralstock >> get__netvaluepersharestatement >> get__accumulatedincomestatementbeforetaxstatement >> get__daily_fut >> get__weekly_fut >> get__monthly_fut >> get__yearly_fut >> get__daily_opt >> get__weekly_opt >> get__monthly_opt >> get__yearly_opt >> get__custodianbanklists >> get__cmlists >> get__clearingbanklists >> get__fcmlists >> get__fcmbranchlists >> get__daily_opt_w >> get__positionlimitnonequity >> get__positionlimitequity >> get__timeandsalesdata >> get__dailyvolumereportoncalendarspreadorders >> get__timeandsalesdataoncalendarspreadorders >> get__optionstimeandsalesdata >> get__marketmakerlistsfut >> get__marketmakerlistsopt >> get__annualtradingvolume >> get__ssflists >> get__ssolists >> get__btcontinuousmatchingsingleorderfutures >> get__btcontinuousmatchingsingleorderoptions >> get__btdailytradeinformationfutures >> get__btdailytradeinformationoptions >> get__dailysummaryofblocktradefutures >> get__dailysummaryofblocktradeoptions >> get__finalsettlementpricefutures >> get__finalsettlementpriceoptions >> get__settledpositionsfutures >> get__settledpositionsoptions >> get__netvaluepershares >> get__netvaluepersharef >> get__netvaluepersharer >> get__accumulatedincomestates >> get__accumulatedincomestatef >> get__accumulatedincomestater >> get__netincomerankings >> get__netincomerankingf >> get__netincomerankingr >> get__incomestatements >> get__incomestatementf >> get__incomestatementr >> get__epsbeforetaxs >> get__epsbeforetaxf >> get__epsbeforetaxr >> get__marketparticipants >> get__monthlytradingstatisticsfutures >> get__monthlytradingstatisticsoptions >> get__finalsettlementpricessf >> get__finalsettlementpricesso >> get__finalsettlementpriceindexfutures >> get__finalsettlementpriceindexoptions >> get__finalsettlementpriceir >> get__finalsettlementpricegold >> get__finalsettlementpricefx >> get__settledpositionsindexfutures >> get__settledpositionsindexoptions >> get__settledpositionsssf >> get__settledpositionssso >> get__settledpositionsir >> get__settledpositionsgold >> get__settledpositionsfx >> get__annualtradingindexfutures >> get__annualtradingindexoptions >> get__annualtradingssf >> get__annualtradingsso >> get__annualtradingir >> get__annualtradinggold >> get__etradeqty >> get__stftop10 >> get__annualtradingfx >> get__settledpositionsfxfutures >> get__fxfuturesandoptionsmargining >> get__acceptablecollaterallogstock >> get__va01 >> get__va02 >> get__va03 >> get__va04 >> get__va05 >> get__va06 >> get__va07 >> get__va08 >> get__va09 >> get__va10 >> get__va11 >> get__va12 >> get__va13 >> get__va14
    # 定義 task 執行順序（平行執行）
    # return [get__ccp_cmlists, get__productsexemptedah, get__ssfadjustedinfo, get__ssfrefferedopeningpriceah, get__ssfrefferedopeningprice, get__contractadj, get__dailymarketreportfut, get__dailymarketreportopt, get__dailyoptionsdelta, get__putcallratio, get__blocktradecontinuousmatchingsingleorder, get__blocktradecontinuousmatchingcombinationorder, get__blocktradenegotiation, get__blocktrade, get__marketdataofmajorinstitutionaltradersgeneralbythedate, get__marketdataofmajorinstitutionaltradersgeneralbytheweek, get__marketdataofmajorinstitutionaltradersdividedbyfuturesandoptionsbythedate, get__marketdataofmajorinstitutionaltradersdividedbyfuturesandoptionsbytheweek, get__marketdataofmajorinstitutionaltradersdetailsoffuturescontractsbythedate, get__marketdataofmajorinstitutionaltradersdetailsoffuturescontractsbytheweek, get__marketdataofmajorinstitutionaltradersdetailsofoptionscontractsbythedate, get__marketdataofmajorinstitutionaltradersdetailsofoptionscontractsbytheweek, get__marketdataofmajorinstitutionaltradersdetailsofcallsandputsbythedate, get__marketdataofmajorinstitutionaltradersdetailsofcallsandputsbytheweek, get__openinterestoflargetradersfutures, get__openinterestoflargetradersoptions, get__dailyforeignexchangerates, get__totalmarketpositionlimitforsinglestockfuturesandequityoptions, get__singlestockfuturescontractreferredopeningprice, get__futuresandoptionsfeeschedule, get__finalsettlementprice, get__settledpositionsofcontractsonexpirationdate, get__indexfuturesandoptionsmargining, get__interestratefuturesmargining, get__goldfuturesandoptionsmargining, get__singlestockfuturesmargining, get__singlestockfuturesetfmargining, get__acceptablecollateralgovernmentbonds, get__acceptablecollateralinternationalbonds, get__acceptablecollateralstock, get__netvaluepersharestatement, get__accumulatedincomestatementbeforetaxstatement, get__daily_fut, get__weekly_fut, get__monthly_fut, get__yearly_fut, get__daily_opt, get__weekly_opt, get__monthly_opt, get__yearly_opt, get__custodianbanklists, get__cmlists, get__clearingbanklists, get__fcmlists, get__fcmbranchlists, get__daily_opt_w, get__positionlimitnonequity, get__positionlimitequity, get__timeandsalesdata, get__dailyvolumereportoncalendarspreadorders, get__timeandsalesdataoncalendarspreadorders, get__optionstimeandsalesdata, get__marketmakerlistsfut, get__marketmakerlistsopt, get__annualtradingvolume, get__ssflists, get__ssolists, get__btcontinuousmatchingsingleorderfutures, get__btcontinuousmatchingsingleorderoptions, get__btdailytradeinformationfutures, get__btdailytradeinformationoptions, get__dailysummaryofblocktradefutures, get__dailysummaryofblocktradeoptions, get__finalsettlementpricefutures, get__finalsettlementpriceoptions, get__settledpositionsfutures, get__settledpositionsoptions, get__netvaluepershares, get__netvaluepersharef, get__netvaluepersharer, get__accumulatedincomestates, get__accumulatedincomestatef, get__accumulatedincomestater, get__netincomerankings, get__netincomerankingf, get__netincomerankingr, get__incomestatements, get__incomestatementf, get__incomestatementr, get__epsbeforetaxs, get__epsbeforetaxf, get__epsbeforetaxr, get__marketparticipants, get__monthlytradingstatisticsfutures, get__monthlytradingstatisticsoptions, get__finalsettlementpricessf, get__finalsettlementpricesso, get__finalsettlementpriceindexfutures, get__finalsettlementpriceindexoptions, get__finalsettlementpriceir, get__finalsettlementpricegold, get__finalsettlementpricefx, get__settledpositionsindexfutures, get__settledpositionsindexoptions, get__settledpositionsssf, get__settledpositionssso, get__settledpositionsir, get__settledpositionsgold, get__settledpositionsfx, get__annualtradingindexfutures, get__annualtradingindexoptions, get__annualtradingssf, get__annualtradingsso, get__annualtradingir, get__annualtradinggold, get__etradeqty, get__stftop10, get__annualtradingfx, get__settledpositionsfxfutures, get__fxfuturesandoptionsmargining, get__acceptablecollaterallogstock, get__va01, get__va02, get__va03, get__va04, get__va05, get__va06, get__va07, get__va08, get__va09, get__va10, get__va11, get__va12, get__va13, get__va14]

# 實例化 DAG
dag = fetch_APIs()

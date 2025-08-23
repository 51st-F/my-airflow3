import requests
import re
from collections import defaultdict

# Step 1: 下載 OpenAPI 規格
def get_openapi_spec():
    url = 'https://openapi.twse.com.tw/v1/swagger.json'
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# Step 2: 分類為 DAG 結構
def parse_openapi_to_dags(openapi_spec):
    dag_structure = defaultdict(list)
    for path, methods in openapi_spec['paths'].items():
        for method, details in methods.items():
            tags = details.get('tags', [])
            summary = details.get('summary', '')
            operation_id = details.get('operationId', f"{method.upper()} {path}")
            for tag in tags:
                dag_structure[tag].append({
                    'path': path,
                    'method': method.upper(),
                    'summary': summary,
                    'operation_id': operation_id
                })
    return dag_structure

# Step 3: 輔助 - 安全命名
def sanitize_name(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name.strip().lower())

def convert_tag_to_dag_id(tag):
    import hashlib
    if tag in tag_en_map:
        return f"{tag_en_map[tag]}"
    else:
        # 對其他中文使用簡單 hash 防止錯誤
        hash_id = hashlib.md5(tag.encode()).hexdigest()[:6]
        return f"{hash_id}"

tag_en_map = {
    '公司治理': 'corporate_governance',
    '其他': 'others',
    '券商資料': 'broker_data',
    '財務報表': 'financial_statements',
    '證券交易': 'securities_trading',
    '指數': 'indices',
    '權證': 'warrants',
}

# Step 4: 產出每個 DAG 的 Python 程式碼
def generate_airflow_dag_py(tag, tasks):
    tag_en = convert_tag_to_dag_id(tag)
    dag_id = f"openapi_twse_{tag_en}"
    dag_py = f'''
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
    url = f"https://openapi.twse.com.tw/v1{{path}}"
    response = requests.request(method, url)
    response.raise_for_status()
    data = response.json()

    # 印出部分資料（方便除錯）
    print("data lens", len(data))
    print(json.dumps(data[:25], indent=2, ensure_ascii=False) if isinstance(data, list) else str(data))

    # 儲存到 MongoDB
    # collection = db_openapi_twse["{tag_en}"]

    # doc = {{
    #     "path": path,
    #     "method": method,
    #     "timestamp": datetime.now(),
    #     "execution_date": execution_date,
    #     "data": data
    # }}

    # from bson import json_util
    # import traceback

    # try:
    #     collection.insert_one(doc)
    # except Exception as e:
    #     print("Mongo insert error:", e)
    #     print("Doc preview:", json.dumps(doc, default=json_util.default)[:1000])
    #     traceback.print_exc()

@dag(
    dag_id="{dag_id}",
    schedule='0 0 * * 1-5',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={{
        'owner': 'ivan',
        'on_failure_callback': send_task_telegram_notification,
    }},
    tags=["twse", "{tag_en}"],
)
def fetch_{tag_en}():'''

    task_ids = []
    
    for task in tasks:
        task_id = sanitize_name(f"{task['method']}_{task['path']}")
        task_ids.append(task_id)
        dag_py += f'''
    # {task['summary']}
    {task_id} = PythonOperator(
        task_id="{task_id}",
        python_callable=call_twse_api,
        op_kwargs={{"path": "{task['path']}", "method": "{task['method']}"}},
    )\n'''
    
    # 串接任務（依序執行）
    if task_ids:
        dag_py += f"\n\n    {task_ids[0]}"
        for i in range(1, len(task_ids)):
            dag_py += f" >> {task_ids[i]}"
    
    # 任務平行執行（無依賴），或你可調整為有依賴
    dag_py += f'''
    # 定義 task 執行順序（平行執行）
    # return [{', '.join(task_ids)}]

# 實例化 DAG
dag = fetch_{tag_en}()
'''
    return dag_id, dag_py

# Step 5: 主程式 - 寫入檔案
if __name__ == "__main__":
    spec = get_openapi_spec()
    dags = parse_openapi_to_dags(spec)

    for tag, tasks in dags.items():
        dag_id, code = generate_airflow_dag_py(tag, tasks)
        filename = f"dags/{dag_id}.py"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(code)
        print(f"✅ Generated: {filename}")

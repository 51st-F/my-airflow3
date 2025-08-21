from airflow.sdk import Variable
from airflow.exceptions import AirflowFailException
import requests


from airflow.operators.python import get_current_context


def send_custom_telegram_message(message: str, parse_mode: str = "Markdown"):
    """
    發送自訂訊息到 Telegram。
    """
    bot_token = Variable.get("TG_BOT_AIRFLOW3")
    chat_id = "1188058900"

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode
    }

    try:
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            raise AirflowFailException(f"Telegram 發送失敗: {response.text}")
    except Exception as e:
        print(f"[Telegram 發送錯誤] {str(e)}")


def send_insert_success_notification(inserted_count: int):
    """
    發送插入成功訊息，含 DAG 執行資訊與筆數。
    """
    context = get_current_context()

    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    run_id = context['run_id']
    execution_date = context['ts']

    message = (
        f"✔️ *insert {inserted_count} rows*\n"
        f"`{run_id}`\n"
        f"Dag: `{dag_id}`\n"
    )

    send_custom_telegram_message(message)


def send_task_telegram_notification(context):
    """
    提供 on_failure_callback 使用的錯誤通知函式。
    """
    bot_token = Variable.get("TG_BOT_AIRFLOW3")
    chat_id = "1188058900"

    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    run_id = context['run_id']
    execution_date = context['ts']
    try_number = context['ti'].try_number
    state = context['ti'].state

    status_emoji = "✔️" if state == "success" else "❌"

    message = (
        f"{status_emoji} *fail notify*\n"
        f"`{run_id}`\n"
        f"*Dag*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Retry*: `{try_number}`"
    )

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            raise AirflowFailException(f"Telegram 發送失敗: {response.text}")
    except Exception as e:
        print(f"[通知錯誤] {str(e)}")

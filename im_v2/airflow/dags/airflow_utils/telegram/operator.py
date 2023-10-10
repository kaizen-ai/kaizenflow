import airflow
from airflow.providers.telegram.operators.telegram import TelegramOperator


def get_telegram_operator(
    dag: airflow.DAG, stage: str, dag_id: str, run_id: str
) -> TelegramOperator:
    tg_operator = TelegramOperator(
        task_id="send_telegram_notification_on_failure",
        # telegram_conn_id=f"{_STAGE}_telegram_conn",
        telegram_conn_id=f"{stage}_telegram_conn",
        text=f"DAG {dag_id} failed \nrun ID: {run_id}",
        dag=dag,
        trigger_rule="one_failed",
    )
    return tg_operator

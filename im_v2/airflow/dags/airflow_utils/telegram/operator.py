from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.models import Variable
#from airflow.configuration import conf

def get_telegram_operator(dag: DAG, stage: str, purpose: str, dag_id: str, run_id: str) -> TelegramOperator:
    """
    Get telegram notification operator.
    
    :param dag: DAG to assign the operator to
    :param stage: stage to run in (test, preprod, prod)
    :param purpose: what type of notification is this related to: datapull, or trading
    :param dag_id: Named of the DAG, to identify which DAG failed
    :param run_id: ID of the current dag run (to identify which specific run the notification belongs to)
    :return: Telegram operator assigned to the passed Airflow DAG
    """
    # Two currently supported types of notifications.
    assert purpose in ["datapull", "trading"]
    
    # Construct the variable name based on the stage and purpose
    chat_id_var_name = f"{stage}_{purpose}_telegram_room_id"

    # Retrieve the chat ID from Airflow variable
    chat_id = Variable.get(chat_id_var_name, default_var="-846237594")  # Handling the case when variable is not set

    #TODO(Juraj): hack, to make other group working, #CmTask7966.
    if chat_id_var_name == "test_trading_telegram_room_id":
        # For a group chat "100" needs to be prepended to the chat id 
        # e.g. -2017110768 -> -1002017110768
        chat_id = "-1002017110768"

    run_mode = "{{ 'scheduled' if 'scheduled' in run_id else 'manual' }}"
    #airflow_hostname = conf.get('webserver', 'base_url')
    # TODO(Juraj): Fix this.
    airflow_hostname = "http://172.30.2.114:8090"
    
    notification_message = f"DAG '{dag_id}' failed\nRun_mode: {run_mode}\n"
    notification_message += "DAG start timestamp: {{ dag_run.start_date }}\n\n"
    notification_message += f"Link:\n{airflow_hostname}/dags/{dag_id}\n"
    
    tg_operator = TelegramOperator(
        task_id="send_telegram_notification_on_failure",
        telegram_conn_id=f"{stage}_telegram_conn",  # The API token
        chat_id=chat_id,  # The dynamic chat ID
        text=notification_message,
        dag=dag,
        trigger_rule='one_failed'
    )
    return tg_operator

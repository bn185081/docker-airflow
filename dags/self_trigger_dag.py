from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

default_args = {"start_date": datetime(2021, 1, 1)}


def _start(**kwargs):
    print("Start Step")
    print(f"context params: {kwargs['params']}")
    print(f"conf: {kwargs['dag_run'].conf}")


def _get_update_date_period(**kwargs):
    print("Getting the period of time when the BQ changed")


def _regular_wf():
    print("Regular WF")


def _check_for_data_updates(**kwargs):
    print("Check for data Updates")
    print(f'AF context: {kwargs}')
    kwargs['ti'].xcom_push(key="data_update_start_date", value="2023-12-01")
    kwargs['ti'].xcom_push(key="data_update_end_date", value="2023-12-31")
    # return not kwargs["dag_run"].external_trigger
    return True

with DAG(
    "self_trigger_dag", schedule_interval="@daily", default_args=default_args, catchup=False
) as dag:
    start = PythonOperator(task_id="Start", python_callable=_start)

    regular_wf = PythonOperator(task_id="Regular_WF", python_callable=_regular_wf)

    get_dates = PythonOperator(
        task_id="GetUpdateDates", python_callable=_get_update_date_period
    )

    check_for_data_updates = ShortCircuitOperator(task_id='Check_Data_Updates', python_callable=_check_for_data_updates)

    trigger = TriggerDagRunOperator(
        task_id='Trigger_Self',
        trigger_dag_id=dag.dag_id,
        # trigger_run_id='dataUpdateTrigger__' + {{ ts }},  # TODO Brenda Check if I can give a special name in this case
        conf={"payload": "{{ task_instance.xcom_pull(task_ids='_check_for_data_updates') }}"},
    )

    start >> regular_wf >> get_dates >> check_for_data_updates >> trigger



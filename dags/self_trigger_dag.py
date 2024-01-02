from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

default_args = {"start_date": datetime(2021, 1, 1)}


def _start():
    print("Start Step")


def _get_update_date_period():
    print("Getting the period of time when the BQ changed")


def _regular_wf():
    print("Regular WF")


def _check_for_data_updates(**context):
    print("Check for data Updates")
    print(f'AF context: {context}')
    return not context["dag_run"].external_trigger

with DAG(
    "self_trigger_dag", schedule_interval="@daily", default_args=default_args, catchup=False
) as dag:
    start = PythonOperator(task_id="Start", python_callable=_start)

    regular_wf = PythonOperator(task_id="Regular_WF", python_callable=_regular_wf)

    get_dates = PythonOperator(
        task_id="GetUpdateDates", python_callable=_get_update_date_period
    )

    interval = BashOperator(task_id="Interval"
                            , bash_command="sleep 60"
                            , retries=1)

    check_for_data_updates = ShortCircuitOperator(task_id='Check_Data_Updates', python_callable=_check_for_data_updates)

    trigger = TriggerDagRunOperator(
        task_id='Trigger_Self',
        trigger_dag_id=dag.dag_id,
        # trigger_run_id='dataUpdateTrigger__{{ ts }}') # Check if I can give a special name in this case
        conf={'data_update_start_date': "2023-12-01", 'data_update_end_date': "2023-12-31"},
    )

    start >> regular_wf >> get_dates >> check_for_data_updates >> trigger

    # trigger_target = TriggerDagRunOperator(
    #     task_id="trigger_target",
    #     trigger_dag_id=target_dag_id,
    #     execution_date="2023-12-25T10:09:10.896248+00:00",
    #     reset_dag_run=True,
    #     wait_for_completion=True,  # To make sure the target dag finishes
    #     # deferrable=True, # Need to set something in the environment for this to work
    #     conf={},
    # )


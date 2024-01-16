from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import datetime

default_args = {"start_date": datetime.datetime(2021, 1, 1)}


def _handle_conf(conf=None):
    if conf is None:
        conf = {}
    print(f"Handling configuration: {conf}")
    data_start_date = conf.get('data_start_date') or ""
    data_end_date = conf.get('data_end_date') or ""
    if data_start_date and data_end_date:
        try:
            datetime.datetime.strptime(data_start_date, "%Y-%m-%d")
            datetime.datetime.strptime(data_end_date, "%Y-%m-%d")
            print(f"Data dates will be used in this workflow run. "
                  f"data_start_date={data_start_date}, data_end_date={data_end_date}")
        except ValueError:
            raise ValueError("data_start_date and data_end_date must be in format YYYY-MM-DD")
    return data_start_date, data_end_date

def _start(**kwargs):
    print("Start Step")
    print(f"context params: {kwargs['params']}")
    print(f"conf: {kwargs['dag_run'].conf}")
    print(f"dag_id: {kwargs['dag'].dag_id}")
    data_start_date, data_end_date = _handle_conf(conf=kwargs['dag_run'].conf)
    kwargs['ti'].xcom_push(key="data_start_date", value=data_start_date)
    kwargs['ti'].xcom_push(key="data_end_date", value=data_end_date)




def _get_update_date_period(**kwargs):
    print("Getting the period of time when the BQ changed")


def _regular_wf():
    print("Regular WF")


def _check_for_data_updates(**kwargs):
    print("Check for data Updates")
    print(f'AF context: {kwargs}')
    kwargs['ti'].xcom_push(key="data_start_date", value="2023-11-01")
    kwargs['ti'].xcom_push(key="data_end_date", value="2023-11-30")
    # return not kwargs["dag_run"].external_trigger
    return True


@dag(dag_id="self_trigger_dag", schedule_interval=None, default_args=default_args, catchup=False)
def workflow_dag():
    start = PythonOperator(task_id="start", python_callable=_start)

    regular_wf = PythonOperator(task_id="regular_wf", python_callable=_regular_wf)

    get_dates = PythonOperator(
        task_id="get_dates", python_callable=_get_update_date_period
    )

    check_for_data_updates = ShortCircuitOperator(task_id='check_for_data_updates',
                                                  python_callable=_check_for_data_updates)

    trigger_self = TriggerDagRunOperator(
        task_id='trigger_self',
        trigger_dag_id="{{ dag.dag_id }}",
        # trigger_run_id="{{ dataUpdateTrigger__ts }}",  # TODO Brenda Check if I can give a special name in this case
        conf={
            "data_start_date": "{{ ti.xcom_pull(task_ids='check_for_data_updates', key='data_start_date') }}",
            "data_end_date": "{{ ti.xcom_pull(task_ids='check_for_data_updates', key='data_end_date') }}"
        },
        # conf={
        #     "data_start_date": "2023-12-01",
        #     "data_end_date": "2023-12-31"
        # },
    )

    start >> regular_wf >> get_dates >> check_for_data_updates >> trigger_self

dag = workflow_dag()

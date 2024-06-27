from airflow import DAG, XComArg
from airflow.decorators import dag
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

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
    elif data_start_date or data_end_date:
        raise ValueError("Only data_start_date OR data_end_date was provided, you must include both for the DAG to be triggered")
    return data_start_date, data_end_date

def _start(**kwargs):
    print("Start Step")
    print(f"context params: {kwargs['params']}")
    print(f"conf: {kwargs['dag_run'].conf}")
    print(f"dag_id: {kwargs['dag'].dag_id}")
    data_start_date, data_end_date = _handle_conf(conf=kwargs['dag_run'].conf)
    kwargs['ti'].xcom_push(key="data_start_date", value=data_start_date)
    kwargs['ti'].xcom_push(key="data_end_date", value=data_end_date)




def _run_sl_trigger_wf_step(**kwargs):
    print("Running SL trigger workflow step.  Get dates from BQ and run condition, save files to GCS")


def _regular_wf():
    print("Regular WF")


def _check_for_data_updates(wf_run_id: str, **kwargs):
    print("Check for data Updates")
    print(f'AF context: {kwargs}')
    print(f'wf_run_id: {wf_run_id}')

    return True


def _get_trigger_dates(wf_run_id: str, **kwargs):
    print("Get Trigger dates")
    print(f'wf_run_id: {wf_run_id}')

    dates_list = [
        {
            "start_date": "2023-12-01",
            "end_date": "2023-12-02"
        },
        {
            "start_date": "2023-12-10",
            "end_date": "2023-12-10"
        },
        {
            "start_date": "2024-02-05",
            "end_date": "2024-02-06"
        }
    ]

    return dates_list


@dag(dag_id="self_trigger_dag", schedule_interval=None, default_args=default_args, catchup=False)
def workflow_dag():
    start = PythonOperator(task_id="start", python_callable=_start)

    regular_wf = PythonOperator(task_id="regular_wf", python_callable=_regular_wf)

    with TaskGroup("my_task_group", tooltip="Tasks for {{ task_id }}") as my_task_group:
        run_SL_trigger_wf_step = PythonOperator(
            task_id="run_SL_trigger_wf_step", python_callable=_run_sl_trigger_wf_step
        )

        check_for_data_updates = ShortCircuitOperator(task_id='check_for_data_updates',
                                                      python_callable=_check_for_data_updates,
                                                      op_kwargs={'wf_run_id': "my_run_id"})

        get_trigger_dates = PythonOperator(
            task_id="get_trigger_dates", python_callable=_get_trigger_dates, op_kwargs={'wf_run_id': "my_run_id"}
        )

        trigger_self = TriggerDagRunOperator.partial(
            task_id='trigger_self',
            trigger_dag_id="{{ dag.dag_id }}",
        ).expand(
            conf=XComArg(get_trigger_dates)
        )
        run_SL_trigger_wf_step >> check_for_data_updates >> get_trigger_dates >> trigger_self

    start >> regular_wf >> my_task_group

dag = workflow_dag()

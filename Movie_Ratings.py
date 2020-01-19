from core.execute import load_data_core, aggregate_data_core, insert_db_core
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

Schedule_Interval = None

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2020, 1, 19),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG('Movie_Dag',
             default_args=default_args,
             schedule_interval=Schedule_Interval,
             concurrency=1,
          max_active_runs=1)

load_data = PythonOperator(
    task_id="load_data",
    python_callable=load_data_core,
    retries=0,
    provide_context=True,
    dag = dag
    )

aggregate_data = PythonOperator(
    task_id="aggregate_data",
    python_callable=aggregate_data_core,
    retries=0,
    provide_context=True,
    dag = dag
    )

insert_db_core = PythonOperator(
    task_id = "insert_db_core",
    python_callable = insert_db_core,
    retries = 0,
    provide_context = True,
    dag = dag
    )


load_data >> aggregate_data >> insert_db_core
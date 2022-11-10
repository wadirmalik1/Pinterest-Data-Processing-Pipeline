from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Wadir',
    'depends_on_past': False,
    'email': ['wadirmalik@hotmail.co.uk'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2020, 1, 1),
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id='pintrest_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 0 * * *") as dag:

    test_task = BashOperator(
        task_id='pintrest_pyspark',
        bash_command='python /Users/wadirmalik/Desktop/pintrest_data_collection/pintrest_pyspark.py',
        dag=dag)
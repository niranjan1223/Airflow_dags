from airflow import DAG
from datetime import datetime,timedelta

from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'niranjan',
    'retires' : 5,
    'retry-delay' : timedelta(minutes=2)
}


with DAG(
    dag_id = 'first-dag',
    default_args = default_args,
    description  = 'sample dag',
    start_date   = datetime(2023,1,24),
    schedule_interval = '@daily',
    catchup=False
    )as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo 'Hello World'",
    )
    task1

  

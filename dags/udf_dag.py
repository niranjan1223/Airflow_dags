from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'diggibyte',
    'retires' : 1,
    'retry-delay' : timedelta(minutes=1)}

def function_execute():
    p = print('Hello world')
    return p


with DAG(
    dag_id='udf_dag',
    default_args=default_args,
    description='sample dag',
    start_date=datetime(2023,1,24),
    schedule_interval='@monthly',
    catchup=False
    )as dag:
    function_execute=PythonOperator(
        task_id='function_execute',
        python_callable=function_execute,)
 

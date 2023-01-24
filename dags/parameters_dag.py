from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'diggibyte',
    'retires' : 1,
    'retry-delay' : timedelta(minutes=1)}

def function_execute(*args,**kwargs):
    variable = kwargs.get('name','didnt get the key')
    print('Hello world :{}'.format(variable))
    return "Hello world" + variable

with DAG(
    dag_id='parameter_dag',
    default_args=default_args,
    description='sample dag',
    start_date=datetime(2023,1,24),
    schedule_interval='@monthly',
    catchup=False
    )as dag:
    passing_args=PythonOperator(
        task_id='function_execute',
        python_callable=function_execute,
        op_kwargs={'name':'data engineer'}
    )
    passing_args  

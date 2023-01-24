from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'diggibyte',
    'retires' : 1,
    'retry-delay' : timedelta(minutes=1)}

def function_execute(**context):
    print('function_execute')
    context['ti'].xcom_push(key='mykey',value='function_execute says hello')

def second_function_execute(**context):
    instance= context.get('ti').xcom_pull(key='mykey')
    print('iam in second_function_execute got value :{}from function 1'.format(instance))

with DAG(
    dag_id='parameter_dag',
    default_args=default_args,
    description='sample dag',
    start_date=datetime(2023,1,24),
    schedule_interval='@monthly',
    catchup=False
    )as dag:
    function_execute=PythonOperator(
        task_id='function_execute',
        python_callable=function_execute,
        provide_context= True,
        op_kwargs={'name':'data engineer'}
    )
    second_function_execute=PythonOperator(
        task_id='second_function_execute',
        python_callable=second_function_execute,
        provide_context=True,
    )
function_execute >> second_function_execute 

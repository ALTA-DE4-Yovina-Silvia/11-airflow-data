from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator

# 1. Create DAG that run in every 5 hours.
dag = DAG(
    'yovina-airflow-task1', 
    description='Airflow Task 1',
    schedule_interval='0 */5 * * *',
    start_date=datetime(2023, 10, 18), 
    catchup=False
)
    
start = EmptyOperator(
    task_id='start',
    dag=dag,
)
# ti = task instance
# 2. Suppose we define a new task that push a variable to xcom.
def push_variable_to_xcom(ti=None):
    ti.xcom_push(key='job_role', value='Backend Engineer')
    ti.xcom_push(key='job_role_1', value='Data Engineer')
    ti.xcom_push(key='job_role_2', value='Frontend Engineer')
    ti.xcom_push(key='job_role_3', value='Quality Assurance')

# 3. How to pull multiple values at once?
def pull_multiple_value_once(ti=None):
    job_role = ti.xcom_pull(task_ids='push_var_job_role', key='job_role')
    job_role_1 = ti.xcom_pull(task_ids='push_var_job_role', key='job_role_1')
    job_role_2 = ti.xcom_pull(task_ids='push_var_job_role', key='job_role_2')
    job_role_3 = ti.xcom_pull(task_ids='push_var_job_role', key='job_role_3')

    print(f'print job_role variable from xcom: {job_role}, {job_role_1}, {job_role_2}, {job_role_3}')

push_variable_to_xcom = PythonOperator(
    task_id = 'push_variable_to_xcom',
    python_callable = push_variable_to_xcom
)

pull_multiple_value_once = PythonOperator(
    task_id = 'pull_multiple_value_once',
    python_callable = pull_multiple_value_once
)

start >> push_variable_to_xcom >> pull_multiple_value_once
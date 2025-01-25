from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'first_dag',  # name of pipeline should equal file name
    default_args=default_args,
    description='My first Airflow DAG with an empty operator',
    schedule_interval=timedelta(days=1),  # pipeline run frequency
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def hello_world():
    print("hello world")

# Dummy intermediate tasks
def intermediate_task():
    print("Intermediate task")

def second_intermediate_task():
    print("Second intermediate task")

# Operators
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

hello_world_task = PythonOperator(
    task_id='hello_world',
    dag=dag,
    python_callable=hello_world,
)

intermediate_task_op = PythonOperator(
    task_id='intermediate_task',
    dag=dag,
    python_callable=intermediate_task,
)

second_intermediate_task_op = PythonOperator(
    task_id='second_intermediate_task',
    dag=dag,
    python_callable=second_intermediate_task,
)

send_email = EmptyOperator(
    task_id='send_email',
    dag=dag,
)

send_ms_team = EmptyOperator(
    task_id='send_ms_team',
    dag=dag,
)

send_line_message = EmptyOperator(
    task_id='send_line_message',
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Dependencies
start >> hello_world_task >> end

start >> [intermediate_task_op,second_intermediate_task_op]  >> end >> [send_ms_team,send_line_message]  

start >> second_intermediate_task_op >> send_email

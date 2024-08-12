from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

#Define default arguments
default_args = {
 'owner': 'your_name_2116',
 'start_date': datetime (2023, 9, 29),
 'retries': 1,
}

# Instantiate your DAG
dag = DAG ('my_first_dag', default_args=default_args, schedule_interval=None)

# Define tasks
def task1():
 print ("Executing Task 1")

def task2():
 print ("Executing Task 2")

task_1 = PythonOperator(
 task_id='task_1',
 python_callable=task1,
 dag=dag,
)
task_2 = PythonOperator(
 task_id='task_2',
 python_callable=task2,
 dag=dag,
)

# Tarefa de início
start = DummyOperator(
    task_id='start'
)
    
# Tarefa de fim
end = DummyOperator(
    task_id='end'
)


# Set task dependencies
start >> task_1 >> task_2 >> end
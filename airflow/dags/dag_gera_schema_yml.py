# dags/1_dag_gera_schema_yml.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

# Importa a função que vai ser chamada pelo PythonOperator
from dags.gera_schema_yml import run_script

# Define os argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 10),
    'retries': 1,
}

# Define a DAG
dag = DAG(
    dag_id='generate_schema_dag',
    default_args=default_args,
    schedule_interval=None,  # DAG sem agendamento; será executada manualmente
    catchup=False,  # Evita execução retroativa
    tags=['dbt', 'yml'],  # Tags para identificar a DAG
)

# Tarefa de início
start = DummyOperator(
    task_id='start',
    dag=dag,
)
    
# Tarefa de fim
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define a tarefa PythonOperator
generate_schema_task = PythonOperator(
    task_id='generate_schema',
    python_callable=run_script,
    dag=dag,
)

# Define a ordem das tarefas
start >> generate_schema_task >> end

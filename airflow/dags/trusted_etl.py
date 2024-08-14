from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Defina o DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_run_dag',
    default_args=default_args,
    description='A DAG to run dbt models',
    schedule_interval=None,  # Executa diariamente às 8:00 AM
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Tarefa de início
    start = DummyOperator(
        task_id='start'
    )
    
    # Tarefa de fim
    end = DummyOperator(
        task_id='end'
    )

    # Tarefa para rodar dbt
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /Users/janpaulosalvador/poc/stack3_090824/airflow-demo && dbt run',  # Caminho ajustado para o diretório do seu projeto dbt
    )

    # Define a ordem das tarefas, se houver mais
    start >> run_dbt >> end

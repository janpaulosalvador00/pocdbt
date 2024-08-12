import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from datetime import timedelta

# Obtém o caminho do projeto DBT a partir de uma variável de ambiente
dbt_project_path = os.getenv('DBT_PROJECT_PATH', '/path/to/your/default/dbt/project')
profiles_dir_path = os.getenv('DBT_PROFILES_DIR', '/path/to/your/default/profiles/dir')

# Definições dos parâmetros padrão e configuração do DAG
default_args = {
    'owner': 'Jan_2156',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='raw_trusted',
    default_args=default_args,
    schedule_interval=None,  # DAG sem agendamento; será executada manualmente
    start_date=days_ago(1),  # Data no passado para garantir que a DAG possa ser executada manualmente
    catchup=False,  # Evita execução retroativa
    tags=['raw','trusted'],
) as dag:

    # Tarefa de início
    start = DummyOperator(
        task_id='start'
    )
    
    # Tarefa de fim
    end = DummyOperator(
        task_id='end'
    )

    # Aciona a sincronização assíncrona com Airbyte
    async_money_to_json = AirbyteTriggerSyncOperator(
        task_id='airbyte_async',
        airbyte_conn_id='airbyte',
        connection_id='bca6afe7-f11e-4813-9458-aeeb22dd5a8c',
        asynchronous=True,
    )

    # Sensor para monitorar o trabalho do Airbyte
    airbyte_sensor = AirbyteJobSensor(
        task_id='airbyte_sensor',
        airbyte_conn_id='airbyte',
        airbyte_job_id=async_money_to_json.output
    )

    # Definindo a ordem de execução das tarefas
    start >> async_money_to_json >> airbyte_sensor >> end
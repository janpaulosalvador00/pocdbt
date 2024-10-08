import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.operators.bash import BashOperator 
from datetime import timedelta

# Obtém o caminho do projeto DBT a partir de uma variável de ambiente
dbt_project_path = os.getenv('DBT_PROJECT_PATH', '/path/to/your/default/dbt/project')
profiles_dir_path = os.getenv('DBT_PROFILES_DIR', '/path/to/your/default/profiles/dir')

# Definições dos parâmetros padrão e configuração do DAG
default_args = {
    'owner': 'Jan_0917',
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
        connection_id='fba4b160-7965-428b-a170-9dd78bc6eb59',
        asynchronous=True,
    )

    # Sensor para monitorar o trabalho do Airbyte
    airbyte_sensor = AirbyteJobSensor(
        task_id='airbyte_sensor',
        airbyte_conn_id='airbyte',
        airbyte_job_id=async_money_to_json.output
    )

    # Chamada do DBT
    # Define the dbt run command as a BashOperator
    run_dbt_model = BashOperator(
        task_id='run_dbt_model',
        bash_command='dbt run',
        dag=dag
    )

    # Definindo a ordem de execução das tarefas
    start >> async_money_to_json >> airbyte_sensor >> run_dbt_model >> end
    #start >> async_money_to_json >> airbyte_sensor >> end
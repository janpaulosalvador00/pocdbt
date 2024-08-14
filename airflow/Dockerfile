FROM quay.io/astronomer/astro-runtime:11.8.0

# Crie a pasta .dbt
#RUN mkdir -p /home/astro/.dbt

COPY requirements.txt  ./

#cria a pasta dentro do container airflow
COPY dags/dbt/  /usr/local/airflow/ 


USER root

# substitua dbt-postgres por outro adaptador suportado se estiver usando um tipo de warehouse diferente
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate


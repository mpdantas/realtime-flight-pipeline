# flight_pipeline_dag.py

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
# Vamos usar o DockerOperator, pois nosso job está em um contêiner Docker
from airflow.operators.docker_operator import DockerOperator

# Define a DAG (o nosso pipeline)
with DAG(
    dag_id="flight_data_pipeline",
    start_date=pendulum.datetime(2025, 7, 22, tz="America/Sao_Paulo"),
    schedule_interval="*/15 * * * *", # Executa a cada 15 minutos
    catchup=False,
    doc_md="""
    ### Pipeline de Dados de Voos
    
    Orquestra a execução do job Spark para processar dados de voos em tempo real.
    - **Fonte de dados**: OpenSky Network API
    - **Processamento**: Spark em um contêiner Docker
    """,
    tags=["data-engineering", "spark", "docker"],
) as dag:
    
    # Define a nossa única tarefa
    process_spark_job = DockerOperator(
        task_id="run_spark_job_container",
        # Nome da imagem Docker que construímos na Fase 1
        image="realtime-flight-pipeline/spark-job:0.1",
        # Equivalente a 'docker run'. O Airflow se encarrega disso.
        command="/opt/bitnami/spark/bin/spark-submit /app/process_flights.py",
        # Garante que o contêiner será removido após a execução
        auto_remove=True,
        # Essencial para que o DockerOperator funcione quando o Airflow
        # também está rodando em Docker.
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )
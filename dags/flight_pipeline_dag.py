# dags/flight_pipeline_dag.py

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.docker_operator import DockerOperator

# Define a DAG (o nosso pipeline)
with DAG(
    # ID único para a nossa DAG na UI do Airflow
    dag_id="flight_data_pipeline",
    
    # Data a partir da qual a DAG se torna elegível para execução
    start_date=pendulum.datetime(2025, 7, 22, tz="America/Sao_Paulo"),
    
    # Frequência de execução em formato cron: "a cada 15 minutos"
    schedule_interval="*/15 * * * *",
    
    # Impede que o Airflow execute agendamentos passados que foram perdidos
    catchup=False,
    
    # Documentação que aparecerá na UI do Airflow
    doc_md="""
    ### Pipeline de Dados de Voos
    
    Orquestra a execução do job Spark para processar dados de voos em tempo real.
    - **Fonte de dados**: OpenSky Network API
    - **Processamento**: Spark num contentor Docker
    """,
    
    # Etiquetas para ajudar a organizar e filtrar as DAGs
    tags=["data-engineering", "spark", "docker"],
) as dag:
    
    # Define a nossa única tarefa usando o DockerOperator
    process_spark_job = DockerOperator(
        # ID único para a tarefa dentro da DAG
        task_id="run_spark_job_container",
        
        # A imagem Docker que queremos executar. Usamos a versão mais recente.
        image="realtime-flight-pipeline/spark-job:0.8",
        
        # Comando a ser executado dentro do contentor
        command="/opt/bitnami/spark/bin/spark-submit /app/process_flights.py",
        
        # Remove o contentor automaticamente após a sua conclusão
        auto_remove=True,
        
        # URL para o socket do Docker, necessário para o Airflow controlar o Docker
        docker_url="unix://var/run/docker.sock",
        
        # Modo de rede "host". Isto remove o isolamento de rede do contentor,
        # permitindo-lhe aceder a 'localhost' como se estivesse a ser executado diretamente na sua máquina.
        # Esta foi a nossa solução final para os problemas de conectividade de rede.
        network_mode="host"
    )

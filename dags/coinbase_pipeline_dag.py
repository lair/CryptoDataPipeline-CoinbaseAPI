from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Importa as funções responsáveis por cada etapa do pipeline (bronze -> silver -> gold)
from src.extract.coinbase_extractor import extract_coinbase_data #bronze
from src.transform.bronze_to_silver import transform_bronze_to_silver #silver
from src.load.silver_to_gold import load_silver_to_gold #gold


# Lista de pares de moedas que serão monitorados/extraídos em todas as etapas do pipeline.
# Essa lista é passada como parâmetro para as três tasks, garantindo consistência entre elas.
CURRENCY_PAIRS = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "USDT-USDC", "USDC-BRL"]

# Configurações padrão aplicadas a todas as tasks da DAG
default_args = {
    "owner": "lair",              # dono/responsável pela DAG
    "depends_on_past": False,     # a execução de uma task não depende do sucesso da execução anterior
    "retries": 2,                 # número de tentativas em caso de falha
    "retry_delay": timedelta(minutes=5),  # tempo de espera entre tentativas
}

# Definição da DAG (o pipeline em si)
with DAG(
    dag_id="coinbase_pipeline_dag",  # identificador único da DAG no Airflow
    description="Pipeline de extração, transformação e carga de cotações da Coinbase (BTC, ETH, SOL para USDT)",
    default_args=default_args,       # aplica as configs padrão definidas acima
    start_date=datetime(2026, 8, 1), # data a partir da qual a DAG pode começar a rodar
    schedule_interval="*/15 * * * *",  # expressão cron: executa a cada 15 minutos
    catchup=False,                   # não executa runs retroativas perdidas entre start_date e hoje
    tags=["coinbase", "crypto", "medallion"],  # tags usadas para organizar/filtrar DAGs na UI do Airflow
) as dag:
    
    # Task 1 - Extração (camada Bronze)
    # Chama a função que busca os dados brutos das cotações na API da Coinbase
    extract_task = PythonOperator(
        task_id="extract_bronze",
        python_callable=extract_coinbase_data,
        op_kwargs={"pairs": CURRENCY_PAIRS},
    )

    # Task 2 - Transformação (camada Silver)
    # Pega os dados brutos (bronze) e aplica limpeza/normalização, gerando os dados da camada silver
    transform_task = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_bronze_to_silver,
        op_kwargs={"pairs": CURRENCY_PAIRS},
    )
    # Task 3 - Carga (camada Gold)
    # Pega os dados já tratados (silver) e os agrega/carrega no destino final (camada gold), geralmente pronta para consumo em relatórios/dashboards
    load_task = PythonOperator(
        task_id="load_gold",
        python_callable=load_silver_to_gold,
        op_kwargs={"pairs": CURRENCY_PAIRS},
    )
    
    # Define a ordem de execução das tasks: extract -> transform -> load
    # O operador ">>" indica dependência sequencial (uma só começa após a anterior terminar)
    extract_task >> transform_task >> load_task
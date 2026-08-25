from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract.coinbase_extractor import extract_coinbase_data
from src.transform.bronze_to_silver import transform_bronze_to_silver
from src.load.silver_to_gold import load_silver_to_gold


# Pares de moedas monitorados pelo pipeline
CURRENCY_PAIRS = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]

default_args = {
    "owner": "lair",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="coinbase_pipeline_dag",
    description="Pipeline de extração, transformação e carga de cotações da Coinbase (BTC, ETH, SOL para USDT)",
    default_args=default_args,
    start_date=datetime(2026, 8, 1),
    schedule_interval="*/15 * * * *",  # a cada 15 minutos
    catchup=False,
    tags=["coinbase", "crypto", "medallion"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_bronze",
        python_callable=extract_coinbase_data,
        op_kwargs={"pairs": CURRENCY_PAIRS},
    )

    transform_task = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_bronze_to_silver,
        op_kwargs={"pairs": CURRENCY_PAIRS},
    )

    load_task = PythonOperator(
        task_id="load_gold",
        python_callable=load_silver_to_gold,
        op_kwargs={"pairs": CURRENCY_PAIRS},
    )

    extract_task >> transform_task >> load_task
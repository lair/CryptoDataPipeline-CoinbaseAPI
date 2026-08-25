import os
import logging
from io import BytesIO
from datetime import datetime, timezone

import pandas as pd
from minio import Minio
from minio.error import S3Error
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

SILVER_BUCKET_NAME = os.getenv("SILVER_BUCKET_NAME", "silver")


def _get_minio_client() -> Minio:
    """Cria e retorna um client MinIO configurado via variáveis de ambiente."""
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def _get_postgres_engine():
    """Cria e retorna uma engine SQLAlchemy para o Postgres (camada Gold)."""
    host = os.getenv("DWH_POSTGRES_HOST")
    port = os.getenv("DWH_POSTGRES_PORT")
    db = os.getenv("DWH_POSTGRES_DB")
    user = os.getenv("DWH_POSTGRES_USER")
    password = os.getenv("DWH_POSTGRES_PASSWORD")

    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(connection_string)


def _read_silver_object(client: Minio, object_path: str) -> pd.DataFrame:
    """Lê um arquivo Parquet da camada Silver e retorna como DataFrame."""
    response = client.get_object(SILVER_BUCKET_NAME, object_path)
    try:
        raw_bytes = response.read()
        return pd.read_parquet(BytesIO(raw_bytes), engine="pyarrow")
    finally:
        response.close()
        response.release_conn()


def _upsert_cotacao_atual(connection, record: dict) -> None:
    """Insere ou atualiza o preço mais recente do par na tabela gold.cotacoes_atuais."""
    query = text("""
        INSERT INTO gold.cotacoes_atuais (pair, price, bid, ask, volume_24h, exchange_time, updated_at)
        VALUES (:pair, :price, :bid, :ask, :volume_24h, :exchange_time, :updated_at)
        ON CONFLICT (pair)
        DO UPDATE SET
            price = EXCLUDED.price,
            bid = EXCLUDED.bid,
            ask = EXCLUDED.ask,
            volume_24h = EXCLUDED.volume_24h,
            exchange_time = EXCLUDED.exchange_time,
            updated_at = EXCLUDED.updated_at
    """)
    connection.execute(query, record)


def _insert_cotacao_historico(connection, record: dict) -> None:
    """Registra o preço do par na tabela histórica gold.cotacoes_historico (append)."""
    query = text("""
        INSERT INTO gold.cotacoes_historico (pair, price, volume_24h, extracted_at)
        VALUES (:pair, :price, :volume_24h, :extracted_at)
    """)
    connection.execute(query, record)


def _upsert_variacao_percentual(connection, pair: str) -> None:
    """
    Calcula a variação percentual do preço atual em relação ao registro
    histórico imediatamente anterior e grava em gold.variacao_percentual.
    """
    query = text("""
        WITH ultimos_precos AS (
            SELECT price, extracted_at
            FROM gold.cotacoes_historico
            WHERE pair = :pair
            ORDER BY extracted_at DESC
            LIMIT 2
        ),
        atual AS (
            SELECT price AS price_atual FROM ultimos_precos ORDER BY extracted_at DESC LIMIT 1
        ),
        anterior AS (
            SELECT price AS price_anterior FROM ultimos_precos ORDER BY extracted_at DESC OFFSET 1 LIMIT 1
        )
        INSERT INTO gold.variacao_percentual (pair, price_atual, price_anterior, variacao_percentual, calculado_em)
        SELECT
            :pair,
            atual.price_atual,
            anterior.price_anterior,
            CASE
                WHEN anterior.price_anterior = 0 OR anterior.price_anterior IS NULL THEN NULL
                ELSE ROUND(((atual.price_atual - anterior.price_anterior) / anterior.price_anterior) * 100, 4)
            END,
            NOW()
        FROM atual, anterior
        ON CONFLICT (pair)
        DO UPDATE SET
            price_atual = EXCLUDED.price_atual,
            price_anterior = EXCLUDED.price_anterior,
            variacao_percentual = EXCLUDED.variacao_percentual,
            calculado_em = EXCLUDED.calculado_em
    """)
    connection.execute(query, {"pair": pair})


def load_silver_to_gold(pairs: list[str], **context) -> None:
    """
    Lê os arquivos Parquet da camada Silver (referenciados via XCom pela task de transformação),
    grava o preço atual e o histórico no Postgres, e calcula a variação percentual por par.
    """
    ti = context["ti"]
    silver_object_paths = ti.xcom_pull(task_ids="transform_silver")

    if not silver_object_paths:
        raise ValueError("Nenhum caminho de arquivo Silver recebido via XCom da task de transformação.")

    minio_client = _get_minio_client()
    engine = _get_postgres_engine()

    for pair in pairs:
        silver_object_path = silver_object_paths.get(pair)
        if not silver_object_path:
            logger.warning("Nenhum arquivo Silver encontrado para o par '%s'. Pulando.", pair)
            continue

        try:
            df = _read_silver_object(minio_client, silver_object_path)
            row = df.iloc[0]

            record = {
                "pair": row["pair"],
                "price": float(row["price"]),
                "bid": float(row["bid"]),
                "ask": float(row["ask"]),
                "volume_24h": float(row["volume_24h"]),
                "exchange_time": row.get("exchange_time"),
                "extracted_at": row.get("extracted_at"),
                "updated_at": datetime.now(timezone.utc),
            }

            with engine.begin() as connection:
                _upsert_cotacao_atual(connection, record)
                _insert_cotacao_historico(connection, {
                    "pair": record["pair"],
                    "price": record["price"],
                    "volume_24h": record["volume_24h"],
                    "extracted_at": record["extracted_at"],
                })
                _upsert_variacao_percentual(connection, record["pair"])

            logger.info("Camada Gold atualizada com sucesso para o par '%s'.", pair)

        except S3Error as error:
            logger.error("Erro ao ler dados de '%s' no MinIO: %s", pair, error)
            raise
        except (KeyError, ValueError, TypeError) as error:
            logger.error("Erro ao processar dados de '%s' para a camada Gold: %s", pair, error)
            raise
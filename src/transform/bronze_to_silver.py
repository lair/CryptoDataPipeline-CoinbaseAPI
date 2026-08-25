import os
import json
import logging
from io import BytesIO

import pandas as pd
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

BRONZE_BUCKET_NAME = os.getenv("BRONZE_BUCKET_NAME", "bronze")
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


def _ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    """Garante que o bucket de destino exista no MinIO, criando-o se necessário."""
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info("Bucket '%s' criado no MinIO.", bucket_name)
    except S3Error as error:
        logger.error("Erro ao verificar/criar bucket '%s': %s", bucket_name, error)
        raise


def _read_bronze_object(client: Minio, object_path: str) -> dict:
    """Lê e desserializa um objeto JSON da camada Bronze."""
    response = client.get_object(BRONZE_BUCKET_NAME, object_path)
    try:
        raw_bytes = response.read()
        return json.loads(raw_bytes)
    finally:
        response.close()
        response.release_conn()


def _normalize_ticker(raw_data: dict, pair: str) -> dict:
    """
    Normaliza o payload bruto do ticker da Coinbase para um schema padronizado.
    Campos defensivos com .get() pois a API pode omitir algum valor eventualmente.
    """
    return {
        "pair": pair,
        "price": float(raw_data.get("price", 0.0)),
        "bid": float(raw_data.get("bid", 0.0)),
        "ask": float(raw_data.get("ask", 0.0)),
        "volume_24h": float(raw_data.get("volume", 0.0)),
        "trade_id": raw_data.get("trade_id"),
        "exchange_time": raw_data.get("time"),
        "extracted_at": raw_data.get("extracted_at"),
    }


def _build_silver_object_path(bronze_object_path: str) -> str:
    """Converte o caminho do objeto Bronze (.json) para o caminho equivalente na Silver (.parquet)."""
    return bronze_object_path.rsplit(".", 1)[0] + ".parquet"


def transform_bronze_to_silver(pairs: list[str], **context) -> dict:
    """
    Lê os arquivos JSON brutos da camada Bronze (referenciados via XCom pela task de extração),
    normaliza os dados e grava em formato Parquet na camada Silver do MinIO.
    """
    ti = context["ti"]
    bronze_object_paths = ti.xcom_pull(task_ids="extract_bronze")

    if not bronze_object_paths:
        raise ValueError("Nenhum caminho de arquivo Bronze recebido via XCom da task de extração.")

    client = _get_minio_client()
    _ensure_bucket_exists(client, SILVER_BUCKET_NAME)

    silver_object_paths = {}

    for pair in pairs:
        bronze_object_path = bronze_object_paths.get(pair)
        if not bronze_object_path:
            logger.warning("Nenhum arquivo Bronze encontrado para o par '%s'. Pulando.", pair)
            continue

        try:
            raw_data = _read_bronze_object(client, bronze_object_path)
            normalized_record = _normalize_ticker(raw_data, pair)

            df = pd.DataFrame([normalized_record])

            buffer = BytesIO()
            df.to_parquet(buffer, engine="pyarrow", index=False)
            buffer.seek(0)

            silver_object_path = _build_silver_object_path(bronze_object_path)

            client.put_object(
                bucket_name=SILVER_BUCKET_NAME,
                object_name=silver_object_path,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream",
            )

            silver_object_paths[pair] = silver_object_path
            logger.info("Dados de '%s' transformados e gravados em '%s/%s'.", pair, SILVER_BUCKET_NAME, silver_object_path)

        except S3Error as error:
            logger.error("Erro ao processar dados de '%s' no MinIO: %s", pair, error)
            raise
        except (KeyError, ValueError, TypeError) as error:
            logger.error("Erro ao normalizar dados de '%s': %s", pair, error)
            raise

    return silver_object_paths
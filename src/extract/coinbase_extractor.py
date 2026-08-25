import os
import json
import logging
from datetime import datetime, timezone
from io import BytesIO

import requests
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

BRONZE_BUCKET_NAME = os.getenv("BRONZE_BUCKET_NAME", "bronze")


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


def _fetch_ticker(pair: str) -> dict:
    """Consulta o endpoint de ticker da Coinbase para um par de moedas."""
    base_url = os.getenv("COINBASE_API_BASE_URL")
    endpoint_template = os.getenv("COINBASE_TICKER_ENDPOINT")
    endpoint = endpoint_template.format(pair=pair)
    url = f"{base_url}{endpoint}"

    response = requests.get(url, timeout=10)
    response.raise_for_status()

    return response.json()


def _build_object_path(pair: str, timestamp: datetime) -> str:
    """Monta o caminho de particionamento no padrão bronze/coinbase/{par}/{ano}/{mes}/{dia}/{arquivo}.json"""
    return (
        f"coinbase/{pair}/"
        f"{timestamp.strftime('%Y')}/{timestamp.strftime('%m')}/{timestamp.strftime('%d')}/"
        f"{pair}_{timestamp.strftime('%Y%m%dT%H%M%S')}.json"
    )


def extract_coinbase_data(pairs: list[str]) -> dict:
    """
    Extrai as cotações atuais dos pares informados na API da Coinbase
    e grava os dados brutos (JSON) na camada Bronze do MinIO.
    """
    client = _get_minio_client()
    _ensure_bucket_exists(client, BRONZE_BUCKET_NAME)

    extraction_timestamp = datetime.now(timezone.utc)
    object_paths = {}

    for pair in pairs:
        try:
            raw_data = _fetch_ticker(pair)
            raw_data["pair"] = pair
            raw_data["extracted_at"] = extraction_timestamp.isoformat()

            object_path = _build_object_path(pair, extraction_timestamp)
            payload = json.dumps(raw_data).encode("utf-8")

            client.put_object(
                bucket_name=BRONZE_BUCKET_NAME,
                object_name=object_path,
                data=BytesIO(payload),
                length=len(payload),
                content_type="application/json",
            )

            object_paths[pair] = object_path
            logger.info("Dados de '%s' gravados em '%s/%s'.", pair, BRONZE_BUCKET_NAME, object_path)

        except requests.RequestException as error:
            logger.error("Erro ao consultar API da Coinbase para '%s': %s", pair, error)
            raise
        except S3Error as error:
            logger.error("Erro ao gravar dados de '%s' no MinIO: %s", pair, error)
            raise

    return object_paths
import os
import json
import logging
from datetime import datetime, timezone
from io import BytesIO

import requests
from minio import Minio
from minio.error import S3Error

# Logger usado para registrar informações e erros durante a execução da extração
logger = logging.getLogger(__name__)

# Nome do bucket MinIO onde os dados brutos (bronze) serão armazenados.
# Pode ser sobrescrito pela variável de ambiente BRONZE_BUCKET_NAME; caso não exista, usa "bronze" como padrão.
BRONZE_BUCKET_NAME = os.getenv("BRONZE_BUCKET_NAME", "bronze")


def _get_minio_client() -> Minio:
    """
    Cria e retorna um client MinIO configurado via variáveis de ambiente.
    Lê as credenciais e endpoint do MinIO a partir das variáveis de ambiente
    """
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    # Instancia o client do MinIO com as credenciais obtidas
    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )
    # secure=False indica que a conexão não usa HTTPS (comum em ambientes locais/dev)

def _ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    """Garante que o bucket de destino exista no MinIO, criando-o se necessário."""
    try:
        # Verifica se o bucket já existe; se não existir, cria
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info("Bucket '%s' criado no MinIO.", bucket_name)
    except S3Error as error:
        # Caso ocorra algum erro na comunicação com o MinIO, registra e propaga a exceção
        logger.error("Erro ao verificar/criar bucket '%s': %s", bucket_name, error)
        raise


def _fetch_ticker(pair: str) -> dict:
    """
    Consulta o endpoint de ticker da Coinbase para um par de moedas.
    Monta a URL da API a partir de variáveis de ambiente (base + template do endpoint)
    """
    base_url = os.getenv("COINBASE_API_BASE_URL")
    endpoint_template = os.getenv("COINBASE_TICKER_ENDPOINT")
    endpoint = endpoint_template.format(pair=pair) # Substitui o placeholder {pair} pelo par de moedas efetivo (ex: BTC-USDT)
    url = f"{base_url}{endpoint}"

    response = requests.get(url, timeout=10) # Faz a requisição HTTP com timeout de 10 segundos
    response.raise_for_status() # Lança exceção automaticamente se o status HTTP indicar erro (4xx/5xx)

    return response.json() # Retorna o corpo da resposta já convertido em dicionário Python


def _build_object_path(pair: str, timestamp: datetime) -> str:
    """
    Monta o caminho de particionamento no padrão bronze/coinbase/{par}/{ano}/{mes}/{dia}/{arquivo}.json
    Cria um caminho particionado por par/ano/mes/dia, facilitando consultas e organização no bucket
    O nome do arquivo inclui o par e um timestamp completo para garantir unicidade
    """
    return (
        f"coinbase/{pair}/"
        f"{timestamp.strftime('%Y')}/{timestamp.strftime('%m')}/{timestamp.strftime('%d')}/"
        f"{pair}_{timestamp.strftime('%Y%m%dT%H%M%S')}.json"
    )


def extract_coinbase_data(pairs: list[str]) -> dict:
    """
    Extrai as cotações atuais dos pares informados na API da Coinbase e grava os dados brutos (JSON) na camada Bronze do MinIO.
    Prepara o client MinIO e garante que o bucket de destino exista antes de gravar
    """
    client = _get_minio_client()
    _ensure_bucket_exists(client, BRONZE_BUCKET_NAME)

    extraction_timestamp = datetime.now(timezone.utc) # Marca o instante da extração (em UTC) para usar no particionamento e nos metadados de cada registro
    object_paths = {} # Dicionário que vai acumular, por par, o caminho do objeto gravado no MinIO

    # Itera sobre cada par de moedas informado
    for pair in pairs:
        try:
            
            raw_data = _fetch_ticker(pair) # Busca os dados de cotação (ticker) do par na API da Coinbase
            # Enriquece o dado bruto com o próprio par e o timestamp de extração, facilitando rastreabilidade nas camadas seguintes
            raw_data["pair"] = pair
            raw_data["extracted_at"] = extraction_timestamp.isoformat()

            
            object_path = _build_object_path(pair, extraction_timestamp) # Define o caminho (object_path) onde o arquivo será salvo dentro do bucket
            payload = json.dumps(raw_data).encode("utf-8") # Serializa o dicionário para JSON e converte para bytes (necessário para o upload)

            # Envia o arquivo JSON para o MinIO
            client.put_object(
                bucket_name=BRONZE_BUCKET_NAME,
                object_name=object_path,
                data=BytesIO(payload),   # o MinIO espera um stream de bytes, por isso o BytesIO
                length=len(payload),     # tamanho do payload em bytes
                content_type="application/json",
            )

            # Registra o caminho salvo para esse par e loga sucesso
            object_paths[pair] = object_path
            logger.info("Dados de '%s' gravados em '%s/%s'.", pair, BRONZE_BUCKET_NAME, object_path)

        except requests.RequestException as error:
            # Erro na chamada à API da Coinbase (timeout, conexão, status de erro etc.)
            logger.error("Erro ao consultar API da Coinbase para '%s': %s", pair, error)
            raise
        except S3Error as error:
            # Erro ao gravar o arquivo no MinIO
            logger.error("Erro ao gravar dados de '%s' no MinIO: %s", pair, error)
            raise

    # Retorna o mapa {par: caminho_do_arquivo}, que pode ser usado pelas próximas tasks do pipeline
    return object_paths
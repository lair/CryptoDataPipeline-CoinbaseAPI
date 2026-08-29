import os
import json
import logging
from io import BytesIO

import pandas as pd
from minio import Minio
from minio.error import S3Error

# Logger usado para registrar informações e erros durante a transformação
logger = logging.getLogger(__name__)

# Nomes dos buckets de origem (bronze) e destino (silver) no MinIO, configuráveis via variáveis de ambiente, com valores padrão
BRONZE_BUCKET_NAME = os.getenv("BRONZE_BUCKET_NAME", "bronze")
SILVER_BUCKET_NAME = os.getenv("SILVER_BUCKET_NAME", "silver")


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


def _read_bronze_object(client: Minio, object_path: str) -> dict:
    """
    Lê e desserializa um objeto JSON da camada Bronze.
    Faz o download do objeto (arquivo) armazenado no bucket bronze
    """
    response = client.get_object(BRONZE_BUCKET_NAME, object_path)
    try:
        # Lê todo o conteúdo em bytes e converte de JSON para dicionário Python
        raw_bytes = response.read()
        return json.loads(raw_bytes)
    finally:
        # Garante o fechamento da conexão/stream mesmo se ocorrer erro na leitura, evitando vazamento de conexões com o MinIO
        response.close()
        response.release_conn()


def _normalize_ticker(raw_data: dict, pair: str) -> dict:
    """
    Normaliza o payload bruto do ticker da Coinbase para um schema padronizado.
    Campos defensivos com .get() pois a API pode omitir algum valor eventualmente.
    """
    # Monta um dicionário com schema fixo e tipos convertidos (ex: float), garantindo consistência dos dados independente de variações da API de origem
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
    """
    Converte o caminho do objeto Bronze (.json) para o caminho equivalente na Silver (.parquet).
    Remove a extensão .json do caminho original e adiciona .parquet, mantendo a mesma estrutura de pastas/particionamento usada na camada bronze
    """
    return bronze_object_path.rsplit(".", 1)[0] + ".parquet"


def transform_bronze_to_silver(pairs: list[str], **context) -> dict:
    """
    Lê os arquivos JSON brutos da camada Bronze (referenciados via XCom pela task de extração), normaliza os dados e grava em formato Parquet na camada Silver do MinIO.
    """
    
    ti = context["ti"] # Recupera a instância da task atual (Task Instance) a partir do contexto do Airflow
    bronze_object_paths = ti.xcom_pull(task_ids="extract_bronze") # Puxa via XCom o dicionário {par: caminho_do_arquivo} produzido pela task de extração (bronze)

    # Validação: se não vier nada do XCom, interrompe a execução com um erro claro
    if not bronze_object_paths:
        raise ValueError("Nenhum caminho de arquivo Bronze recebido via XCom da task de extração.")

    # Prepara o client MinIO e garante que o bucket silver exista antes de gravar
    client = _get_minio_client()
    _ensure_bucket_exists(client, SILVER_BUCKET_NAME)

    
    silver_object_paths = {} # Dicionário que vai acumular, por par, o caminho do arquivo parquet gravado na silver

    # Itera sobre cada par de moedas informado
    for pair in pairs:
        # Busca o caminho do arquivo bronze correspondente a esse par
        bronze_object_path = bronze_object_paths.get(pair)
        if not bronze_object_path:
            # Se a task de extração não gerou arquivo para esse par, pula com um aviso
            logger.warning("Nenhum arquivo Bronze encontrado para o par '%s'. Pulando.", pair)
            continue

        try:
            
            raw_data = _read_bronze_object(client, bronze_object_path) # Lê o JSON bruto da camada bronze 
            normalized_record = _normalize_ticker(raw_data, pair) # Normaliza os dados para o schema padronizado da silver

            
            df = pd.DataFrame([normalized_record]) # Cria um DataFrame de uma única linha com o registro normalizado

            
            buffer = BytesIO() # Converte o DataFrame para o formato Parquet em memória (buffer de bytes)
            df.to_parquet(buffer, engine="pyarrow", index=False)
            buffer.seek(0)  # reposiciona o ponteiro no início do buffer antes de ler/enviar

            
            silver_object_path = _build_silver_object_path(bronze_object_path) # Define o caminho de destino no bucket silver, espelhando a estrutura do bronze

            # Envia o arquivo parquet para o MinIO
            client.put_object(
                bucket_name=SILVER_BUCKET_NAME,
                object_name=silver_object_path,
                data=buffer,
                length=buffer.getbuffer().nbytes,  # tamanho do conteúdo em bytes
                content_type="application/octet-stream",
            )

            # Registra o caminho salvo para esse par e loga sucesso
            silver_object_paths[pair] = silver_object_path
            logger.info("Dados de '%s' transformados e gravados em '%s/%s'.", pair, SILVER_BUCKET_NAME, silver_object_path)

        except S3Error as error:
            # Erro relacionado à comunicação com o MinIO (leitura ou escrita)
            logger.error("Erro ao processar dados de '%s' no MinIO: %s", pair, error)
            raise
        except (KeyError, ValueError, TypeError) as error:
            # Erro ao normalizar os dados (campo ausente, tipo inválido, conversão de valor etc.)
            logger.error("Erro ao normalizar dados de '%s': %s", pair, error)
            raise

    # Retorna o mapa {par: caminho_do_arquivo_parquet}, usado pela próxima task (load_gold)
    return silver_object_paths
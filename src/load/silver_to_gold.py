import os
import logging
from io import BytesIO
from datetime import datetime, timezone

import pandas as pd
from minio import Minio
from minio.error import S3Error
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__) # Logger usado para registrar informações e erros durante a carga na camada Gold

SILVER_BUCKET_NAME = os.getenv("SILVER_BUCKET_NAME", "silver") # Nome do bucket de origem (silver) no MinIO, configurável via variável de ambiente


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


def _get_postgres_engine():
    """
    Cria e retorna uma engine SQLAlchemy para o Postgres (camada Gold).
    Lê os parâmetros de conexão do Data Warehouse Postgres via variáveis de ambiente
    """
    host = os.getenv("DWH_POSTGRES_HOST")
    port = os.getenv("DWH_POSTGRES_PORT")
    db = os.getenv("DWH_POSTGRES_DB")
    user = os.getenv("DWH_POSTGRES_USER")
    password = os.getenv("DWH_POSTGRES_PASSWORD")

    # Monta a connection string no formato esperado pelo driver psycopg2
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    # Cria a engine do SQLAlchemy, que gerencia o pool de conexões com o banco
    return create_engine(connection_string)


def _read_silver_object(client: Minio, object_path: str) -> pd.DataFrame:
    """Lê um arquivo Parquet da camada Silver e retorna como DataFrame.
    Faz o download do objeto (arquivo parquet) armazenado no bucket silver
    """
    response = client.get_object(SILVER_BUCKET_NAME, object_path)
    try:
        raw_bytes = response.read() # Lê o conteúdo em bytes e converte para DataFrame usando o engine pyarrow
        return pd.read_parquet(BytesIO(raw_bytes), engine="pyarrow")
    finally:
        # Garante o fechamento da conexão/stream mesmo se ocorrer erro na leitura, evitando vazamento de conexões com o MinIO
        response.close()
        response.release_conn()


def _upsert_cotacao_atual(connection, record: dict) -> None:
    """
    Insere ou atualiza o preço mais recente do par na tabela gold.cotacoes_atuais.
    Query de upsert: insere um novo registro ou, se já existir uma linha com o mesmo "pair"(conflito na chave), atualiza os campos com os novos valores (ON CONFLICT ... DO UPDATE)
    """
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
    # Executa a query usando os parâmetros nomeados (:pair, :price etc.) preenchidos pelo dicionário record
    connection.execute(query, record)


def _insert_cotacao_historico(connection, record: dict) -> None:
    """
    Registra o preço do par na tabela histórica gold.cotacoes_historico (append).
    Diferente do upsert acima, aqui é sempre um INSERT simples (append), pois o objetivo é manter o histórico completo de cotações ao longo do tempo
    """
    query = text("""
        INSERT INTO gold.cotacoes_historico (pair, price, volume_24h, extracted_at)
        VALUES (:pair, :price, :volume_24h, :extracted_at)
    """)
    connection.execute(query, record)


def _upsert_variacao_percentual(connection, pair: str) -> None:
    """
    Calcula a variação percentual do preço atual em relação ao registro histórico imediatamente anterior e grava em gold.variacao_percentual.
    
    Query em várias etapas (CTEs - Common Table Expressions):
    1) "ultimos_precos": busca os 2 últimos registros históricos daquele par, ordenados por data
    2) "atual": extrai o preço mais recente desses 2 registros
    3) "anterior": extrai o preço do registro anterior ao mais recente (OFFSET 1)
    Em seguida, insere/atualiza a variação percentual entre esses dois preços
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
                -- Evita divisão por zero ou por valor nulo (ex: quando ainda não há histórico suficiente)
                WHEN anterior.price_anterior = 0 OR anterior.price_anterior IS NULL THEN NULL
                -- Fórmula clássica de variação percentual: ((atual - anterior) / anterior) * 100
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
    # Executa a query passando apenas o par como parâmetro
    connection.execute(query, {"pair": pair})


def load_silver_to_gold(pairs: list[str], **context) -> None:
    """
    Lê os arquivos Parquet da camada Silver (referenciados via XCom pela task de transformação),
    grava o preço atual e o histórico no Postgres, e calcula a variação percentual por par.
    """
    
    ti = context["ti"] # Recupera a instância da task atual (Task Instance) a partir do contexto do Airflow
    silver_object_paths = ti.xcom_pull(task_ids="transform_silver") # Puxa via XCom o dicionário {par: caminho_do_arquivo} produzido pela task de transformação (silver)

    # Validação: se não vier nada do XCom, interrompe a execução com um erro claro
    if not silver_object_paths:
        raise ValueError("Nenhum caminho de arquivo Silver recebido via XCom da task de transformação.")

    # Prepara o client MinIO (para leitura) e a engine do Postgres (para escrita na camada gold)
    minio_client = _get_minio_client()
    engine = _get_postgres_engine()

    # Itera sobre cada par de moedas informado
    for pair in pairs:
        silver_object_path = silver_object_paths.get(pair) # Busca o caminho do arquivo silver correspondente a esse par
        if not silver_object_path:            
            logger.warning("Nenhum arquivo Silver encontrado para o par '%s'. Pulando.", pair) # Se a task de transformação não gerou arquivo para esse par, pula com um aviso
            continue

        try:            
            df = _read_silver_object(minio_client, silver_object_path) # Lê o arquivo parquet da camada silver e converte em DataFrame            
            row = df.iloc[0] # Como cada arquivo contém apenas um registro (uma linha), pega a primeira linha do DataFrame

            # Monta o dicionário de dados que será usado nas queries SQL, convertendo os tipos numéricos e adicionando o timestamp de atualização
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

            # Abre uma transação no Postgres: todas as operações dentro do "with" são commitadas juntas ao final, ou revertidas (rollback) caso alguma falhe
            with engine.begin() as connection:
                # 1) Atualiza a tabela com o preço mais recente do par (upsert)
                _upsert_cotacao_atual(connection, record)
                # 2) Insere um novo registro na tabela de histórico (append)
                _insert_cotacao_historico(connection, {
                    "pair": record["pair"],
                    "price": record["price"],
                    "volume_24h": record["volume_24h"],
                    "extracted_at": record["extracted_at"],
                })
                # 3) Recalcula e grava a variação percentual do par com base no histórico
                _upsert_variacao_percentual(connection, record["pair"])

            # Loga sucesso ao final do processamento desse par
            logger.info("Camada Gold atualizada com sucesso para o par '%s'.", pair)

        except S3Error as error:
            # Erro ao ler o arquivo parquet no MinIO
            logger.error("Erro ao ler dados de '%s' no MinIO: %s", pair, error)
            raise
        except (KeyError, ValueError, TypeError) as error:
            # Erro ao processar/converter os dados (campo ausente, tipo inválido etc.)
            logger.error("Erro ao processar dados de '%s' para a camada Gold: %s", pair, error)
            raise
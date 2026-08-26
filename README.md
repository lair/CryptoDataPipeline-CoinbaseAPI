# Crypto Data Pipeline — Coinbase API

Pipeline de dados end-to-end para extração, transformação e disponibilização de cotações de criptomoedas (BTC, ETH e SOL em relação a USDT) obtidas via API pública da Coinbase, seguindo a arquitetura Medallion (Bronze, Silver, Gold).

## 🎯 Objetivo

Projeto de portfólio para demonstrar competências em engenharia de dados: extração via API, orquestração de pipelines, armazenamento em data lake, modelagem em data warehouse e criação de dashboards.

## 🏗️ Arquitetura

Coinbase API 

│ 

▼ [Airflow DAG] ── extração ──▶ [MinIO / Bronze] (JSON bruto) 

│ 

▼ [Airflow DAG] ── transformação ──▶ [MinIO ou Postgres / Silver] (dados limpos, Parquet) 

│ 

▼ [Airflow DAG] ── curadoria ──▶ [Postgres / Gold] (tabelas agregadas) 

│ 

▼ [Metabase] ── dashboards e visualizações

### Camadas

- **Bronze**: dados brutos exatamente como recebidos da API, em JSON, armazenados no MinIO com particionamento por data (`bronze/coinbase/{par}/{ano}/{mes}/{dia}/{arquivo}.json`).
- **Silver**: dados normalizados e tipados, com schema padronizado (`pair`, `price`, `bid`, `ask`, `volume_24h`, `trade_id`, `exchange_time`, `extracted_at`). Armazenados em Parquet no MinIO, espelhando o mesmo particionamento da Bronze.
- **Gold**: tabelas de negócio prontas para consumo (último preço por par, histórico completo e variação percentual), armazenadas em Postgres.

## 🧰 Stack utilizada

| Componente | Função |
|---|---|
| Python | Extração e transformação dos dados |
| Docker / Docker Compose | Orquestração dos containers |
| Apache Airflow | Orquestração e agendamento das DAGs |
| MinIO | Data lake (armazenamento de objetos S3-compatível) |
| PostgreSQL | Data warehouse (camada Gold) e metadados do Airflow |
| Metabase | Camada de visualização e BI |


## 📁 Estrutura do projeto

crypto-data-pipeline/

├── docker-compose.yml 

├── Dockerfile

├── .env 

├── README.md 

├── requirements.txt

├── dags/ 

│ └── coinbase_pipeline_dag.py 

├── src/ 

│ ├── init.py

│ ├── extract/ 

│ │ ├── init.py

│ │ └── coinbase_extractor.py 

│ ├── transform/ 

│ │ ├── init.py

│ │ └── bronze_to_silver.py 

│ └── load/ 

│ │ ├── init.py

│   └── silver_to_gold.py 

├── sql/ 

│ └── gold_tables.sql 

A imagem do Airflow é construída a partir de um `Dockerfile` próprio, que instala as dependências do projeto (`minio`, `pandas`, `pyarrow`, `psycopg2-binary`, etc.) respeitando o arquivo de constraints oficial da versão do Airflow utilizada, evitando conflitos de dependência.

## 🔗 API utilizada

Dados obtidos via [Coinbase Exchange API](https://docs.cloud.coinbase.com/exchange/docs), endpoint público de ticker, para os pares:

- `BTC-USDT`
- `ETH-USDT`
- `SOL-USDT`

Endpoint de referência:

GET https://api.exchange.coinbase.com/products/{PAR}/ticker

## ⚙️ Como executar

### Pré-requisitos
- Docker e Docker Compose instalados

### Passos

1. Clone o repositório:

   git clone https://github.com/seu-usuario/crypto-data-pipeline.git

   cd crypto-data-pipeline

2. Configure as variáveis de ambiente no arquivo `.env` (credenciais do Postgres, MinIO, Airflow, endpoints da Coinbase, etc.).

3. Construa as imagens e suba os containers:

docker compose build --no-cache docker compose up -d

4. Acesse as interfaces:
   - Airflow: `http://localhost:8080`
   - MinIO Console: `http://localhost:9001`
   - Metabase: `http://localhost:3000`

5. Ative a DAG `coinbase_pipeline_dag` na interface do Airflow para iniciar a extração agendada (execução a cada 15 minutos por padrão).

6. Na primeira vez que acessar o Metabase, crie a conta de administrador e conecte-o ao Postgres da camada Gold, apontando para o schema `gold`.

## 🗂️ Modelagem da camada Gold

Tabelas criadas via `sql/gold_tables.sql`, no schema `gold` do Postgres:

- `gold.cotacoes_atuais`: último preço conhecido de cada par (uma linha por par, atualizada via upsert a cada execução da DAG).
- `gold.cotacoes_historico`: série temporal completa de preços por par (uma linha por extração, formando o histórico ao longo do tempo).
- `gold.variacao_percentual`: variação percentual do preço atual em relação ao registro histórico imediatamente anterior, por par.

## 📊 Dashboards no Metabase

Consultas já validadas para compor o dashboard principal:

- Tabela com o preço atual de cada par (`gold.cotacoes_atuais`).
- Gráfico de linha com a evolução do preço de BTC, ETH e SOL ao longo do tempo (`gold.cotacoes_historico`).
- Cards com a variação percentual mais recente por par (`gold.variacao_percentual`).
- Gráfico de linha com a variação percentual calculada ponto a ponto via `LAG()` sobre o histórico, para identificar picos de volatilidade.
- Gráfico de barras comparando o volume negociado nas últimas 24h entre os pares.
- Card de monitoramento de *freshness*, indicando há quantos minutos cada par foi atualizado pela última vez (útil para detectar falhas silenciosas na DAG).

## 🚀 Próximos passos / Roadmap

- [ ] Adicionar testes automatizados (pytest) para as etapas de extração, transformação e carga.
- [ ] Implementar alertas de falha das DAGs (e-mail/Slack).
- [ ] Adicionar camada de qualidade de dados (Great Expectations).
- [ ] Versionar schema das tabelas Gold com migrations.
- [ ] Adicionar agregação horária (`gold.cotacoes_horarias`) com preço médio, mínimo e máximo por hora.
- [ ] Deploy em ambiente cloud (ex: AWS S3 no lugar do MinIO).

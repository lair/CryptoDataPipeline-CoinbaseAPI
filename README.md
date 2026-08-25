# Crypto Data Pipeline — Coinbase API

Pipeline de dados end-to-end para extração, transformação e disponibilização de cotações de criptomoedas (BTC, ETH e SOL em relação a USDT) obtidas via API pública da Coinbase, seguindo a arquitetura Medallion (Bronze, Silver, Gold).

## 🎯 Objetivo

Projeto de portfólio para demonstrar competências em engenharia de dados: extração via API, orquestração de pipelines, armazenamento em data lake, modelagem em data warehouse e criação de dashboards.

## 🏗️ Arquitetura

Coinbase API │ ▼ [Airflow DAG] ── extração ──▶ [MinIO / Bronze] (JSON bruto) │ ▼ [Airflow DAG] ── transformação ──▶ [MinIO ou Postgres / Silver] (dados limpos, Parquet) │ ▼ [Airflow DAG] ── curadoria ──▶ [Postgres / Gold] (tabelas agregadas) │ ▼ [Metabase] ── dashboards e visualizações

### Camadas

- **Bronze**: dados brutos exatamente como recebidos da API, em JSON, armazenados no MinIO com particionamento por data (`bronze/coinbase/{par}/{ano}/{mes}/{dia}/{arquivo}.json`).
- **Silver**: dados limpos, tipados e deduplicados, com schema padronizado (`timestamp`, `par`, `preco`, `volume_24h`). Armazenados em Parquet no MinIO.
- **Gold**: tabelas de negócio prontas para consumo (preço médio por hora, variação percentual, últimos preços por par), armazenadas em Postgres.

## 🧰 Stack utilizada

| Componente | Função |
|---|---|
| Python | Extração e transformação dos dados |
| Docker / Docker Compose | Orquestração dos containers |
| Apache Airflow | Orquestração e agendamento das DAGs |
| MinIO | Data lake (armazenamento de objetos S3-compatível) |
| PostgreSQL | Data warehouse (camada Gold e metadados do Airflow) |
| Metabase | Camada de visualização e BI |

## 📁 Estrutura do projeto

crypto-data-pipeline/ ├── docker-compose.yml ├── .env ├── README.md ├── dags/ │ └── coinbase_pipeline_dag.py ├── src/ │ ├── extract/ │ │ └── coinbase_extractor.py │ ├── transform/ │ │ └── bronze_to_silver.py │ └── load/ │ └── silver_to_gold.py ├── sql/ │ └── gold_tables.sql └── requirements.txt

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

2. Configure as variáveis de ambiente no arquivo `.env` (credenciais do Postgres, MinIO, Airflow, etc.).

3. Suba os containers:
docker-compose up -d

4. Acesse as interfaces:
   - Airflow: `http://localhost:8080`
   - MinIO Console: `http://localhost:9001`
   - Metabase: `http://localhost:3000`

5. Ative a DAG `coinbase_pipeline_dag` na interface do Airflow para iniciar a extração agendada.

## 🗂️ Modelagem da camada Gold

Exemplo de tabelas na camada Gold (Postgres):

- `gold.cotacoes_atuais`: último preço registrado por par.
- `gold.cotacoes_horarias`: preço médio, mínimo e máximo agregados por hora.
- `gold.variacao_percentual`: variação percentual do preço em relação ao período anterior.

## 📊 Dashboards no Metabase

Sugestões de visualizações:
- Evolução do preço de BTC, ETH e SOL ao longo do tempo.
- Comparativo de variação percentual entre as três moedas.
- Tabela com últimas cotações atualizadas.

## 🚀 Próximos passos / Roadmap

- [ ] Adicionar testes automatizados (pytest) para as etapas de extração e transformação.
- [ ] Implementar alertas de falha das DAGs (e-mail/Slack).
- [ ] Adicionar camada de qualidade de dados (Great Expectations).
- [ ] Versionar schema das tabelas Gold com migrations.
- [ ] Deploy em ambiente cloud (ex: AWS S3 no lugar do MinIO).

## 📄 Licença

Este projeto está sob a licença MIT.

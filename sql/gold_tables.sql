-- ============================================
-- Camada Gold: schema e tabelas de negócio
-- Pipeline Coinbase (BTC, ETH, SOL para USDT)
-- ============================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ---------------------------------------------------
-- Tabela: gold.cotacoes_atuais
-- Armazena o último preço conhecido de cada par (1 linha por par)
-- ---------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.cotacoes_atuais (
    pair            VARCHAR(20) PRIMARY KEY,
    price           NUMERIC(20, 8) NOT NULL,
    bid             NUMERIC(20, 8),
    ask             NUMERIC(20, 8),
    volume_24h      NUMERIC(20, 8),
    exchange_time   TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------
-- Tabela: gold.cotacoes_historico
-- Série temporal de preços por par (append, uma linha por extração)
-- ---------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.cotacoes_historico (
    id              BIGSERIAL PRIMARY KEY,
    pair            VARCHAR(20) NOT NULL,
    price           NUMERIC(20, 8) NOT NULL,
    volume_24h      NUMERIC(20, 8),
    extracted_at    TIMESTAMPTZ NOT NULL,
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cotacoes_historico_pair_extracted_at
    ON gold.cotacoes_historico (pair, extracted_at DESC);

-- ---------------------------------------------------
-- Tabela: gold.variacao_percentual
-- Variação percentual do preço atual em relação ao registro anterior (1 linha por par)
-- ---------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.variacao_percentual (
    pair                    VARCHAR(20) PRIMARY KEY,
    price_atual             NUMERIC(20, 8),
    price_anterior          NUMERIC(20, 8),
    variacao_percentual     NUMERIC(10, 4),
    calculado_em            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
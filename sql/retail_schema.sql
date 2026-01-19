-- =========================================================================
-- DATABRICKS LAKEHOUSE SCHEMA DEFINITION
-- Projeto: Azure Databricks Retail Pipeline
-- Autor: Willian Phaiffer Cardoso
-- =========================================================================

-- Criação dos Schemas (Camadas da Arquitetura Medalhão)
CREATE SCHEMA IF NOT EXISTS BRONZE_RETAIL;
CREATE SCHEMA IF NOT EXISTS SILVER_RETAIL;
CREATE SCHEMA IF NOT EXISTS GOLD_RETAIL;

-- =========================================================================
-- 1. CAMADA BRONZE (Dados Brutos + Metadados)
-- Fonte: Ingestão de API (sales_transactions.csv)
-- =========================================================================
CREATE TABLE IF NOT EXISTS BRONZE_RETAIL.SALES_TRANSACTIONS (
    transaction_id STRING,
    date STRING,               -- Ainda como string, pois vem do CSV
    customer_email STRING,
    product_id STRING,
    category STRING,
    price STRING,
    quantity STRING,
    total_amount STRING,
    payment_type STRING,
    ingestion_timestamp TIMESTAMP -- Metadado adicionado pelo Spark
)
USING PARQUET
COMMENT 'Tabela raw com histórico completo de ingestão, sem tipagem forte.';

-- =========================================================================
-- 2. CAMADA SILVER (Dados Limpos e Tipados)
-- Fonte: BRONZE_RETAIL.SALES_TRANSACTIONS
-- Transformações: Tipagem, Deduplicação, Correção de valores negativos
-- =========================================================================
CREATE TABLE IF NOT EXISTS SILVER_RETAIL.SALES_CLEAN (
    transaction_id STRING,
    transaction_date DATE,     -- Convertido para Date
    customer_email STRING,
    category STRING,
    product_id INT,            -- Cast para Inteiro
    price DOUBLE,              -- Cast para Double
    quantity INT,              -- Cast para Inteiro
    total_amount DOUBLE,       -- Valor absoluto (sem negativos)
    payment_type STRING
)
USING PARQUET
COMMENT 'Dados de vendas limpos, deduplicados e validados.';

-- =========================================================================
-- 3. CAMADA GOLD (Agregações de Negócio)
-- Fonte: SILVER_RETAIL.SALES_CLEAN + USERS.JSON (Join)
-- Objetivo: KPI de Receita por Região
-- =========================================================================
CREATE TABLE IF NOT EXISTS GOLD_RETAIL.CATEGORY_PERFORMANCE (
    country STRING,            -- Enriquecido via Join com Users
    category STRING,
    revenue DOUBLE             -- Soma agregada de total_amount
)
USING PARQUET
COMMENT 'Agregação de receita total por Categoria e País para Dashboards.';

-- =========================================================================
-- CONSULTAS DE EXEMPLO (Para validação)
-- =========================================================================

-- Verificando qualidade da limpeza (Silver vs Bronze)
-- SELECT count(*) FROM BRONZE_RETAIL.SALES_TRANSACTIONS;
-- SELECT count(*) FROM SILVER_RETAIL.SALES_CLEAN;

-- Top 5 Categorias no Brasil (Gold)
-- SELECT category, revenue
-- FROM GOLD_RETAIL.CATEGORY_PERFORMANCE
-- WHERE country = 'Brazil'
-- ORDER BY revenue DESC
-- LIMIT 5;
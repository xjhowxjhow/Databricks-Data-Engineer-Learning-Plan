-- ============================================================
-- COMPRAS PIPELINE
-- ============================================================


-- ============================================================
-- BRONZE
-- ============================================================

CREATE OR REFRESH STREAMING TABLE compras_bronze
COMMENT 'Bronze - dados brutos de compras da DummyJSON'
AS

SELECT
    *
FROM STREAM read_files(
    '/Volumes/dev/pipelines_sdp/files/orders/',
    format => 'json'
);


-- ============================================================
-- SILVER
-- ============================================================

CREATE OR REFRESH STREAMING TABLE compras_silver
COMMENT 'Silver - itens das compras tratados'
AS

SELECT

    CAST(c.id AS INT) AS compra_id,

    CAST(c.userId AS INT) AS cliente_id,

    CAST(p.id AS INT) AS produto_id,

    p.title AS produto,

    CAST(
        p.price AS DECIMAL(18,2)
    ) AS preco,

    CAST(
        p.quantity AS INT
    ) AS quantidade,

    CAST(
        p.total AS DECIMAL(18,2)
    ) AS total,

    CAST(
        p.discountPercentage
        AS DECIMAL(10,2)
    ) AS percentual_desconto,

    CAST(
        p.discountedTotal
        AS DECIMAL(18,2)
    ) AS total_com_desconto

FROM STREAM compras_bronze c

LATERAL VIEW explode(
    c.products
) products_exploded AS p;


-- ============================================================
-- GOLD
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW compras_gold
COMMENT 'Gold - resumo de compras por cliente'
AS

SELECT

    cliente_id,

    COUNT(
        DISTINCT compra_id
    ) AS quantidade_compras,

    SUM(
        quantidade
    ) AS quantidade_itens,

    SUM(
        total
    ) AS valor_total,

    SUM(
        total_com_desconto
    ) AS valor_total_com_desconto

FROM compras_silver

GROUP BY cliente_id;
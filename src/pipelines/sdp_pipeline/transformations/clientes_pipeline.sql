-- ============================================================
-- CLIENTES PIPELINE
-- ============================================================


-- ============================================================
-- BRONZE
-- ============================================================

CREATE OR REFRESH STREAMING TABLE clientes_bronze
COMMENT 'Bronze - dados brutos de clientes da DummyJSON'
AS

SELECT
    *
FROM STREAM read_files(
    '/Volumes/dev/pipelines_sdp/files/customers/',
    format => 'json'
);


-- ============================================================
-- SILVER
-- ============================================================

CREATE OR REFRESH STREAMING TABLE clientes_silver
COMMENT 'Silver - clientes tratados'
AS

SELECT

    CAST(id AS INT) AS cliente_id,

    TRIM(firstName) AS nome,

    TRIM(lastName) AS sobrenome,

    LOWER(TRIM(email)) AS email,

    TRIM(phone) AS telefone,

    TRIM(username) AS username,

    CAST(age AS INT) AS idade,

    gender AS genero,

    birthDate AS data_nascimento,

    role AS tipo_usuario

FROM STREAM clientes_bronze

WHERE id IS NOT NULL;


-- ============================================================
-- GOLD
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW clientes_gold
COMMENT 'Gold - clientes para consumo analítico'
AS

SELECT

    cliente_id,

    CONCAT(
        nome,
        ' ',
        sobrenome
    ) AS nome_completo,

    email,

    telefone,

    username,

    idade,

    genero,

    data_nascimento,

    tipo_usuario

FROM clientes_silver;
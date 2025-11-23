CREATE OR REFRESH MATERIALIZED VIEW dev.dividas.devedores_gold
COMMENT "GOLD: Materialized View detalhada para analytics, com refresh incremental automático"
TBLPROPERTIES(
      'quality' = 'gold'
)
AS
SELECT
    -- Identificadores
    cpf_cnpj,
    nome_devedor,
    termo,
    
    -- Categorias do domínio
    tipo_devedor,
    origem,
    credor,
    situacao_credito,
    motivo_suspensao,

    -- Datas
    data_inscricao,
    ano,
    mes,
    DAY(data_inscricao) AS dia,

    -- Métricas financeiras
    saldo_devedor,
    CASE 
        WHEN saldo_devedor < 1000 THEN '0-1k'
        WHEN saldo_devedor < 5000 THEN '1k-5k'
        WHEN saldo_devedor < 20000 THEN '5k-20k'
        ELSE '20k+'
    END AS faixa_saldo,

    -- Flags úteis
    CASE WHEN motivo_suspensao IS NOT NULL THEN 1 ELSE 0 END AS flag_suspenso,
    CASE WHEN situacao_credito = 'ATIVO' THEN 1 ELSE 0 END AS flag_ativo,

    -- Auditoria
    data_processamento,
    CURRENT_TIMESTAMP() AS data_processamento_gold

FROM dev.dividas.devedores_silver;

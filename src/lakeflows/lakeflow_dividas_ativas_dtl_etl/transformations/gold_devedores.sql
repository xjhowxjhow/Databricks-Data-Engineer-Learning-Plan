CREATE OR REFRESH MATERIALIZED VIEW dev.dividas.devedores_gold
COMMENT 'GOLD: Visão analítica unificada, derivada da Silver'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
    cpf_cnpj,
    nome_devedor,
    termo,
    tipo_devedor,
    origem,
    credor,
    situacao_credito,
    motivo_suspensao,
    data_inscricao,
    ano,
    mes,
    DAY(data_inscricao) AS dia,
    saldo_devedor,

    CASE 
        WHEN saldo_devedor IS NULL THEN 'SEM SALDO'
        WHEN saldo_devedor < 1000 THEN '0-1k'
        WHEN saldo_devedor < 5000 THEN '1k-5k'
        WHEN saldo_devedor < 20000 THEN '5k-20k'
        ELSE '20k+'
    END AS faixa_saldo,

    CASE 
        WHEN motivo_suspensao IS NOT NULL AND motivo_suspensao <> '' 
        THEN 1 ELSE 0 
    END AS flag_suspenso,

    CASE 
        WHEN situacao_credito IN ('ATIVO', 'EM EXECUCAO') 
        THEN 1 ELSE 0 
    END AS flag_ativo,

    data_processamento,
    CURRENT_TIMESTAMP() AS data_processamento_gold

FROM dev.dividas.devedores_silver;

CREATE OR REFRESH STREAMING TABLE dev.dividas.devedores_silver
COMMENT 'SILVER: Limpeza e tratamento dos dados de devedores'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT
  CAST(cpf_cnpj_devedor AS STRING) AS cpf_cnpj,
  UPPER(TRIM(nome_devedor)) AS nome_devedor,
  tipo_devedor,
  origem,
  credor,
  CAST(numero_termo_inscricao AS BIGINT) AS termo,
  TO_DATE(data_da_inscricao, 'dd/MM/yyyy') AS data_inscricao,
  CAST(saldo_devedor_sem_honorarios AS DOUBLE) AS saldo_devedor,
  situacao_credito,
  motivo_suspensao,
  ano,
  mes,
  CURRENT_TIMESTAMP() AS data_processamento
FROM STREAM dev.dividas.devedores_bronze
WHERE cpf_cnpj_devedor IS NOT NULL;

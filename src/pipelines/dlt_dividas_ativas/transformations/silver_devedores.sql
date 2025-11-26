CREATE OR REFRESH STREAMING TABLE dev.dividas.devedores_silver
COMMENT 'SILVER: Limpeza e tratamento dos dados de devedores'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT
  regexp_replace(cpf_cnpj_devedor, '[^0-9Xx]', '') AS cpf_cnpj,
  dev.dividas.remove_acentos(nome_devedor) AS nome_devedor,
  dev.dividas.remove_acentos(tipo_pessoa) AS tipo_pessoa,
  dev.dividas.remove_acentos(tipo_devedor) AS tipo_devedor,
  dev.dividas.remove_acentos(origem) AS origem,
  dev.dividas.remove_acentos(credor) AS credor,
  CAST(regexp_replace(numero_termo_inscricao, '[^0-9]', '') AS BIGINT) AS termo,
  TO_DATE(data_da_inscricao, 'dd/MM/yyyy') AS data_inscricao,
  sequencial_credito,
  dev.dividas.remove_acentos(situacao_credito) AS situacao_credito,
  dev.dividas.remove_acentos(motivo_suspensao) AS motivo_suspensao,
  ano,
  mes,
  CAST(
    regexp_replace(
      regexp_replace(
        regexp_replace(saldo_devedor_sem_honorarios, '[^0-9,.-]', ''), 
        ',', '.'
      ),
      '\\.(?=.*\\.)', ''
    ) AS DOUBLE
  ) AS saldo_devedor,
  CURRENT_TIMESTAMP() AS data_processamento

FROM STREAM dev.dividas.devedores_bronze
WHERE cpf_cnpj_devedor IS NOT NULL;

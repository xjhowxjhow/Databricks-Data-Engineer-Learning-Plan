CREATE OR REFRESH STREAMING TABLE dev.dividas.devedores_silver
COMMENT 'SILVER: Limpeza e tratamento dos dados de devedores'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT
  regexp_replace(cpf_cnpj_devedor, '[^0-9Xx]', '') AS cpf_cnpj,
  upper(translate(regexp_replace(nome_devedor, '\uFFFD', ''),
    'ÁÂÀÃÄÅáâàãäåÉÊÈËéêèëÍÎÌÏíîìïÓÔÒÕÖóôòõöÚÛÙÜúûùüÇçÑñ',
    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'
  )) AS nome_devedor,
  upper(translate(regexp_replace(tipo_pessoa, '\uFFFD', ''),
    'ÁÂÀÃÄÅáâàãäåÉÊÈËéêèëÍÎÌÏíîìïÓÔÒÕÖóôòõöÚÛÙÜúûùüÇçÑñ',
    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'
  )) AS tipo_pessoa,
  upper(translate(regexp_replace(tipo_devedor, '\uFFFD', ''),
    'ÁÂÀÃÄÅáâàãäåÉÊÈËéêèëÍÎÌÏíîìïÓÔÒÕÖóôòõöÚÛÙÜúûùüÇçÑñ',
    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'
  )) AS tipo_devedor,
  upper(translate(regexp_replace(origem, '\uFFFD', ''),
    'ÁÂÀÃÄÅáâàãäåÉÊÈËéêèëÍÎÌÏíîìïÓÔÒÕÖóôòõöÚÛÙÜúûùüÇçÑñ',
    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'
  )) AS origem,
  upper(translate(regexp_replace(credor, '\uFFFD', ''),
    'ÁÂÀÃÄÅáâàãäåÉÊÈËéêèëÍÎÌÏíîìïÓÔÒÕÖóôòõöÚÛÙÜúûùüÇçÑñ',
    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'
  )) AS credor,
  CAST(regexp_replace(numero_termo_inscricao, '[^0-9]', '') AS BIGINT) AS termo,
  TO_DATE(data_da_inscricao, 'dd/MM/yyyy') AS data_inscricao,
  sequencial_credito,
  upper(translate(regexp_replace(situacao_credito, '\uFFFD', ''),
    'ÁÂÀÃÄÅáâàãäåÉÊÈËéêèëÍÎÌÏíîìïÓÔÒÕÖóôòõöÚÛÙÜúûùüÇçÑñ',
    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'
  )) AS situacao_credito,

  upper(translate(regexp_replace(motivo_suspensao, '\uFFFD', ''),
    'ÁÂÀÃÄÅáâàãäåÉÊÈËéêèëÍÎÌÏíîìïÓÔÒÕÖóôòõöÚÛÙÜúûùüÇçÑñ',
    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'
  )) AS motivo_suspensao,
  ano,
  mes,
  CAST(
    regexp_replace(
      regexp_replace(
        regexp_replace(saldo_devedor_sem_honorarios, '[^0-9,.-]', ''), 
        ',', '.'
      ),
      '\\.(?=.*\\.)', ''  -- remove pontos de milhar, deixa só o último
    ) AS DOUBLE
  ) AS saldo_devedor,
  CURRENT_TIMESTAMP() AS data_processamento
  
FROM STREAM dev.dividas.devedores_bronze
WHERE cpf_cnpj_devedor IS NOT NULL;

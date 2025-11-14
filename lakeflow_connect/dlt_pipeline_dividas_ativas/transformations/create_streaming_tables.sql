CREATE OR REFRESH STREAMING TABLE dev.lakeflow.dim_devedores_bronze
COMMENT 'BRONZE: Armazenamento dos devedores sem tratamento'
TBLPROPERTIES (
  -- 'delta.appendOnly' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.columnMapping.mode' = 'name'
)
PARTITIONED BY(ano,mes)
AS 
SELECT
  a.*,
  -- Colunas Metadados
  _metadata,
  _metadata.file_path,
  _metadata.file_modification_time,
  _metadata.file_size,
  _metadata.file_block_start,
  year(a.data_de_geracao) AS ano,
  month(a.data_de_geracao) AS mes
FROM STREAM
  read_files(
    '/Volumes/dev/lakeflow/files/raw/DEVEDORES_DIVIDA_ATIVA-2025-06*',
    format       => 'csv',
    header       => true,
    dateFormat   => 'dd/mm/yyyy',
    schema       => 'data_de_geracao date,
                     cpf_cnpj_devedor string,
                     tipo_pessoa string,
                     tipo_devedor string,
                     nome_devedor string,
                     numero_termo_inscricao string,
                     data_da_inscricao string,
                     sequencial_credito string,
                     situacao_credito string,
                     motivo_suspensao string,
                     origem string,
                     credor string,
                     saldo_devedor_sem_honorarios string,
                     observacao string',
    sep          => ';'
  ) a
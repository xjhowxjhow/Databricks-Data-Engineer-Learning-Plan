CREATE OR REFRESH STREAMING TABLE dev.dividas.devedores_bronze
COMMENT 'BRONZE: Ingestão bruta dos devedores (Auto Loader)'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.columnMapping.mode' = 'name',
  'delta.enableChangeDataFeed' = 'true',
  'quality' = 'bronze'
)
PARTITIONED BY (ano, mes)
AS
SELECT
  a.*,
  _metadata,
  _metadata.file_path,
  _metadata.file_modification_time,
  _metadata.file_size,
  _metadata.file_block_start,

  YEAR(a.data_de_geracao) AS ano,
  MONTH(a.data_de_geracao) AS mes

FROM STREAM read_files(
  '/Volumes/dev/dividas/files/raw/DEVEDORES_DIVIDA_ATIVA*',
  format => 'csv',
  header => true,
  dateFormat => 'dd/MM/yyyy',
  sep => ';',
  schema => '
      data_de_geracao date,
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
      observacao string'
) a;

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
  --Colunas Metadados
  _metadata,
  _metadata.file_path,
  _metadata.file_modification_time,
  _metadata.file_size,
  _metadata.file_block_start,
  year(to_date(a.`DATA DE GERAÇÃO`, 'yyyy-MM-dd')) AS ano,
  month(to_date(a.`DATA DE GERAÇÃO`, 'yyyy-MM-dd')) AS mes
FROM STREAM
  read_files(
    '/Volumes/dev/lakeflow/files/raw/DEVEDORES_DIVIDA_ATIVA*',
    format => 'csv',
    header => true,
    inferSchema => true,
    sep => ';'
  ) a
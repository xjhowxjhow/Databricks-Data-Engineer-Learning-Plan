CREATE OR REFRESH STREAMING TABLE dev.dividas.audit_ingestao
COMMENT 'Auditoria dos arquivos processados'
AS
SELECT
  file_path,
  file_modification_time,
  file_size,
  COUNT(*) AS total_registros,
  CURRENT_TIMESTAMP() AS processado_em
FROM STREAM dev.dividas.devedores_bronze
GROUP BY file_path, file_modification_time, file_size;

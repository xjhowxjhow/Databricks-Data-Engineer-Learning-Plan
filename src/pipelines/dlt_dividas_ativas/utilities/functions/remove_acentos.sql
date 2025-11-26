CREATE OR REPLACE FUNCTION dev.dividas.remove_acentos(str STRING)
RETURNS STRING
RETURN upper(
  translate(
    regexp_replace(str, '\uFFFD', ''),
    'ÁÂÀÃÄÅáâàãäåÉÊÈËéêèëÍÎÌÏíîìïÓÔÒÕÖóôòõöÚÛÙÜúûùüÇçÑñ',
    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'
  )
);

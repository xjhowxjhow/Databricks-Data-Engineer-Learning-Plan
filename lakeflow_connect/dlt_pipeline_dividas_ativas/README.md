# dividas_ativas

Pipeline DLT responsável por ingerir, tratar e disponibilizar dados de Devedores da Dívida Ativa no Lakehouse da Databricks.
os dados obtidos sao publicos disponibilizado Banco central ex: https://www.bcb.gov.br/conteudo/dadosabertos/BCBPGBCB/DEVEDORES_DIVIDA_ATIVA-2025-06.csv

## Visão geral do pipeline

![Pipeline Graph](src/img/pipeline_graph.png)

## Estrutura

- **configuration/**  
  Scripts iniciais de configuração e validação do ambiente.

- **explorations/**  
  Notebooks e consultas auxiliares.

- **src/img/**  
  Imagens usadas na documentação.  
  Exemplos:
  
  - Estrutura da pasta Raw (streaming):

    ![Raw folder](src/img/raw_streaming.png)

  - Visualização de arquivos ingeridos:

    ![Audit Streaming Files](src/img/audit_streaming_files_view.png)

- **transformations/**  
  Contém as transformações principais:
  - `bronze_devedores.sql`
  - `silver_devedores.sql`
  - `gold_devedores.sql`
  - `audit.sql`

## Como funciona

O pipeline segue o fluxo padrão do Lakehouse:

**Bronze → Silver → Gold**

- Bronze lê arquivos raw adicionados no UC /Volumes e adiciona metadados.  
- Silver trata, normaliza e valida os dados.  
- Gold consolida e estrutura informações para consumo analítico.  
- A auditoria registra os arquivos processados e auxilia no monitoramento.

Este projeto serve como base para ingestão contínua via Delta Live Tables e disponibilização dos dados para BI e análises operacionais.

# lakeflow_dividas_ativas_dtl

This folder defines all source code for the lakeflow_dividas_ativas_dtl pipeline:

- `explorations/`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations/`: All dataset definitions and transformations.
- `utilities/` (optional): Utility functions and Python modules used in this pipeline.
- `data_sources/` (optional): View definitions describing the source data for this pipeline.

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* By convention, every dataset under `transformations` is in a separate file.
* Take a look at the sample called "sample_trips_lakeflow_dividas_ativas_dtl.py" to get familiar with the syntax.
  Read more about the syntax at https://docs.databricks.com/dlt/python-ref.html.
* If you're using the workspace UI, use `Run file` to run and preview a single transformation.
* If you're using the CLI, use `databricks bundle run lakeflow_dividas_ativas_dtl_etl --select sample_trips_lakeflow_dividas_ativas_dtl` to run a single transformation.

For more tutorials and reference material, see https://docs.databricks.com/dlt.



# dividas_ativas

Pipeline DLT responsável por ingerir, tratar e disponibilizar dados de **Devedores da Dívida Ativa** no Lakehouse da Databricks.  
Os dados processados são **públicos**, disponibilizados pelo Banco Central, por exemplo:  
https://www.bcb.gov.br/conteudo/dadosabertos/BCBPGBCB/DEVEDORES_DIVIDA_ATIVA-2025-06.csv

---

## Visão geral do pipeline

![Pipeline Graph](doc/img/pipeline_graph.png)

---

## Estrutura do Projeto

### **configuration/**
Scripts de preparação e validação do ambiente.

### **explorations/**
Notebooks e consultas auxiliares para análise dos dados processados.

### **src/img/**
Imagens utilizadas na documentação.

- Estrutura da pasta Raw utilizada no streaming:  
  ![Raw folder](doc/img/raw_streaming.png)

- Visualização de arquivos ingeridos pelo Auto Loader:  
  ![Audit Streaming Files](doc/img/audit_streaming_files_view.png)

### **transformations/**
Transformações do pipeline Delta Live Tables:

- **`bronze_devedores.sql`**  
  Ingestão contínua via *STREAMING READ FILES* (Auto Loader).

- **`silver_devedores.sql`**  
  Padronização, limpeza e validação dos dados via *STREAMING READ TABLE* (Bronze).

- **`gold_devedores.sql`**  
  Estrutura analítica final consumida por BI, implementada como **Materialized View** (sobre Silver).

- **`audit.sql`**  
  Auditoria automática dos arquivos ingeridos via *STREAMING READ TABLE* (Bronze).

---

## Como funciona

O pipeline segue o fluxo padrão do Lakehouse:

### **Bronze → Silver → Gold**

- **Bronze**  
  Lê os arquivos CSV adicionados ao `UC /Volumes`, anexa metadados e mantém um histórico contínuo.

- **Silver**  
  Normaliza, padroniza e aplica regras de qualidade (Data Quality Expectations).

- **Gold**  
  Estrutura final para consumo analítico, com métricas, colunas derivadas e materialização para performance.

- **Auditoria**  
  Registra arquivos ingeridos, tamanhos, timestamps e demais informações úteis para monitoramento.

---

Este projeto serve como base para ingestão contínua via **Delta Live Tables**, entregando dados confiáveis e prontos para análises, dashboards e integrações operacionais.


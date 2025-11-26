# Lakeflow Spark Declarative Pipelines (SDP) / DLT (old) 



# Devedores Dividas Ativas Banco Central

O objetivo deste projeto é implementar um pipeline (SDP) responsável por ingerir, processar e disponibilizar dados no Lakehouse da plataforma Databricks, utilizando uma arquitetura de dados em Medalhação (Bronze, Silver e Gold). A ingestão é realizada de forma automatizada através do Auto Loader.

Os dados processados são **públicos**, disponibilizados pelo Banco Central, por exemplo:  
https://www.bcb.gov.br/conteudo/dadosabertos/BCBPGBCB/DEVEDORES_DIVIDA_ATIVA-2025-06.csv

---

## Visão geral do pipeline

![Pipeline Graph](doc/img/pipeline_graph.png)

---

## Estrutura do Projeto

### **config/**
Scripts de preparação e validação do ambiente.

### **explorations/**
Notebooks e consultas auxiliares para análise dos dados processados.

### **doc**
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

### **utilities/**
Utilidades/Recursos do Pipeline:

- **`/functions/remove_acentos.sql`**  
  Funcao reutilizável no lakehouse, ideal para ter a query mais limpa.

- **`/download_files.ipynb`**  
  Notebook python para download dos CSVs disponibilizados para testar o Auto Loader.

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

- Estrutura final do Lakehouse  
  ![Raw folder](doc/img/estrutura_lakehouse.png)


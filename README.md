# Projeto ETL AdventureWorks - DW Multidimensional

Este projeto foi desenvolvido como atividade acadêmica para a disciplina de Engenharia de Dados. O objetivo foi aplicar na prática a modelagem de um Data Warehouse (DW) seguindo o padrão Star Schema, com orquestração de pipeline ETL através do Apache Airflow.

## Tecnologias Utilizadas
- **Linguagem:** Python
- **Banco de Dados:** PostgreSQL
- **Orquestração:** Apache Airflow
- **Modelagem:** Star Schema (Modelo Estrela)

## Estrutura do Projeto
- `dag_vendas_incremental.py`: Script oficial com a DAG do Airflow para orquestração e carga incremental.
- `pipeline.py`: Script base utilizado para testes iniciais de carga no banco.
- `requirements.txt`: Dependências necessárias para execução do ambiente.

## Sobre a Modelagem
O DW foi projetado com uma tabela fato central (`fato_vendas`) conectada a três dimensões (`dim_cliente`, `dim_produto`, `dim_tempo`). Essa estrutura garante performance nas consultas e permite a análise de indicadores de negócio (KPIs) conforme solicitado no enunciado da atividade.

## Estratégia de ETL
O processo foi desenvolvido para ser incremental, garantindo que apenas novos dados ou modificações sejam processados, otimizando o consumo de recursos e seguindo as boas práticas de engenharia de dados.

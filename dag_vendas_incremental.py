from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.ops.python_operator import PythonOperator

# Configurações padrão da DAG
default_args = {
    'owner': 'Henrique Bruneli',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 12),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG obrigatória do professor
dag = DAG(
    'pipeline_vendas_adventureworks',
    default_args=default_args,
    description='Pipeline ETL Multidimencional e Incremental da AdventureWorks',
    schedule_interval=timedelta(days=1),  # Execução diária automatizada
    catchup=False
)

# 1. TAREFA DE EXTRAÇÃO (Extract)


def extrair_dados_incremental(**kwargs):
    print("Iniciando a extração incremental...")
    # Aqui o Airflow usa a data de execução para buscar apenas registros novos (Delta)
    data_execucao = kwargs.get('ds')
    print(f"Buscando dados modificados na data de hoje: {data_execucao}")

    # Dados simulando o incremento do dia
    clientes = [(3, 'Bruna Oliver', 'Física',
                 'Vila Velha', 'Espírito Santo', 'Brasil')]
    produtos = [(103, 'Luva de Ciclismo Pro',
                 'Acessórios', 'Luvas', 'Azul', 25.00)]
    tempos = [(20260613, datetime.strptime(
        '2026-06-13', '%Y-%m-%d').date(), 2026, 6, 'Junho', 13, 2)]
    vendas = [(3, 103, 20260613, 2, 50.00, 100.00, 50.00)]

    return {"clientes": clientes, "produtos": produtos, "tempos": tempos, "vendas": vendas}

# 2. TAREFA DE TRANSFORMAÇÃO (Transform)


def transformar_dados_multidimencional(**kwargs):
    print("Formatando os dados para o padrão Star Schema...")
    ti = kwargs['ti']
    dados_brutos = ti.xcom_pull(task_ids='extrair_dados_brutos')
    return dados_brutos

# 3. TAREFAS DE CARGA ISOLADAS (Load)


def carregar_tabela_dimensao(tabela, campo_conflito, **kwargs):
    ti = kwargs['ti']
    dados = ti.xcom_pull(task_ids='transformar_dados_finais')
    registros = dados[tabela]

    conexao = psycopg2.connect(
        host="localhost", database="dw_adventureworks", user="postgres", password="henrique")
    cursor = conexao.cursor()

    if tabela == 'clientes':
        sql = "INSERT INTO dim_cliente VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id_cliente) DO NOTHING;"
    elif tabela == 'produtos':
        sql = "INSERT INTO dim_produto VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id_produto) DO NOTHING;"
    elif tabela == 'tempos':
        sql = "INSERT INTO dim_tempo VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id_tempo) DO NOTHING;"

    for item in registros:
        cursor.execute(sql, item)
    conexao.commit()
    cursor.close()
    conexao.close()
    print(f"Dimensão {tabela} atualizada de forma incremental.")


def carregar_tabela_fato(**kwargs):
    ti = kwargs['ti']
    dados = ti.xcom_pull(task_ids='transformar_dados_finais')
    vendas = dados['vendas']

    conexao = psycopg2.connect(
        host="localhost", database="dw_adventureworks", user="postgres", password="henrique")
    cursor = conexao.cursor()
    sql = "INSERT INTO fato_vendas (id_cliente, id_produto, id_tempo, quantidade, valor_unitario, valor_total, custo_total) VALUES (%s,%s,%s,%s,%s,%s,%s);"

    for venda in vendas:
        cursor.execute(sql, venda)
    conexao.commit()
    cursor.close()
    conexao.close()
    print("Tabela fato_vendas atualizada de forma incremental.")


# Instanciando as Tasks do Airflow
task_extract = PythonOperator(task_id='extrair_dados_brutos',
                              python_callable=extrair_dados_incremental, provide_context=True, dag=dag)
task_transform = PythonOperator(task_id='transformar_dados_finais',
                                python_callable=transformar_dados_multidimencional, provide_context=True, dag=dag)

task_load_clientes = PythonOperator(task_id='carregar_dim_cliente', python_callable=carregar_tabela_dimensao, op_kwargs={
                                    'tabela': 'clientes', 'campo_conflito': 'id_cliente'}, provide_context=True, dag=dag)
task_load_produtos = PythonOperator(task_id='carregar_dim_produto', python_callable=carregar_tabela_dimensao, op_kwargs={
                                    'tabela': 'produtos', 'campo_conflito': 'id_produto'}, provide_context=True, dag=dag)
task_load_tempos = PythonOperator(task_id='carregar_dim_tempo', python_callable=carregar_tabela_dimensao, op_kwargs={
                                  'tabela': 'tempos', 'campo_conflito': 'id_tempo'}, provide_context=True, dag=dag)
task_load_fato = PythonOperator(task_id='carregar_fato_vendas',
                                python_callable=carregar_tabela_fato, provide_context=True, dag=dag)

# Definição estrita das dependências (Orquestração do Grafo)
task_extract >> task_transform
task_transform >> [task_load_clientes, task_load_produtos, task_load_tempos]
[task_load_clientes, task_load_produtos, task_load_tempos] >> task_load_fato

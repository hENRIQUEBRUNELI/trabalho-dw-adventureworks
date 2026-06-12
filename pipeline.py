import datetime
import psycopg2


def extrair_dados():
    print("Iniciando a extração dos dados brutos...")

    # 1. Dados de clientes
    novos_clientes = [
        (1, 'Henrique Bruneli', 'Física', 'Vila Velha', 'Espírito Santo', 'Brasil'),
        (2, 'Lucas Silva', 'Física', 'Vitória', 'Espírito Santo', 'Brasil')
    ]

    # 2. Dados de produtos
    novos_produtos = [
        (101, 'Mountain Bike de Alumínio',
         'Bicicletas', 'Mountain', 'Preto', 1800.00),
        (102, 'Capacete Esportivo Pro', 'Acessórios', 'Capacetes', 'Vermelho', 90.00)
    ]

    # 3. Dados para a Dimensão Tempo
    dados_tempo = [
        (20260611, datetime.date(2026, 6, 11), 2026, 6, 'Junho', 11, 2),
        (20260612, datetime.date(2026, 6, 12), 2026, 6, 'Junho', 12, 2)
    ]

    # 4. Dados da Tabela de Fatos (Mapeando as vendas reais)
    # Estrutura: (id_venda, id_cliente, id_produto, id_tempo, quantidade, valor_unitario, valor_total, custo_total)
    # Nota: id_venda é omitido na tupla porque é SERIAL (gerado automaticamente pelo PostgreSQL)
    dados_vendas = [
        # Venda 1: Henrique comprou a Bike no dia 12
        (1, 101, 20260612, 1, 2500.00, 2500.00, 1800.00),
        # Venda 2: Lucas comprou 2 Capacetes no dia 12
        (2, 102, 20260612, 2, 150.00, 300.00, 180.00)
    ]

    return novos_clientes, novos_produtos, dados_tempo, dados_vendas


def transformar_dados(dados):
    print("Iniciando a transformação e modelagem para o Esquema Estrela...")
    return dados


def carregar_dados(dados_finais):
    print("Iniciando a carga os dados no Data Warehouse (dw_adventureworks)...")
    clientes, produtos, tempos, vendas = dados_finais

    try:
        conexao = psycopg2.connect(
            host="localhost",
            database="dw_adventureworks",
            user="postgres",
            password="henrique"
        )
        cursor = conexao.cursor()

        # --- CARGA DA DIM_CLIENTE ---
        sql_cliente = """
            INSERT INTO dim_cliente (id_cliente, nome, tipo_cliente, cidade, estado, pais)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_cliente) DO NOTHING;
        """
        for cliente in clientes:
            cursor.execute(sql_cliente, cliente)
        print(
            f"-> Sucesso! {len(clientes)} registros processados na dim_cliente.")

        # --- CARGA DA DIM_PRODUTO ---
        sql_produto = """
            INSERT INTO dim_produto (id_produto, nome_produto, categoria, subcategoria, cor, custo_padrao)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_produto) DO NOTHING;
        """
        for produto in produtos:
            cursor.execute(sql_produto, produto)
        print(
            f"-> Sucesso! {len(produtos)} registros processados na dim_produto.")

        # --- CARGA DA DIM_TEMPO ---
        sql_tempo = """
            INSERT INTO dim_tempo (id_tempo, data_completa, ano, mes, nome_mes, dia, trimestre)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_tempo) DO NOTHING;
        """
        for tempo in tempos:
            cursor.execute(sql_tempo, tempo)
        print(f"-> Sucesso! {len(tempos)} registros processados na dim_tempo.")

        # --- CARGA DA FATO_VENDAS ---
        # Como id_venda é SERIAL, não passamos ele explicitamente nas colunas do INSERT
        sql_venda = """
            INSERT INTO fato_vendas (id_cliente, id_produto, id_tempo, quantidade, valor_unitario, valor_total, custo_total)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        for venda in vendas:
            cursor.execute(sql_venda, venda)
        print(
            f"-> Sucesso! {len(vendas)} transações comerciais carregadas na fato_vendas.")

        conexao.commit()
        cursor.close()
        conexao.close()
        print("Pipeline finalizado! O Esquema Estrela está totalmente povoado.")

    except Exception as erro:
        print(f"Erro durante a carga de dados: {erro}")


if __name__ == "__main__":
    print(f"--- Executando Pipeline de Dados - {datetime.date.today()} ---")
    dados_brutos = extrair_dados()
    dados_limpos = transformar_dados(dados_brutos)
    carregar_dados(dados_limpos)

import argparse
import sys
import psycopg
import os
import pandas as pd

def get_conn(host, port, dbname, user, password):
    # cria e retorna uma conexão com o banco de dados usando os parametros fornecidos
    try:
        conn_string = f"host={host} port={port} dbname={dbname} user={user} password={password}" #f para não ter que usar conn_string = "host=" + host + ...
        conn = psycopg.connect(conn_string)
        return conn
    except psycopg.OperationalError as e:
        print(f"Erro ao conectar ao banco de dados: {e}", file=sys.stderr)
        sys.exit(1)

def print_results(cursor, title,  output=None, csv_filename=None):
    # imprime os resultados de uma consulta de forma formatada
    print(f"\n{'='*10} {title} {'='*10}")
    results = cursor.fetchall()
    if not results:
        print("Nenhum resultado encontrado.")
        return

    headers = [desc[0] for desc in cursor.description] # o psycopg preenche o .description com o nome das colunas
    df = pd.DataFrame(results, columns=headers)

    # Imprime no STDOUT
    print(df.to_string(index=False)) # index=False para não imprimir o índice do DataFrame que o pandas cria automaticamente
    print(f"Total de registros: {len(results)}")

    # Salva em csv se o diretorio e nome do arquivo forem fornecidos
    if output and csv_filename:
        try:
            # Garante que o diretorio de saída exista
            os.makedirs(output, exist_ok=True)
            filepath = os.path.join(output, csv_filename)
            df.to_csv(filepath, index=False)
            print(f"--> Resultado salvo em: {filepath}")
        except IOError as e:
            print(f"Erro ao salvar o arquivo CSV '{filepath}': {e}", file=sys.stderr)
        except Exception as e:
            print(f"Ocorreu um erro inesperado ao salvar o CSV: {e}", file=sys.stderr)

#  Funções de Consultas:

def query1(conn, product_asin, output):
    #Dado um produto, lista os 5 comentários mais úteis e com maior avaliaçãoe os 5 comentários mais úteis e com menor avaliação.
    
    with conn.cursor() as cur:
        # 5 mais úteis com maior avaliação
        sql_top = """
            SELECT rating, helpful, votes, customer_id, review_date
            FROM reviews
            WHERE product_asin = %s
            ORDER BY helpful DESC, rating DESC
            LIMIT 5;
        """
        cur.execute(sql_top, (product_asin,))
        print_results(cur, f"Query 1: Top 5 comentários úteis e com maior avaliação (ASIN: {product_asin})",output, f"q1_top5_reviews_pos_{product_asin}.csv")

        # 5 mais úteis com menor avaliação
        sql_bottom = """
            SELECT rating, helpful, votes, customer_id, review_date
            FROM reviews
            WHERE product_asin = %s
            ORDER BY helpful DESC, rating ASC
            LIMIT 5;
        """
        cur.execute(sql_bottom, (product_asin,))
        print_results(cur, f"Query 1: Top 5 comentários úteis e com menor avaliação (ASIN: {product_asin})",output, f"q1_top5_reviews_neg_{product_asin}.csv")

def query2(conn, product_asin, output):
    # dado um produto, lista os produtos similares com maiores vendas (melhor salesrank).
    
    with conn.cursor() as cur:
        sql = """
            WITH TargetProduct AS (
                SELECT salesrank FROM Products WHERE asin = %s
            )
            SELECT p.asin, p.titulo, p.salesrank
            FROM Related_products rp
            JOIN Products p ON p.asin = CASE
                                    WHEN rp.product1_asin = %s THEN rp.product2_asin
                                    ELSE rp.product1_asin
                                END
            WHERE (rp.product1_asin = %s OR rp.product2_asin = %s)
              AND p.salesrank IS NOT NULL
              AND p.salesrank > 0
              AND p.salesrank < (SELECT salesrank FROM TargetProduct)
            ORDER BY p.salesrank ASC;
        """
        cur.execute(sql, (product_asin, product_asin, product_asin, product_asin))
        print_results(cur, f"Query 2: produtos similares a {product_asin} com melhor ranking de vendas", output, f"q2_similar_products_sales_melhor_{product_asin}.csv")


def query3(conn, product_asin, output):
    #dado um produto, mostra a evolução diária das médias de avaliação

    with conn.cursor() as cur:
        sql = """
            SELECT
                review_date,
                COUNT(rating) AS num_avaliacoes,
                AVG(rating) AS media_avaliacoes
            FROM reviews
            WHERE product_asin = %s
            GROUP BY review_date
            ORDER BY review_date ASC;
        """
        cur.execute(sql, (product_asin,))
        print_results(cur, f"Query 3: Evolução diária das médias de avaliação para o produto {product_asin}", output, f"q3_evolucao_media_avaliacoes_{product_asin}.csv")

def query4(conn, output):
    #lista os 10 produtos líderes de venda em cada grupo de produtos.
    
    with conn.cursor() as cur:
        sql = """
            WITH RankedProducts AS (
                SELECT
                    p.titulo,
                    p.group_name,
                    p.salesrank,
                    ROW_NUMBER() OVER(PARTITION BY p.group_name ORDER BY p.salesrank ASC) as rank_in_group
                FROM Products p
                WHERE p.salesrank > 0 AND p.group_name IS NOT NULL
            )
            SELECT
                group_name,
                rank_in_group,
                titulo,
                salesrank
            FROM RankedProducts
            WHERE rank_in_group <= 10;
        """
        cur.execute(sql)
        print_results(cur, "Query 4: Top 10 produtos líderes de venda por grupo de produtos", output, "q4_top10_produtos_lideres_venda_por_grupo.csv")

def query5(conn, output):
    #lista os 10 produtos com a maior média de avaliações úteis positivas.

    with conn.cursor() as cur:
        sql = """
            SELECT
                p.asin,
                p.titulo,
                AVG(r.helpful) AS media_avaliacoes_uteis,
                COUNT(r.review_id) AS total_avaliacoes
            FROM Products p
            JOIN reviews r ON p.asin = r.product_asin
            GROUP BY p.asin, p.titulo
            HAVING COUNT(r.review_id) > 0
            ORDER BY media_avaliacoes_uteis DESC
            LIMIT 10;
        """
        cur.execute(sql)
        print_results(cur, "Query 5: Top 10 produtos com maior média de avaliações úteis positivas", output, "q5_top10_produtos_maior_media_avaliacoes_uteis.csv")

def query6(conn, output):
    # lista as 5 categorias com a maior média de avaliações úteis positivas, considerando a hierarquia (subcategorias contam para as categorias "pai").

    with conn.cursor() as cur:
        sql = """
            WITH RECURSIVE CategoriaCompleta AS (
                -- Ponto de partida: a categoria direta e o produto
                SELECT
                    pc.category_id,
                    pc.product_asin
                FROM
                    Product_category pc

                UNION ALL

                -- Passo recursivo: sobe na hierarquia, mantendo a referência ao produto
                SELECT
                    c.parent_id,
                    cc.product_asin
                FROM
                    CategoriaCompleta cc
                JOIN
                    Categories c ON cc.category_id = c.category_id
                WHERE
                    c.parent_id IS NOT NULL
            )
            -- Agora, agregue os resultados
            SELECT
                cat.category_name,
                AVG(r.helpful) AS media_avaliacoes_uteis
            FROM
                CategoriaCompleta cc
            JOIN
                Categories cat ON cc.category_id = cat.category_id
            JOIN
                Reviews r ON cc.product_asin = r.product_asin
            GROUP BY
                cat.category_name
            ORDER BY
                media_avaliacoes_uteis DESC
            LIMIT 5;
        """
        cur.execute(sql)
        print_results(cur, "Query 6: Top 5 categorias com maior média de avaliações úteis positivas", output, "q6_top5_categorias_maior_media_avaliacoes_uteis.csv")

def query7(conn, output):
    # lista os 10 clientes que mais fizeram comentários por grupo de produto.
    
    with conn.cursor() as cur:
        sql = """
            WITH CustomerRankByGroup AS (
                SELECT
                    r.customer_id,
                    p.group_name,
                    COUNT(r.review_id) as total_comentarios,
                    ROW_NUMBER() OVER(PARTITION BY p.group_name ORDER BY COUNT(r.review_id) DESC) as rank_in_group
                FROM reviews r
                JOIN Products p ON r.product_asin = p.asin
                WHERE p.group_name IS NOT NULL
                GROUP BY r.customer_id, p.group_name
            )
            SELECT
                group_name,
                rank_in_group,
                customer_id,
                total_comentarios
            FROM CustomerRankByGroup
            WHERE rank_in_group <= 10;
        """
        cur.execute(sql)
        print_results(cur, "Query 7: Top 10 clientes que mais fizeram comentários por grupo de produto", output, "q7_top10_clientes_mais_comentarios_por_grupo.csv")

def main():
    """
    Função principal que executa o script do dashboard.
    """
    parser = argparse.ArgumentParser(description="Executa consultas do dashboard no banco de dados de e-commerce.")
    parser.add_argument("--db-host", required=True, help="Host do banco de dados")
    parser.add_argument("--db-port", type=int, default=5432, help="Porta do banco de dados")
    parser.add_argument("--db-name", required=True, help="Nome do banco de dados")
    parser.add_argument("--db-user", required=True, help="Usuário do banco de dados")
    parser.add_argument("--db-pass", required=True, help="Senha do banco de dados")
    parser.add_argument("--product-asin", help="ASIN do produto para as consultas 1, 2 e 3")
    parser.add_argument("--output", help="Diretório para salvar os resultados das consultas em arquivos CSV")

    args = parser.parse_args()

    conn = None
    try:
        conn = get_conn(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass)
        print("Conexão com o banco de dados estabelecida com sucesso.")

        # Executa consultas que dependem de um ASIN
        if args.product_asin:
            query1(conn, args.product_asin, args.output)
            query2(conn, args.product_asin, args.output)
            query3(conn, args.product_asin, args.output)
        else:
            print("\nAVISO: As consultas 1, 2 e 3 não foram executadas pois o parâmetro --product-asin não foi fornecido.")

        # Executa consultas gerais
        query4(conn, args.output)
        query5(conn, args.output)
        query6(conn, args.output)
        query7(conn, args.output)

        sys.exit(0)

    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()
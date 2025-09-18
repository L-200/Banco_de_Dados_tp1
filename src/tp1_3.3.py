import argparse
import sys
import psycopg
import pandas as pd

def get_conn(host, port, dbname, user, password):
    """
    Cria e retorna uma conexão com o banco de dados PostgreSQL
    usando os parâmetros fornecidos.
    """
    try:
        conn_string = f"host={host} port={port} dbname={dbname} user={user} password={password}"
        conn = psycopg.connect(conn_string)
        return conn
    except psycopg.OperationalError as e:
        print(f"Erro ao conectar ao banco de dados: {e}", file=sys.stderr)
        sys.exit(1)

def print_results(cursor, title):
    """
    Imprime os resultados de uma consulta de forma formatada usando pandas.
    """
    print(f"\n{'='*10} {title} {'='*10}")
    results = cursor.fetchall()
    if not results:
        print("Nenhum resultado encontrado.")
        return

    headers = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=headers)
    print(df.to_string(index=False))
    print(f"Total de registros: {len(results)}")


# --- Funções de Consulta ---

def query1(conn, product_asin):
    """
    Dado um produto, lista os 5 comentários mais úteis e com maior avaliação
    e os 5 comentários mais úteis e com menor avaliação.
    """
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
        print_results(cur, f"Query 1: Top 5 Comentários Úteis e com Maior Avaliação (ASIN: {product_asin})")

        # 5 mais úteis com menor avaliação
        sql_bottom = """
            SELECT rating, helpful, votes, customer_id, review_date
            FROM reviews
            WHERE product_asin = %s
            ORDER BY helpful DESC, rating ASC
            LIMIT 5;
        """
        cur.execute(sql_bottom, (product_asin,))
        print_results(cur, f"Query 1: Top 5 Comentários Úteis e com Menor Avaliação (ASIN: {product_asin})")

def query2(conn, product_asin):
    """
    Dado um produto, lista os produtos similares com maiores vendas (melhor salesrank).
    """
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
        print_results(cur, f"Query 2: Produtos Similares a {product_asin} com Melhor Ranking de Vendas")


def query3(conn, product_asin):
    """
    Dado um produto, mostra a evolução diária das médias de avaliação.
    """
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
        print_results(cur, f"Query 3: Evolução Diária da Média de Avaliações (ASIN: {product_asin})")

def query4(conn):
    """
    Lista os 10 produtos líderes de venda em cada grupo de produtos.
    """
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
        print_results(cur, "Query 4: Top 10 Produtos Líderes de Venda por Grupo")


def query5(conn):
    """
    Lista os 10 produtos com a maior média de avaliações úteis positivas.
    """
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
        print_results(cur, "Query 5: Top 10 Produtos com Maior Média de Avaliações Úteis")


def query6(conn):
    """
    Lista as 5 categorias com a maior média de avaliações úteis positivas.
    """
    with conn.cursor() as cur:
        sql = """
            SELECT
                c.category_name,
                AVG(r.helpful) AS media_avaliacoes_uteis
            FROM Categories c
            JOIN Product_category pc ON c.category_id = pc.category_id
            JOIN reviews r ON pc.product_asin = r.product_asin
            GROUP BY c.category_name
            ORDER BY media_avaliacoes_uteis DESC
            LIMIT 5;
        """
        cur.execute(sql)
        print_results(cur, "Query 6: Top 5 Categorias com Maior Média de Avaliações Úteis")

def query7(conn):
    """
    Lista os 10 clientes que mais fizeram comentários por grupo de produto.
    """
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
        print_results(cur, "Query 7: Top 10 Clientes com Mais Comentários por Grupo de Produtos")


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

    args = parser.parse_args()

    conn = None
    try:
        conn = get_conn(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass)
        print("Conexão com o banco de dados estabelecida com sucesso.")

        # Executa consultas que dependem de um ASIN
        if args.product_asin:
            query1(conn, args.product_asin)
            query2(conn, args.product_asin)
            query3(conn, args.product_asin)
        else:
            print("\nAVISO: As consultas 1, 2 e 3 não foram executadas pois o parâmetro --product-asin não foi fornecido.")

        # Executa consultas gerais
        query4(conn)
        query5(conn)
        query6(conn)
        query7(conn)

    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()
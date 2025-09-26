import argparse
import sys
import os
import time
import pandas as pd
from db import get_conn

#mesma coisa do que tá no 3.2.py
def log_time(func):
    """Decorator que regista e imprime o tempo de execução de uma consulta."""
    def wrapper(*args, **kwargs):
        # usando 'func.__name__' para obter o nome da função original
        print(f"\n-> Executando consulta: '{func.__name__}'...")
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"<- Consulta '{func.__name__}' concluída em {total_time:.4f} segundos.") #unica mudança
        return result
    return wrapper

def print_results(cursor, title,  output=None, csv_filename=None):

    print(f"\n{'='*10} {title} {'='*10}")
    results = cursor.fetchall()
    if not results:
        print("Nenhum resultado encontrado.")
        return
    headers = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=headers)
    print(df.to_string(index=False))
    print(f"Total de registros: {len(results)}")
    if output and csv_filename:
        try:
            os.makedirs(output, exist_ok=True)
            filepath = os.path.join(output, csv_filename)
            df.to_csv(filepath, index=False)
            print(f"--> Resultado salvo em: {filepath}")
        except IOError as e:
            print(f"Erro ao salvar o arquivo CSV '{filepath}': {e}", file=sys.stderr)
        except Exception as e:
            print(f"Ocorreu um erro inesperado ao salvar o CSV: {e}", file=sys.stderr)

def get_product_asin(conn, identifier, identifier_type):
    """
    Busca o ASIN de um produto usando seu source_id, título ou ASIN.
    """
    with conn.cursor() as cur:
        if identifier_type == 'source_id':
            sql = "SELECT asin FROM Products WHERE source_id = %s;" 

            cur.execute(sql, (identifier,))
        elif identifier_type == 'titulo':
            sql = "SELECT asin, titulo FROM Products WHERE titulo ILIKE %s;"
            cur.execute(sql, (f'%{identifier}%',))

        else: # é um asin
            sql = "SELECT asin FROM Products WHERE asin = %s;"
            cur.execute(sql, (identifier,)) #checando se tem mais de um (não deveria ter, mas vai que acontece)

        results = cur.fetchall()

        if len(results) == 0:
            print(f"ERRO: Nenhum produto encontrado com {identifier_type} '{identifier}'.", file=sys.stderr)
            return None
        elif len(results) > 1:
            print(f"ERRO: Múltiplos produtos encontrados com o título '{identifier}'. Seja mais específico ou use o ASIN/source_id.", file=sys.stderr)
            print("Produtos encontrados:")
            for row in results:
                print(f"  - ASIN: {row[0]}, Título: {row[1]}")
            return None
        else:
            return results[0][0]

#  Funções de Consultas
@log_time
def query1(conn, product_asin, output):

    # dado um produto, lista os 5 comentários mais úteis e com maior avaliação
    # e os 5 comentários mais úteis e com menor avaliação.
    with conn.cursor() as cur:
        sql_top = """
        SELECT rating, helpful, votes, customer_id, review_date
        FROM reviews 
        WHERE product_asin = %s 
        ORDER BY helpful DESC, rating DESC LIMIT 5;
        """
        cur.execute(sql_top, (product_asin,))
        print_results(cur, f"Query 1: Top 5 comentários úteis e com maior avaliação (ASIN: {product_asin})",output, f"q1_top5_reviews_pos_{product_asin}.csv")
        
        sql_bottom = """SELECT rating, helpful, votes, customer_id, review_date 
        FROM reviews 
        WHERE product_asin = %s 
        ORDER BY helpful DESC, rating ASC LIMIT 5;"""
        cur.execute(sql_bottom, (product_asin,))
        print_results(cur, f"Query 1: Top 5 comentários úteis e com menor avaliação (ASIN: {product_asin})",output, f"q1_top5_reviews_neg_{product_asin}.csv")

@log_time
def query2(conn, product_asin, output):
    # dado um produto, lista os produtos similares com maiores vendas (melhor salesrank)
    
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

@log_time
def query3(conn, product_asin, output):
    #dado um produto, mostra a evolução diária das médias de avaliação

    with conn.cursor() as cur:
        sql = """
            SELECT
                review_date,
                COUNT(rating) AS num_avaliacoes,
                CAST(AVG(rating) AS DECIMAL(3, 2)) AS media_avaliacoes
            FROM reviews
            WHERE product_asin = %s
            GROUP BY review_date
            ORDER BY review_date ASC;
        """
        cur.execute(sql, (product_asin,))
        print_results(cur, f"Query 3: Evolução diária das médias de avaliação para o produto {product_asin}", output, f"q3_evolucao_media_avaliacoes_{product_asin}.csv")

@log_time
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

@log_time
def query5(conn, output):
    # lista os 10 produtos com a maior média de avaliações úteis positivas,
    # considerando avaliações com rating >= 3.
    with conn.cursor() as cur:
        sql = """
            SELECT
                p.asin,
                p.titulo,
                ROUND(AVG(r.helpful), 2) AS media_avaliacoes_uteis,
                COUNT(r.review_id) AS total_avaliacoes_positivas
            FROM Products p
            JOIN reviews r ON p.asin = r.product_asin
            WHERE r.rating >= 3  -- apenas avaliações com nota 3 ou superior
            GROUP BY p.asin, p.titulo
            HAVING COUNT(r.review_id) > 0
            ORDER BY media_avaliacoes_uteis DESC
            LIMIT 10;
        """
        cur.execute(sql)
        print_results(cur, "Query 5: Top 10 produtos com maior média de avaliações úteis (rating >= 3)", output, "q5_top10_produtos_maior_media_avaliacoes_uteis.csv")

@log_time
def query6(conn, output):
    # lista as 5 categorias com a maior média de avaliações úteis positivas, considerando avaliações com rating >= 3

    with conn.cursor() as cur:
        select_sql = """
            WITH
            DirectCategoryTotals AS (
                SELECT
                    pc.category_id,
                    SUM(r.helpful) AS total_helpful,
                    COUNT(r.review_id) AS total_reviews
                FROM
                    Product_category pc
                JOIN
                    reviews r ON pc.product_asin = r.product_asin
                WHERE
                    r.rating >= 3  -- ADICIONADO: Filtra apenas avaliações com nota 3 ou superior
                GROUP BY
                    pc.category_id
            ),
            -- para cada categoria pai, calcula a soma dos totais de seus filhos diretos
            ChildTotals AS (
                SELECT
                    h.parent_category_id AS category_id,
                    SUM(dct.total_helpful) AS total_helpful,
                    SUM(dct.total_reviews) AS total_reviews
                FROM
                    Category_Hierarchy h
                JOIN
                    DirectCategoryTotals dct ON h.child_category_id = dct.category_id
                GROUP BY
                    h.parent_category_id
            )
            -- Combina os totais diretos com os totais dos filhos e calcula a media final
            SELECT
                cat.category_name,
                ROUND(
                    (COALESCE(dct.total_helpful, 0) + COALESCE(ct.total_helpful, 0))::DECIMAL
                    / NULLIF(COALESCE(dct.total_reviews, 0) + COALESCE(ct.total_reviews, 0), 0), 2
                ) AS media_avaliacoes_uteis,
                (COALESCE(dct.total_reviews, 0) + COALESCE(ct.total_reviews, 0)) AS total_reviews_agregado
            FROM
                Categories cat
            LEFT JOIN
                DirectCategoryTotals dct ON cat.category_id = dct.category_id
            LEFT JOIN
                ChildTotals ct ON cat.category_id = ct.category_id
            WHERE
                (COALESCE(dct.total_reviews, 0) + COALESCE(ct.total_reviews, 0)) > 0
            ORDER BY
                media_avaliacoes_uteis DESC
            LIMIT 5;
        """
        cur.execute(select_sql)
        print_results(cur, "Query 6: Top 5 categorias com maior média de avaliações úteis (rating >= 3)", output, "q6_top5_categorias_maior_media_avaliacoes_uteis.csv")

@log_time
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
    #declarando os argumentos aceitos
    parser = argparse.ArgumentParser(description="Executa consultas do dashboard no banco de dados de e-commerce.")
    parser.add_argument("--db-host", required=True, help="Host do banco de dados")
    parser.add_argument("--db-port", type=int, default=5432, help="Porta do banco de dados")
    parser.add_argument("--db-name", required=True, help="Nome do banco de dados")
    parser.add_argument("--db-user", required=True, help="Usuário do banco de dados")
    parser.add_argument("--db-pass", required=True, help="Senha do banco de dados")
    parser.add_argument("--output", help="Diretório para salvar os resultados das consultas em arquivos CSV")

    #criando um grupo de argumentos mutuamente exclusivos para identificar o produto
    product_identifier_group = parser.add_mutually_exclusive_group()
    product_identifier_group.add_argument("--product-asin", help="ASIN do produto para as consultas 1, 2 e 3")
    product_identifier_group.add_argument("--product-id", type=int, help="ID do produto (usa a coluna source_id) para as consultas 1, 2 e 3")
    product_identifier_group.add_argument("--product-title", help="Título (ou parte do título) do produto para as consultas 1, 2 e 3")

    main_start_time = time.perf_counter()
    print("="*50)
    print("INICIANDO SCRIPT DE CONSULTAS")
    print("="*50)

    args = parser.parse_args()

    conn = None
    try:
        conn = get_conn(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass)
        print("Conexão com o banco de dados estabelecida com sucesso.")

        target_asin = None
        
        if args.product_asin:
            target_asin = get_product_asin(conn, args.product_asin, 'asin')
        elif args.product_id:
            target_asin = get_product_asin(conn, args.product_id, 'source_id')
        elif args.product_title:
            target_asin = get_product_asin(conn, args.product_title, 'titulo')

        if target_asin:
            print(f"\n--- Executando consultas para o produto com ASIN: {target_asin} ---")
            query1(conn, target_asin, args.output)
            query2(conn, target_asin, args.output)
            query3(conn, target_asin, args.output)
        else:
            print("\nAVISO: As consultas 1, 2 e 3 não foram executadas pois nenhum identificador de produto foi fornecido ou o produto não foi encontrado.")

        print("\n--- Executando consultas gerais ---")
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
        
        main_end_time = time.perf_counter()
        total_script_time = main_end_time - main_start_time
        print("="*50)
        print(f"FIM DO SCRIPT. Tempo total de execução: {total_script_time:.4f} segundos.")
        print("="*50)


if __name__ == "__main__":
    main()
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from utils import extract_all_categories, parse_snap
from db import get_conn

BATCH_SIZE = 2000
CATEGORY_BATCH = 500

def log_time(func):
    """Decorator que regista e imprime o tempo de execução de uma função."""
    def wrapper(*args, **kwargs):
        # usando 'func.__name__' para obter o nome da função original
        print(f"-> Iniciando etapa: '{func.__name__}'...")
        start_time = time.perf_counter()
        result = func(*args, **kwargs) #aqui que a função original é de fato executada
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"<- Etapa '{func.__name__}' concluída em {total_time:.4f} segundos.")
        return result
    return wrapper

@log_time
def create_schema(conn, schema_filepath):
    """
    Executa o ficheiro SQL para criar ou recriar as tabelas na base de dados.
    """
    print(f"A aplicar o esquema da base de dados a partir de: {schema_filepath}...")
    try:
        with open(schema_filepath, 'r', encoding='utf-8') as f:
            conn.cursor().execute(f.read())
        conn.commit()
        print("Esquema aplicado com sucesso.")
    except Exception as e:
        print(f"ERRO ao aplicar o esquema: {e}")
        conn.rollback()
        raise

@log_time
def insert_categories(conn, categories_by_old_id):
    # (O resto do código permanece exatamente o mesmo...)
    cur = conn.cursor()
    all_names = list(set(info['name'] for info in categories_by_old_id.values()))
    sql = "INSERT INTO Categories (category_name) VALUES (%s) ON CONFLICT (category_name) DO NOTHING"
    for i in range(0, len(all_names), CATEGORY_BATCH):
        batch = [(name,) for name in all_names[i:i + CATEGORY_BATCH]]
        cur.executemany(sql, batch)
    conn.commit()
    cur.execute("SELECT category_name, category_id FROM Categories")
    name_to_new_id = {name: cat_id for name, cat_id in cur.fetchall()}
    old_to_new_id_map = {
        old_id: name_to_new_id.get(info['name'])
        for old_id, info in categories_by_old_id.items()
    }
    cur.close()
    return old_to_new_id_map

@log_time
def insert_category_hierarchy(conn, categories_by_old_id, old_to_new_map):
    # (O resto do código permanece exatamente o mesmo...)
    hierarchy_pairs = []
    for old_id, info in categories_by_old_id.items():
        parent_old_id = info.get('parent_old_id')
        if parent_old_id:
            child_new_id = old_to_new_map.get(old_id)
            parent_new_id = old_to_new_map.get(parent_old_id)
            if child_new_id and parent_new_id and parent_new_id != child_new_id:
                hierarchy_pairs.append((parent_new_id, child_new_id))
    if hierarchy_pairs:
        cur = conn.cursor()
        sql = "INSERT INTO Category_Hierarchy (parent_category_id, child_category_id) VALUES (%s, %s) ON CONFLICT DO NOTHING"
        cur.executemany(sql, hierarchy_pairs)
        conn.commit()
        cur.close()

@log_time
def process_products_and_reviews(conn, input_file, old_to_new_map):
    cur = conn.cursor()
    prod_sql = "INSERT INTO Products (source_id, asin, titulo, group_name, salesrank, total_reviews, average_rating, qntd_downloads) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (asin) DO UPDATE SET titulo = EXCLUDED.titulo, group_name = EXCLUDED.group_name, salesrank = EXCLUDED.salesrank, total_reviews = EXCLUDED.total_reviews, average_rating = EXCLUDED.average_rating"
    prodcat_sql = "INSERT INTO Product_category (product_asin, category_id) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    reviews_sql = "INSERT INTO reviews (product_asin, customer_id, rating, review_date, votes, helpful) VALUES (%s,%s,%s,%s,%s,%s)"
    all_valid_asins = set()
    all_potential_related_pairs = []
    valid_product_count = 0
    prod_batch, review_batch, prodcat_batch = [], [], []
    def flush_batches():
        nonlocal prod_batch, review_batch, prodcat_batch
        if prod_batch: cur.executemany(prod_sql, prod_batch)
        if prodcat_batch: cur.executemany(prodcat_sql, prodcat_batch)
        if review_batch: cur.executemany(reviews_sql, review_batch)
        conn.commit()
        prod_batch, review_batch, prodcat_batch = [], [], []
    for product in parse_snap(input_file):
        asin = product.get('asin')
        titulo = product.get('title')
        if not asin or not titulo:
            continue
        all_valid_asins.add(asin)
        total_reviews = len(product['reviews'])
        avg_rating = round(sum(r['rating'] for r in product['reviews']) / total_reviews, 2) if total_reviews > 0 else None
        prod_batch.append((product['id'], asin, titulo, product['group'], product['salesrank'], total_reviews, avg_rating, 0))
        valid_product_count += 1
        for cat in product['categories']:
            new_cat_id = old_to_new_map.get(cat['old_id'])
            if new_cat_id:
                prodcat_batch.append((asin, new_cat_id))
        for sim in product['similar']:
            if sim and sim != asin:
                all_potential_related_pairs.append(tuple(sorted((asin, sim))))
        for r in product['reviews']:
            review_batch.append((asin, r['customer'], r['rating'], r['date'], r['votes'], r['helpful']))
        if valid_product_count > 0 and valid_product_count % BATCH_SIZE == 0:
            flush_batches()
            print(f"{valid_product_count} produtos válidos processados...")
    flush_batches()
    cur.close()
    print(f"Processamento de produtos finalizado. Total de produtos válidos: {valid_product_count}")
    return all_valid_asins, all_potential_related_pairs

@log_time
def insert_filtered_related_products(conn, valid_asins, potential_pairs):
    print("A filtrar e inserir produtos relacionados...")
    valid_pairs = set()
    for p1, p2 in potential_pairs:
        if p1 in valid_asins and p2 in valid_asins:
            valid_pairs.add((p1, p2))
    if not valid_pairs:
        print("Nenhuma relação válida entre produtos encontrada.")
        return
    print(f"Encontradas {len(valid_pairs)} relações válidas. A inserir na base de dados...")
    cur = conn.cursor()
    related_sql = "INSERT INTO Related_products (product1_asin, product2_asin) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    cur.executemany(related_sql, list(valid_pairs))
    conn.commit()
    cur.close()
    print("Inserção de produtos relacionados concluída.")


def main():
    parser = argparse.ArgumentParser(description="Script de ETL para o dataset Amazon SNAP.")
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    main_start_time = time.perf_counter()
    print("="*50)
    print("INICIANDO PROCESSO DE ETL")
    print("="*50)
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        schema_filepath = os.path.join(project_root, 'sql', 'schema.sql')
    except NameError:
        schema_filepath = 'sql/schema.sql'
    conn = get_conn(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass)
    try:
        create_schema(conn, schema_filepath)
        start_cat_extract = time.perf_counter()
        print("-> Iniciando etapa: 'extract_all_categories' (leitura do ficheiro)...")
        categories = extract_all_categories(args.input)
        end_cat_extract = time.perf_counter()
        print(f"<- Etapa 'extract_all_categories' concluída em {end_cat_extract - start_cat_extract:.4f} segundos.")
        print(f"Encontradas {len(categories)} categorias únicas.")
        id_map = insert_categories(conn, categories)
        insert_category_hierarchy(conn, categories, id_map)
        valid_asins, potential_pairs = process_products_and_reviews(conn, args.input, id_map)
        insert_filtered_related_products(conn, valid_asins, potential_pairs)
        print("\nProcesso de ETL concluído com sucesso!")
        sys.exit(0)
    except Exception as e:
        print(f"\nOcorreu um erro fatal durante o ETL: {e}")
    finally:
        if conn:
            conn.close()
            print("Conexão com a base de dados fechada.")
        main_end_time = time.perf_counter()
        total_etl_time = main_end_time - main_start_time
        print("="*50)
        print(f"FIM DO PROCESSO DE ETL. Tempo total de execução: {total_etl_time:.4f} segundos.")
        print("="*50)

if __name__ == "__main__":
    main()
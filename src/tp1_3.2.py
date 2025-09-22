
#!/usr/bin/env python3
"""
tp1_3.2.py - ETL otimizado para o SNAP amazon-meta.txt

Principais características:
- Duas passadas para categorias (coleta old_ids -> insere -> atualiza parent_id com novos ids).
- Parsing robusto e streaming (usa src/utils.py).
- Batch inserts/updates para minimizar round-trips ao banco.
- Calcula total_reviews e average_rating por produto.
- Não usa tabela Customers (insere customer_id direto em reviews).
- Comentários explicativos para o professor.
"""

import argparse
import os
from math import ceil

# Assumimos que src é package: ajustar imports conforme estrutura do seu repo
from src.utils import extract_all_categories, parse_snap
from src.db import get_conn

# Tuneable params
BATCH_SIZE = 2000   # quantos registros processar antes de um commit (ajuste conforme memória/DB)
CATEGORY_BATCH = 500  # quantos categories inserir por batch

def insert_categories(conn, categories_by_old_id):
    """
    Insere todas as categorias no banco (sem parent_id) em batches e retorna id_map: old_id -> new_id.
    categories_by_old_id: dict old_id -> {"name": name, "parent_old_id": parent_old_id}
    Estratégia:
    - Inserir nomes com ON CONFLICT DO NOTHING (em batches).
    - Depois selecionar os category_id para as names inseridas e mapear old_id -> new_id.
    """
    cur = conn.cursor()

    # 1) preparar lista única de nomes (preservando ordem não é necessário)
    old_id_to_name = {old_id: info['name'] for old_id, info in categories_by_old_id.items()}
    names = list(set(old_id_to_name.values()))  # lista de names únicos

    # 2) inserir names em batches usando ON CONFLICT DO NOTHING
    #    (não informamos parent_id nessa etapa)
    insert_sql = "INSERT INTO Categories (category_name) VALUES (%s) ON CONFLICT (category_name) DO NOTHING"
    for i in range(0, len(names), CATEGORY_BATCH):
        batch = names[i:i + CATEGORY_BATCH]
        cur.executemany(insert_sql, [(n,) for n in batch])
        conn.commit()  # commit por batch para liberar locks e reduzir memória

    # 3) buscar ids recém-criados (ou existentes) para todas as names
    #    Vamos buscar por names em lotes para não extrapolar tamanho de query
    name_to_newid = {}
    select_sql_base = "SELECT category_id, category_name FROM Categories WHERE category_name = ANY(%s)"
    for i in range(0, len(names), CATEGORY_BATCH):
        batch = names[i:i + CATEGORY_BATCH]
        cur.execute(select_sql_base, (batch,))
        for cid, cname in cur.fetchall():
            name_to_newid[cname] = cid

    # 4) construir old_id -> new_id usando o mapping via nome
    old_to_new = {}
    for old_id, name in old_id_to_name.items():
        new_id = name_to_newid.get(name)
        if new_id is None:
            # caso raro: talvez name contenha espaços invisíveis -> tentar trim
            new_id = name_to_newid.get(name.strip())
        if new_id is None:
            raise RuntimeError(f"Não encontrou novo id para categoria nome='{name}' (old_id={old_id})")
        old_to_new[old_id] = new_id

    cur.close()
    return old_to_new


def update_parent_ids(conn, categories_by_old_id, old_to_new):
    """
    Atualiza parent_id das categorias no banco em lote, convertendo parent_old_id -> parent_new_id.
    """
    cur = conn.cursor()
    # Construir lista de updates (new_parent_id, category_id) onde category_id é o novo id do category
    updates = []
    for old_id, info in categories_by_old_id.items():
        parent_old = info.get('parent_old_id')
        if parent_old is None:
            continue
        # parent_old can sometimes not be present if data is messy; guardamos isso
        parent_new = old_to_new.get(parent_old)
        if parent_new is None:
            # se está faltando, pulamos (ou poderíamos setar NULL). Aqui registramos para debug.
            print(f"[WARN] parent_old_id {parent_old} não encontrado para category old_id {old_id}")
            continue
        cat_new_id = old_to_new[old_id]
        updates.append((parent_new, cat_new_id))

    # Executar updates em batch usando executemany
    update_sql = "UPDATE Categories SET parent_id = %s WHERE category_id = %s"
    for i in range(0, len(updates), CATEGORY_BATCH):
        batch = updates[i:i + CATEGORY_BATCH]
        cur.executemany(update_sql, batch)
        conn.commit()
    cur.close()


def process_products_and_reviews(conn, input_file, old_to_new, batch_size=BATCH_SIZE):
    """
    Faz a segunda passada: lê produtos com parse_snap (stream) e insere produtos, product_category, related_products e reviews.
    Usa dicts em memória para evitar SELECTs repetidos:
      - seen_asins: evita re-inserir produtos já inseridos
      - category_name/id map já disponível via old_to_new mapping (através do nome)
    Usa batch inserts onde possível (acumula listas e executa executemany).
    """
    cur = conn.cursor()

    # helpers em memória:
    seen_asins = set()   # asins já inseridos
    # Build a mapping category_old_id->category_new_id (old_to_new já tem isso)
    category_old_to_new = old_to_new

    # preparar batches
    prod_batch = []
    prod_sql = """
        INSERT INTO Products (source_id, asin, titulo, group_name, salesrank, total_reviews, average_rating, qntd_downloads)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (asin) DO UPDATE
          SET titulo = EXCLUDED.titulo,
              group_name = EXCLUDED.group_name,
              salesrank = EXCLUDED.salesrank,
              total_reviews = EXCLUDED.total_reviews,
              average_rating = EXCLUDED.average_rating
    """

    prodcat_sql = "INSERT INTO Product_category (product_asin, category_id) VALUES (%s, %s) ON CONFLICT DO NOTHING"

    related_sql = "INSERT INTO Related_products (product1_asin, product2_asin) VALUES (%s, %s) ON CONFLICT DO NOTHING"

    reviews_sql = "INSERT INTO reviews (product_asin, customer_id, rating, review_date, votes, helpful) VALUES (%s,%s,%s,%s,%s,%s)"

    prod_count = 0
    review_batch = []
    prodcat_batch = []
    related_batch = []

    def flush_batches():
        nonlocal prod_batch, review_batch, prodcat_batch, related_batch
        if prod_batch:
            cur.executemany(prod_sql, prod_batch)
            prod_batch = []
        if prodcat_batch:
            cur.executemany(prodcat_sql, prodcat_batch)
            prodcat_batch = []
        if related_batch:
            cur.executemany(related_sql, related_batch)
            related_batch = []
        if review_batch:
            cur.executemany(reviews_sql, review_batch)
            review_batch = []
        conn.commit()

    # streaming parse
    for product in parse_snap(input_file):
        asin = product.get('asin')
        if not asin:
            # pular produtos inválidos
            continue

        # calcular total_reviews e average_rating
        total_reviews = len(product['reviews'])
        avg_rating = None
        if total_reviews > 0:
            s = 0
            for r in product['reviews']:
                s += r.get('rating', 0)
            avg_rating = round(s / total_reviews, 2)

        # preparar linha do produto
        prod_row = (
            product.get('id'),
            asin,
            product.get('title'),
            product.get('group'),
            product.get('salesrank'),
            total_reviews,
            avg_rating,
            0  # qntd_downloads default 0 (SNAP não fornece)
        )
        prod_batch.append(prod_row)
        prod_count += 1

        # --- Product_category ---
        for cat in product['categories']:
            old_id = cat.get('old_id')
            new_cat_id = category_old_to_new.get(old_id)
            if new_cat_id is None:
                # caso raro (categoria não mapeada), pular com aviso
                print(f"[WARN] Categoria old_id {old_id} não mapeada para produto {asin}")
                continue
            prodcat_batch.append((asin, new_cat_id))

        # --- Related_products ---
        # Inserimos pares (LEAST, GREATEST) para respeitar CHECK product1 < product2
        for sim in product['similar']:
            if sim and sim != asin:
                a, b = (asin, sim) if asin < sim else (sim, asin)
                related_batch.append((a, b))

        # --- Reviews ---
        for r in product['reviews']:
            # customer_id é string; reviews table tem customer_id as text/varchar
            review_batch.append((asin, r.get('customer'), r.get('rating'), r.get('date'), r.get('votes'), r.get('helpful')))

        # Flush por blocos para manter memória e performance
        if prod_count % batch_size == 0:
            flush_batches()
            print(f"{prod_count} produtos processados...")

    # fim do loop, flush final
    flush_batches()
    cur.close()
    print(f"Processamento finalizado. Produtos processados: {prod_count}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--input", required=True, help="caminho para amazon-meta.txt")
    args = parser.parse_args()

    # Conectar
    conn = get_conn(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass)
    try:
        # --- Passo 0: coletar todas as categories (old ids) em memória (uma leitura rápida do arquivo) ---
        print("Coletando categorias (old_id) — primeira passada (apenas nomes/old_ids)...")
        categories_by_old_id = extract_all_categories(args.input)
        print(f"Categorias únicas encontradas: {len(categories_by_old_id)}")

        # --- Passo 1: inserir categorias sem parent_id e mapear old_id -> new_id ---
        print("Inserindo categorias sem parent_id e mapeando old_id -> new_id...")
        old_to_new = insert_categories(conn, categories_by_old_id)
        print(f"Mapeamento old->new construído (ex.: {next(iter(old_to_new.items())) if old_to_new else 'nenhum'})")

        # --- Passo 2: atualizar parent_id em lote ---
        print("Atualizando parent_id das categorias (usando o mapeamento old->new)...")
        update_parent_ids(conn, categories_by_old_id, old_to_new)

        # --- Passo 3: segunda passada — processar produtos/reviews/related/product_category ---
        print("Processando produtos, product_category, related_products e reviews (segunda passada)...")
        process_products_and_reviews(conn, args.input, old_to_new)

    finally:
        conn.close()


if __name__ == "__main__":
    main()

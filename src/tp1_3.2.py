#!/usr/bin/env python3
# tp1_3.2.py (versão ajustada)
import argparse
import os
from src.utils import parse_snap
from src.db import get_conn

BATCH_SIZE = 1000

def find_schema_path(preferred="sql/schema.sql"):
    # tenta caminho padrão do repo e fallback para schema.sql na raiz
    if os.path.exists(preferred):
        return preferred
    if os.path.exists("schema.sql"):
        return "schema.sql"
    raise FileNotFoundError("schema.sql não encontrado (procure em sql/schema.sql ou schema.sql)")

def execute_schema(conn, schema_file=None):
    if schema_file is None:
        schema_file = find_schema_path()
    with open(schema_file, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def ensure_unique_title(cur, title, asin):
    """Garante que o campo titulo não viole UNIQUE: se já existir com outro asin, adiciona [asin]."""
    if not title:
        return asin or "unknown_title"
    cur.execute("SELECT asin FROM Products WHERE titulo = %s", (title,))
    row = cur.fetchone()
    if not row:
        return title
    existing_asin = row[0]
    if existing_asin == asin:
        return title
    # torna título único adicionando o asin
    return f"{title} [{asin}]"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--input", required=True, help="caminho para o arquivo amazon-meta.txt (SNAP)")
    parser.add_argument("--schema", required=False, help="caminho opcional para schema.sql")
    args = parser.parse_args()

    # Conecta ao banco (usa a função do seu módulo src.db)
    conn = get_conn(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass)

    # (Re)cria esquema
    execute_schema(conn, schema_file=args.schema)

    cur = conn.cursor()

    category_map = set()  # evita inserts repetidos de category_name

    count = 0
    for product in parse_snap(args.input):
        try:
            asin = product["asin"]
            title = product["title"]
            group_name = product["group"]  # no schema atual, Products tem group_name TEXT
            salesrank = product["salesrank"]

            # --- Products (garantindo título único) ---
            safe_title = ensure_unique_title(cur, title, asin)
            cur.execute(
                """
                INSERT INTO Products (source_id, asin, titulo, group_name, salesrank)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (asin) DO UPDATE
                  SET titulo = EXCLUDED.titulo,
                      group_name = EXCLUDED.group_name,
                      salesrank = EXCLUDED.salesrank
                """,
                (product["id"], asin, safe_title, group_name, salesrank),
            )

            # --- Categories & Product_category ---
            for cat in product["categories"]:
                if cat not in category_map:
                    cur.execute(
                        "INSERT INTO Categories (category_name) VALUES (%s) ON CONFLICT (category_name) DO NOTHING",
                        (cat,),
                    )
                    category_map.add(cat)
                cur.execute(
                    "INSERT INTO Product_category (product_asin, category_id) "
                    "SELECT %s, category_id FROM Categories WHERE category_name=%s "
                    "ON CONFLICT DO NOTHING",
                    (asin, cat),
                )

            # --- Related_products ---
            for sim in product["similar"]:
                if sim and sim != asin:
                    cur.execute(
                        "INSERT INTO Related_products (product1_asin, product2_asin) "
                        "VALUES (LEAST(%s,%s), GREATEST(%s,%s)) ON CONFLICT DO NOTHING",
                        (asin, sim, asin, sim),
                    )

            # --- Reviews ---
            for r in product["reviews"]:
                # reviews table no schema: (product_asin, customer_id, rating, review_date, votes, helpful)
                cur.execute(
                    """INSERT INTO reviews (product_asin, customer_id, rating, review_date, votes, helpful)
                       VALUES (%s,%s,%s,%s,%s,%s)""",
                    (asin, r["customer"], r["rating"], r["date"], r["votes"], r["helpful"]),
                )

            count += 1
            if count % BATCH_SIZE == 0:
                conn.commit()
                print(f"{count} produtos processados...")

        except Exception as e:
            # registra erro e tenta seguir em frente: faz rollback para limpar transação
            print(f"Erro ao processar produto ASIN={product.get('asin')} id={product.get('id')}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()
    print(f"Carga finalizada com sucesso. Total processado: {count}")

if __name__ == "__main__":
    main()

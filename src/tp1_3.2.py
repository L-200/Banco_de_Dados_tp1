
import argparse
from utils import parse_snap
from db import get_conn

def main():
    parser = argparse.ArgumentParser(description="Carga do SNAP para o banco de dados")
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--input", required=True, help="Arquivo SNAP de entrada")
    args = parser.parse_args()

    conn = get_conn(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass)
    cur = conn.cursor()

    # Antes de carregar, limpamos as tabelas (garantindo consistência)
    cur.execute("TRUNCATE Products CASCADE;")
    cur.execute("TRUNCATE Categories CASCADE;")
    cur.execute("TRUNCATE Customers CASCADE;")
    conn.commit()

    for product in parse_snap(args.input):
        # --- cálculo dos campos derivados ---
        total_reviews = len(product["reviews"])
        avg_rating = None
        if total_reviews > 0:
            soma = sum(r["rating"] for r in product["reviews"])
            avg_rating = soma / total_reviews

        # --- inserção do produto ---
        cur.execute("""
            INSERT INTO Products (asin, titulo, group_name, salesrank,
                                  total_reviews, average_rating, qntd_downloads)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (asin) DO NOTHING
            RETURNING product_id
        """, (
            product["asin"],
            product["title"],      # título vem exatamente do SNAP (sem alterar)
            product["group"],
            product["salesrank"],
            total_reviews,
            avg_rating,
            0   # downloads não existem no SNAP, então inicia com 0
        ))
        row = cur.fetchone()
        if row:
            product_id = row[0]
        else:
            # produto já estava no banco
            cur.execute("SELECT product_id FROM Products WHERE asin = %s", (product["asin"],))
            product_id = cur.fetchone()[0]

        # --- inserção das categorias ---
        for cat in product["categories"]:
            cur.execute("""
                INSERT INTO Categories (category_name) VALUES (%s)
                ON CONFLICT (category_name) DO NOTHING
                RETURNING category_id
            """, (cat,))
            row = cur.fetchone()
            if row:
                cat_id = row[0]
            else:
                cur.execute("SELECT category_id FROM Categories WHERE category_name = %s", (cat,))
                cat_id = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO Product_category (product_id, category_id)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, (product_id, cat_id))

        # --- inserção de produtos similares ---
        for sim_asin in product["similar"]:
            cur.execute("""
                INSERT INTO Products (asin, titulo) VALUES (%s, %s)
                ON CONFLICT (asin) DO NOTHING
                RETURNING product_id
            """, (sim_asin, None))
            row = cur.fetchone()
            if row:
                sim_id = row[0]
            else:
                cur.execute("SELECT product_id FROM Products WHERE asin = %s", (sim_asin,))
                sim_id = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO Related_products (product_id, related_asin)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, (product_id, sim_id))

        # --- inserção de reviews ---
        for r in product["reviews"]:
            cur.execute("""
                INSERT INTO Customers (customer_id) VALUES (%s)
                ON CONFLICT (customer_id) DO NOTHING
            """, (r["customer"],))

            cur.execute("""
                INSERT INTO Reviews (product_id, customer_id, review_date, rating, votes, helpful)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (product_id, r["customer"], r["date"], r["rating"], r["votes"], r["helpful"]))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()

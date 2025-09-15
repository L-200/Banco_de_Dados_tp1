import argparse
from src.utils import parse_snap
from src.db import get_conn

def execute_schema(conn, schema_file="sql/schema.sql"):
    with open(schema_file, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    # conecta ao banco
    conn = get_conn(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass)

    # recria as tabelas
    execute_schema(conn)

    # prepara cursor
    cur = conn.cursor()

    # dicionários auxiliares
    group_map = {}
    category_map = set()

    for product in parse_snap(args.input):
        asin = product["asin"]
        title = product["title"]
        group = product["group"]
        salesrank = product["salesrank"]

        # --- Groups ---
        if group and group not in group_map:
            cur.execute(
                "INSERT INTO Groups (group_name) VALUES (%s) ON CONFLICT (group_name) DO NOTHING RETURNING group_id",
                (group,),
            )
            row = cur.fetchone()
            if row:
                group_map[group] = row[0]
            else:
                cur.execute("SELECT group_id FROM Groups WHERE group_name=%s", (group,))
                group_map[group] = cur.fetchone()[0]

        # --- Products ---
        cur.execute(
            """INSERT INTO Products (source_id, asin, titulo, group_id, salesrank)
               VALUES (%s,%s,%s,%s,%s)
               ON CONFLICT (asin) DO NOTHING""",
            (product["id"], asin, title, group_map.get(group), salesrank,) # Adicione a vírgula aqui
        )
# ...
        # --- Categories ---
        for cat in product["categories"]:
            if cat not in category_map:
                cur.execute(
                    "INSERT INTO Categories (category_name) VALUES (%s) ON CONFLICT DO NOTHING RETURNING category_id",
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
            if sim != asin:
                cur.execute(
                    "INSERT INTO Related_products (product1_asin, product2_asin) "
                    "VALUES (LEAST(%s,%s), GREATEST(%s,%s)) ON CONFLICT DO NOTHING",
                    (asin, sim, asin, sim),
                )

        # --- Reviews + Customers ---
        for r in product["reviews"]:
            cust = r["customer"]
            cur.execute(
                "INSERT INTO Customers (customer_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (cust,),
            )
            cur.execute(
                """INSERT INTO Reviews (product_asin, customer_id, rating, review_date, votes, helpful)
                   VALUES (%s,%s,%s,%s,%s,%s)""",
                (asin, cust, r["rating"], r["date"], r["votes"], r["helpful"]),
            )

    conn.commit()
    conn.close()
    print("Carga finalizada com sucesso")

if __name__ == "__main__":
    main()

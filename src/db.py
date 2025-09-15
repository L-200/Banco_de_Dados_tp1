# src/db.py
import psycopg

def get_conn(host, port, dbname, user, password):
    return psycopg.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )

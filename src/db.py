import sys
import psycopg

# A função agora ACEITA parâmetros
def get_conn(host, port, dbname, user, password):
    """
    Cria e retorna uma conexão com o banco de dados PostgreSQL
    usando os parâmetros fornecidos.
    """
    try:
        conn_string = f"host={host} port={port} dbname={dbname} user={user} password={password}"
        return psycopg.connect(conn_string)

    except psycopg.OperationalError as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        sys.exit(1)
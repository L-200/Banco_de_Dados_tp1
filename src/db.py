import os
import sys
import psycopg

def get_conn():
    """
    Cria e retorna uma conexão com o banco de dados PostgreSQL.
    As configurações são lidas a partir de variáveis de ambiente.
    """
    try:
        # usa os.getenv() para ler as variáveis de ambiente passadas pelo docker-compose
        # o segundo parâmetro (ex: 'localhost') é um valor padrão caso a variável não esteja definida
        host = os.getenv('DB_HOST', 'localhost')
        port = os.getenv('DB_PORT', '5432')
        dbname = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')

        # validação para garantir que as variáveis essenciais foram definidas.
        if not all([dbname, user, password]):
            print("Erro: As variáveis de ambiente DB_NAME, DB_USER e DB_PASSWORD devem ser definidas.")
            sys.exit(1) # e o script se a configuração estiver incompleta

        #cCria a string de conexão
        conn_string = f"host={host} port={port} dbname={dbname} user={user} password={password}"
        
        # retorna a conexão
        return psycopg.connect(conn_string)

    except psycopg.OperationalError as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        # encerra o script em caso de falha na conexão para não continuar a execução.
        sys.exit(1)

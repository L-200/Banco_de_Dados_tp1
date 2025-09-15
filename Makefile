#indica que esses alvos não correspondem a arquivos e sim a apelidos de comandos
.PHONY: help up down etl logs shell clean

# primeiro alvo a ser executado quando 'make' é chamado sem argumentos
help:
	@echo "Comandos disponíveis para o projeto:"
	@echo ""
	@echo "  make up     -> Constrói as imagens e inicia todos os serviços em background."
	@echo "  make down   -> Para e remove os contêineres e redes."
	@echo "  make etl    -> Executa o script de ETL em um contêiner temporário."
	@echo "  make logs   -> Exibe os logs do serviço da aplicação em tempo real."
	@echo "  make shell  -> Abre um terminal (shell) dentro do contêiner da aplicação."
	@echo "  make clean  -> Para tudo e remove também os volumes (APAGA OS DADOS DO BANCO)."
	@echo ""

#constroi e inicia os serviços em background
up:
	docker compose up -d --build

#remove os containers e a rede criada para eles
down:
	docker compose down

# Executa o script de ETL como um comando unico em um conteiner que será removido no final.
# Corresponde ao 'docker compose run ...'
etl:
	docker compose run --rm -e PYTHONPATH=/app app python src/tp1_3.2.py --db-host db --db-port 5432 --db-name ecommerce --db-user postgres --db-pass postgres --input /data/snap_amazon.txt

# Mostra os logs do serviço 'app' e continua exibindo em tempo real (-f).
logs:
	docker compose logs -f app

# Executa um shell 'sh' dentro do contêiner do serviço 'app' para depuração.
shell:
	docker compose exec app sh

# Comando de limpeza mais agressivo: para os contêineres e remove os volumes de dados.
# Use com cuidado, pois apaga todos os dados do banco!
clean:
	docker compose down -v
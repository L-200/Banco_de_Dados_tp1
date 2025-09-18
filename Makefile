# setando variaveis para caso necessario alterações, não precisar digitar a mesma coisa duas vezes
DB_HOST ?= db
DB_PORT ?= 5432
DB_NAME ?= ecommerce
DB_USER ?= postgres
DB_PASS ?= postgres

# ?= define uma variável de ambiente ASIN com esse valor por padrão e variaveis Title e ID caso o user queira buscar por elas
ASIN ?= "DEFAULT_ASIN_CHANGE_ME"
TITLE ?= 
ID ?=

#indica que esses alvos não correspondem a arquivos e sim a apelidos de comandos
.PHONY: help up down etl dashboard logs shell clean

# primeiro alvo a ser executado quando 'make' é chamado sem argumentos
help:
	@echo "Comandos disponíveis para o projeto:"
	@echo ""
	@echo "  make up     -> Constrói as imagens e inicia todos os serviços em background."
	@echo "  make down   -> Para e remove os contêineres e redes."
	@echo "  make etl    -> Executa o script de ETL em um contêiner temporário."
	@echo "  make dashboard <var>=<valor> -> Executa as consultas para um produto específico."
	@echo "     Use: ASIN=..., TITLE=\"...\" ou ID=..."
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

# executa o script de etl como um comando unico em um conteiner que será removido no final
# corresponde ao 'docker compose run 3.2'
#passa as variaveis de conexão com o banco e o caminho do arquivo de dados como especificado na seção 4.1 do trabalho
etl:
	docker compose run --rm app python src/tp1_3.2.py \
		--db-host $(DB_HOST) \
		--db-port $(DB_PORT) \
		--db-name $(DB_NAME) \
		--db-user $(DB_USER) \
		--db-pass $(DB_PASS) \
		--input /data/snap_amazon.txt

# Executa o script de consultas do dashboard como um comando unico em um conteiner que será removido no final.
# Corresponde ao 'docker compose run 3.3'
# Passa as variaveis de conexão com o banco como especificado na seção 4.1 do trabalho
#logica if para poder buscar não só pelo asin (como foi dito que seria necessário no discord)
dashboard:
	$(eval PRODUCT_ARG := --product-asin $(ASIN))
	ifdef TITLE
		$(eval PRODUCT_ARG := --product-title "$(TITLE)")
	endif
	ifdef ID
		$(eval PRODUCT_ARG := --product-id $(ID))
	endif
	docker compose run --rm app python src/tp1_3.3.py \
		--db-host $(DB_HOST) \
		--db-port $(DB_PORT) \
		--db-name $(DB_NAME) \
		--db-user $(DB_USER) \
		--db-pass $(DB_PASS) \
		$(PRODUCT_ARG) \
		--output /app/out
		
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
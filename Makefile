# setando variaveis para caso necessario alterações, não precisar digitar a mesma coisa duas vezes
DB_HOST ?= db
DB_PORT ?= 5432
DB_NAME ?= ecommerce
DB_USER ?= postgres
DB_PASS ?= postgres

# para o comando dashboard, que pode receber um dos três argumentos opcionais
# se nenhum for passado, o comando roda sem nenhum filtro de produto
PRODUCT_ARG := # Começa vazia por padrão

ifdef ASIN
    PRODUCT_ARG := --product-asin $(ASIN)
endif
ifdef TITLE
    PRODUCT_ARG := --product-title "$(TITLE)"
endif
ifdef ID
    PRODUCT_ARG := --product-id $(ID)
endif

#indica que esses alvos não correspondem a arquivos e sim a apelidos de comandos
.PHONY: help up down etl dashboard logs clean

# primeiro alvo a ser executado quando 'make' é chamado sem argumentos
help:
	@echo "Comandos disponíveis para o projeto:"
	@echo ""
	@echo "  make up     -> Constrói as imagens e inicia todos os serviços em background."
	@echo "  make down   -> Para e remove os contêineres e redes."
	@echo "  make etl    -> Executa o script de ETL em um contêiner temporário."
	@echo "  make dashboard <var>=<valor> -> Executa as consultas para um produto específico."
	@echo "     Use: ASIN=..., TITLE=\"...\", ID=... ou só deixe ele vazio se não quiser as querys que dependem de um produto"
	@echo "  make logs   -> Exibe os logs do serviço da aplicação em tempo real."
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

# Comando de limpeza mais agressivo: para os contêineres e remove os volumes de dados.
# Use com cuidado, pois apaga todos os dados do banco!
clean:
	docker compose down -v
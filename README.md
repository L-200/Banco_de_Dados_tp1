# Sobre este trabalho:
Este projeto implementa ETL (Extração, Transformação e Carga) para processar um conjunto de dados de dados de produtos da Amazon. Os dados são lidos de um arquivo de texto, processados e carregados em um banco de dados PostgreSQL com um esquema projetado pela técnica bottom-up e depois normalizado (3FN). Após isso são feitas queries para se obter diversos dados interessantes sobre esse conjunto de dados.

Utiliza Docker e Docker-compose para facilitar a reprodutibilidade de sua execução.

# Requisitos:

- docker compose

- docker

# Opcional:
Se você quiser baixar o arquivo na pasta data junto com o repositorio você vai precisar do git lfs ativado em sua máquina. Caso você não queira utilizá-lo, você pode baixar o arquivo [aqui](https://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz).

# Como rodar:

## 1) Construir e subir os serviços:

```
make up
```

ou

```
docker compose up -d --build
```

## 2) (Opcional) conferir saúde dos containers

```
docker compose ps
```

## 3) Criar esquema e carregar dados

```
make etl
```

ou 

```
docker compose run --rm app python src/tp1_3.2.py \
		--db-host db \
		--db-port 5432 \
		--db-name ecommerce \
		--db-user postgres \
		--db-pass postgres \
		--input /data/snap_amazon.txt
```


## 4) Executar o Dashboard (todas as consultas)

```
make dashboard <ASIN, TITLE ou ID>=<asin, titulo (entre aspas) ou id>
```

ou 

```
docker compose run --rm app python src/tp1_3.3.py \
  --db-host db \
  --db-port 5432 \
  --db-name ecommerce \
  --db-user postgres \
  --db-pass postgres \
  --<product-asin, product-title ou product-id> <valor para identificar o item>\
  --output /app/out
``` 
---
Em caso de dúvida utilize o make help.
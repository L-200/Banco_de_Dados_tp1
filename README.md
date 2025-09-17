# Requisitos:
-docker compose

-docker

# Opcional:
Se você quiser baixar o arquivo em data junto com o repositorio você vai precisar de git lfs, caso contrário você pode baixar esse arquivo em [link para baixar o arquivo zip](https://snap.stanford.edu/data/bigdata/amazon/amazon-meta.txt.gz).

# 1) Construir e subir os serviços:
make up

# 2) (Opcional) conferir saúde do PostgreSQL
docker compose ps

# 3) Criar esquema e carregar dados
make etl

# 4) Executar o Dashboard (todas as consultas)
make dashboard
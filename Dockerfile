#ESTAGIO 1: configuração da base e dependências

#imagem base oficial do python baseada no bullseye do debian
FROM python:3.10-slim-bullseye
#essencial para garantir que os outputs (prints e logs) sejam exibidos no terminal em tempo real, permitindo melhor debuging e experiência de usuário
ENV PYTHONUNBUFFERED=1 
#impedir arquivos .pyc para criar imagens mais limpas e evitar possíveis problemas de compatibilidade
ENV PYTHONDONTWRITEBYTECODE=1
#definir o diretório de trabalho dentro do container
WORKDIR /app


#ESTAGIO 2: instalação das dependências

#copia e instala apenas as dependencias para aproveitar o cache do docker e evitar reinstalações desnecessárias caso ocorra mudanças no código
COPY requirements.txt .
#--no-cache-dir evita que o pip armazene em cache os pacotes baixados, economiza tamanho da imagem
#NOTA: esse --no-cache é um cache diferente do cache do docker, o docker reutilizará camadas antigas do SEU cache se ocorrer alterações, mas o  cache do pip não será armazenado
RUN pip install --no-cache-dir -r requirements.txt


#ESTAGIO 3: copiar o código da aplicação

#copia todo o código da aplicação para dentro do container
COPY . .


#ESTAGIO 4: comando padrão para rodar no inicio da aplicação

#popular o banco de dados ao iniciar o container
CMD ["python", "src/tp1_3.2.py"]
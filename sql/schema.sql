--limpeza de tabelas antigas caso por algum motivo não tenham sido apagadas
DROP TABLE IF EXISTS Product_category CASCADE;
DROP TABLE IF EXISTS Related_products CASCADE;
DROP TABLE IF EXISTS Reviews CASCADE;
DROP TABLE IF EXISTS Products CASCADE;
DROP TABLE IF EXISTS Categories CASCADE;
DROP TABLE IF EXISTS Customers CASCADE;
DROP TABLE IF EXISTS Product_groups CASCADE;

--tabela que guarda os groups
CREATE TABLE Product_groups (
    group_id   SERIAL,          
    group_name TEXT NOT NULL UNIQUE,

    PRIMARY KEY (group_id)
);

--tabela que guarda os clientes que fazem as reviews
CREATE TABLE Customers (
    customer_id VARCHAR(20),

    PRIMARY KEY (customer_id)
);

--tabela que guarda as categorias
CREATE TABLE Categories (
    category_id SERIAL, --vamos ter que gerar esse id 
    category_name TEXT NOT NULL UNIQUE,
    parent_id INT,

    PRIMARY KEY (category_id),
    FOREIGN KEY (parent_id) REFERENCES Categories(category_id) 
);

--tabela que guarda os produtos
CREATE TABLE Products (
    source_id INT UNIQUE, --os 3 tem que ser unic para podermos pesquisar com os 3
    asin VARCHAR(20),
    titulo TEXT NOT NULL UNIQUE, 
    group_id INT NOT NULL,
    salesrank INT,
    total_reviews INT DEFAULT 0,
    qntd_downloads INT DEFAULT 0,
    average_rating DECIMAL (3, 2),

    PRIMARY KEY (asin),
    Foreign Key (group_id) REFERENCES Groups(group_id)
);

--tabelas que guardam relações

--tabela que relaciona as reviews com os consumidores, os produtos e diz informações sobre essas reviews
CREATE TABLE reviews (
    review_id      SERIAL PRIMARY KEY,  --precisamos gerar
    product_asin   VARCHAR(20) NOT NULL,         
    customer_id    VARCHAR(20) NOT NULL,                
    rating         SMALLINT,            
    review_date    DATE,
    votes          INT DEFAULT 0, 
    helpful        INT DEFAULT 0,

    FOREIGN KEY (product_asin) REFERENCES products(asin),

    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),

    --checagem para garantir que a avaliação esteja entre 1 e 5
    CHECK (rating >= 1 AND rating <= 5)
);

--tabela que guarda os produtos relacionados entre si
CREATE TABLE Related_products (
    product1_asin VARCHAR(20),
    product2_asin VARCHAR(20),

    PRIMARY KEY (product1_asin, product2_asin),
    Foreign Key (product1_asin) REFERENCES Products(asin),
    Foreign Key (product2_asin) REFERENCES Products(asin),

    --restrição para não permitir que um produto seja relacionado a ele mesmo ou que haja duplicidade de relações
    CHECK (product1_asin < product2_asin)
);

CREATE TABLE Product_category (
    product_asin VARCHAR(20),
    category_id INT,

    PRIMARY KEY (product_asin, category_id),
    Foreign Key (product_asin) REFERENCES Products(asin),
    Foreign Key (category_id) REFERENCES Categories(category_id)
);
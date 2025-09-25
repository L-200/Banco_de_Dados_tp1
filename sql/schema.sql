--limpeza de tabelas antigas caso por algum motivo não tenham sido apagadas
DROP TABLE IF EXISTS Product_category CASCADE;
DROP TABLE IF EXISTS Related_products CASCADE;
DROP TABLE IF EXISTS Reviews CASCADE;
DROP TABLE IF EXISTS Products CASCADE;
DROP TABLE IF EXISTS Category_Hierarchy CASCADE;
DROP TABLE IF EXISTS Categories CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ProductReviewSummary; -- Adicionado por segurança

--tabela que guarda as categorias
CREATE TABLE Categories (
    category_id SERIAL PRIMARY KEY,
    category_source_id INT UNIQUE NOT NULL,
    category_name TEXT NOT NULL
);

-- Tabela que armazena explicitamente as relações de hierarquia
CREATE TABLE Category_Hierarchy (
    parent_category_id INT REFERENCES Categories(category_id),
    child_category_id INT REFERENCES Categories(category_id),
    PRIMARY KEY (parent_category_id, child_category_id),
    CHECK (parent_category_id <> child_category_id)
);

--tabela que guarda os produtos
CREATE TABLE Products (
    source_id INT NOT NULL UNIQUE,
    asin VARCHAR(20) PRIMARY KEY,
    titulo TEXT NOT NULL,
    group_name TEXT NOT NULL,
    salesrank INT,
    total_reviews INT DEFAULT 0,
    qntd_downloads INT DEFAULT 0,
    average_rating DECIMAL (3, 2),
    similar_products_count INT DEFAULT 0 NOT NULL,
    categories_count INT DEFAULT 0 NOT NULL
);

--tabela que relaciona as reviews com os consumidores, os produtos e diz informações sobre essas reviews
CREATE TABLE reviews (
    review_id      SERIAL PRIMARY KEY,
    product_asin   VARCHAR(20) NOT NULL REFERENCES Products(asin),         
    customer_id    VARCHAR(20) NOT NULL,                
    rating         SMALLINT NOT NULL,            
    review_date    DATE NOT NULL,
    votes          INT DEFAULT 0 NOT NULL, 
    helpful        INT DEFAULT 0 NOT NULL,
    CHECK (rating >= 1 AND rating <= 5)
);

--tabela que guarda os produtos relacionados entre si
CREATE TABLE Related_products (
    product1_asin VARCHAR(20) REFERENCES Products(asin),
    product2_asin VARCHAR(20) REFERENCES Products(asin),
    PRIMARY KEY (product1_asin, product2_asin),
    CHECK (product1_asin < product2_asin)
);

CREATE TABLE Product_category (
    product_asin VARCHAR(20) REFERENCES Products(asin),
    category_id INT REFERENCES Categories(category_id),
    PRIMARY KEY (product_asin, category_id) --já torna os 2 em not null
);


-- Resumo de reviews por produto (evita recalcular sempre)
CREATE MATERIALIZED VIEW ProductReviewSummary AS
SELECT
    product_asin,
    SUM(helpful) AS total_helpful,
    COUNT(*) AS total_reviews
FROM Reviews
GROUP BY product_asin;

-- Índices para joins e recursão
CREATE INDEX idx_reviews_product_asin
    ON Reviews(product_asin);

CREATE INDEX idx_product_category_product_asin
    ON Product_category(product_asin);

CREATE INDEX idx_product_category_category_id
    ON Product_category(category_id);

CREATE INDEX idx_categories_parent_id
    ON Category_Hierarchy(parent_category_id);

CREATE INDEX idx_child_category_id
    ON Category_Hierarchy(child_category_id);
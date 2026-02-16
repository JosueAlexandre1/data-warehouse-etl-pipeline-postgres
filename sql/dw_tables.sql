CREATE SCHEMA IF NOT EXISTS dw;
SET search_path TO dw;

CREATE TABLE dim_location (
    location_sk SERIAL PRIMARY KEY,
    country VARCHAR(50),
    region VARCHAR(50),
    state VARCHAR(50),
    city VARCHAR(50),
    postal_code VARCHAR(20)
);

CREATE TABLE dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    customer_name VARCHAR(255),
    segment VARCHAR(50),
    location_sk INTEGER,
    CONSTRAINT fk_location
        FOREIGN KEY (location_sk)
        REFERENCES dim_location(location_sk)
);


CREATE TABLE dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100)
);

CREATE TABLE dim_ship_mode (
    ship_mode_sk SERIAL PRIMARY KEY,
    ship_mode VARCHAR(50)
);

CREATE TABLE dim_date(
    date_sk INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER
);

CREATE TABLE fact_sales (
    sales_sk SERIAL PRIMARY KEY,

    customer_sk INTEGER NOT NULL,
    product_sk INTEGER NOT NULL,
    date_sk INTEGER NOT NULL,
    ship_mode_sk INTEGER NOT NULL,

    order_id VARCHAR(50),
    sales NUMERIC(10,2),

    CONSTRAINT fk_customer
        FOREIGN KEY (customer_sk)
        REFERENCES dim_customer(customer_sk),

    CONSTRAINT fk_product
        FOREIGN KEY (product_sk)
        REFERENCES dim_product(product_sk),

    CONSTRAINT fk_date
        FOREIGN KEY (date_sk)
        REFERENCES dim_date(date_sk),
    
    CONSTRAINT fk_ship_mode
        FOREIGN KEY (ship_mode_sk)
        REFERENCES dim_ship_mode(ship_mode_sk)
);

CREATE INDEX idx_fact_customer ON fact_sales(customer_sk);
CREATE INDEX idx_fact_product ON fact_sales(product_sk);
CREATE INDEX idx_fact_date ON fact_sales(date_sk);
CREATE INDEX idx_fact_ship_mode ON fact_sales(ship_mode_sk);
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def get_connection():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

def move_to_dw(engine):
    with engine.connect() as conn:
        
        print("Limpando DW para recarga total...")
        # O CASCADE limpa a fato e as dimensões de uma vez
        conn.execute(text("""
            TRUNCATE dw.fact_sales, dw.dim_customer, dw.dim_product, 
                     dw.dim_location, dw.dim_ship_mode, dw.dim_date 
            RESTART IDENTITY CASCADE;
        """))
        print("Iniciando migração Staging -> DW...")
        print("Iniciando migração Staging -> DW...")

        print("Carregando dim_location...")
        conn.execute(text("""
                          INSERT INTO dw.dim_location (country, region, state, city, postal_code)
                          SELECT DISTINCT country, region, state, city, postal_code
                          FROM staging.superstore_sales
                          ON CONFLICT DO NOTHING;
                          """))
        
        print("- Carregando dim_customer...")
        conn.execute(text("""
            INSERT INTO dw.dim_customer (customer_id, customer_name, segment, location_sk)
            SELECT DISTINCT s.customer_id, s.customer_name, s.segment, l.location_sk
            FROM staging.superstore_sales s
            JOIN dw.dim_location l ON s.postal_code::TEXT = l.postal_code 
                                  AND s.city = l.city
            ON CONFLICT DO NOTHING;
        """))

        # 3. POPULAR DIM_PRODUCT
        print("- Carregando dim_product...")
        conn.execute(text("""
            INSERT INTO dw.dim_product (product_id, product_name, category, sub_category)
            SELECT DISTINCT product_id, product_name, category, sub_category
            FROM staging.superstore_sales
            ON CONFLICT DO NOTHING;
        """))

        # 4. POPULAR DIM_SHIP_MODE
        print("- Carregando dim_ship_mode...")
        conn.execute(text("""
            INSERT INTO dw.dim_ship_mode (ship_mode)
            SELECT DISTINCT ship_mode FROM staging.superstore_sales
            ON CONFLICT DO NOTHING;
        """))

        # 5. POPULAR DIM_DATE (Gera o date_sk no formato YYYYMMDD)
        print("- Carregando dim_date...")
        conn.execute(text("""
            INSERT INTO dw.dim_date (date_sk, full_date, year, month, day)
            SELECT DISTINCT 
                CAST(TO_CHAR(order_date, 'YYYYMMDD') AS INTEGER),
                order_date,
                EXTRACT(YEAR FROM order_date),
                EXTRACT(MONTH FROM order_date),
                EXTRACT(DAY FROM order_date)
            FROM staging.superstore_sales
            ON CONFLICT DO NOTHING;
        """))

        # 6. POPULAR FACT_SALES (A "pescaria" final de todas as SKs)
        print("- Carregando fact_sales...")
        conn.execute(text("""
            INSERT INTO dw.fact_sales (customer_sk, product_sk, date_sk, ship_mode_sk, order_id, sales)
            SELECT 
                c.customer_sk,
                p.product_sk,
                CAST(TO_CHAR(s.order_date, 'YYYYMMDD') AS INTEGER),
                sm.ship_mode_sk,
                s.order_id,
                s.sales
            FROM staging.superstore_sales s
            JOIN dw.dim_customer c ON s.customer_id = c.customer_id
            JOIN dw.dim_product p ON s.product_id = p.product_id
            JOIN dw.dim_ship_mode sm ON s.ship_mode = sm.ship_mode
            ON CONFLICT DO NOTHING;
        """))

        conn.commit()
        print("✅ DW populado com sucesso!")

if __name__ == "__main__":
    engine = get_connection()
    move_to_dw(engine)
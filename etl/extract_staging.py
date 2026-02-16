import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def get_connection():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    data = os.getenv("POSTGRES_DB")

    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{data}"
    )   
    return engine

def extract_to_staging(engine):
    df = pd.read_csv("../data/superstore_messy.csv", encoding="utf-8")

    df.columns = [
        "row_id", "order_id", "order_date", "ship_date", "ship_mode",
        "customer_id", "customer_name", "segment",
        "country", "city", "state", "postal_code", "region",
        "product_id", "category", "sub_category", "product_name",
        "sales"
    ]

    df["order_date"] = pd.to_datetime(df["order_date"], dayfirst=True, format='mixed', errors='coerce')
    df["ship_date"] = pd.to_datetime(df["ship_date"], dayfirst=True, format='mixed', errors='coerce')

    df.to_sql(
        "superstore_sales",
        engine,
        schema="staging",
        if_exists="replace",
        index=False
    )

    print("Staging carregado com sucesso!")

if __name__ == "__main__":
    engine = get_connection()
    extract_to_staging(engine)

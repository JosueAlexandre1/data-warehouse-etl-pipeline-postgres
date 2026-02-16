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

def get_data(engine):
    print("Puxando os dados da staging...")
    df = pd.read_sql("SELECT * FROM staging.superstore_sales", engine)
    return df

def diagnose_data(df):
    print("=== RELATÓRIO DE DIAGNÓSTICO ===")
    
    # 1. Checar Nulos
    nulls = df.isnull().sum().sum()
    print(f"- Valores nulos encontrados: {nulls}")

    # 2. Checar duplicidade de texto ( Henderson vs henderson )
    raw_cities = df['city'].nunique()
    clean_cities = df['city'].str.strip().str.upper().nunique()
    if raw_cities != clean_cities:
        print(f"- Sujeira em 'city': {raw_cities - clean_cities} variações de escrita encontradas.")

    # 3. Checar integridade de ID (1 ID para 2 nomes)
    inconsistent_products = df.groupby('product_id')['product_name'].nunique()
    bad_ids = inconsistent_products[inconsistent_products > 1].count()
    if bad_ids > 0:
        print(f"- Alerta: {bad_ids} IDs de produtos possuem nomes diferentes no CSV.")
    print("================================")


def transform(df):
    print("Iniciando normalização dos dados...")
    
    df['sales'] = df['sales'].fillna(0)
    df['sales'] = df['sales'].astype(str).str.replace(',', '.')
    df['sales'] = pd.to_numeric(df['sales'], errors='coerce').fillna(0)
    df['customer_name'] = df['customer_name'].fillna('DESCONHECIDO')

    df['order_date'] = pd.to_datetime(df['order_date'])


    text_cols = ['city', 'state', 'category', 'sub_category', 'product_name']
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip().str.upper()
    

    print("Corrigindo inconsistência entre product_id e product_name...")

    mask_errado = df['product_name'].str.contains(
        "NOME TOTALMENTE ERRADO",
        na=False
    )

    nomes_validos = df[~mask_errado]

    nome_correto_por_id = (
        nomes_validos
        .groupby('product_id')['product_name']
        .agg(lambda x: x.mode()[0])
    )

    df['product_name'] = df['product_id'].map(nome_correto_por_id)
    print(
        df[df['product_name'].str.contains("ERRADO", na=False)]
        [['product_id', 'product_name']]
        .head(10)
    )
    return df

if __name__ == "__main__":
    engine = get_connection()
    df_raw = get_data(engine)
    diagnose_data(df_raw)
    df = transform(df_raw)
    
    print("Salvando dados limpos de volta na Staging...")
    df.to_sql('superstore_sales', engine, schema='staging', if_exists='replace', index=False)
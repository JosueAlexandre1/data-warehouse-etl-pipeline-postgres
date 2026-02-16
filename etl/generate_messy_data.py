import pandas as pd
import numpy as np
import os

# 1. Carrega o seu dataset atual
BASE_PATH = "../data"
FULL_DATASET = os.path.join(BASE_PATH, "superstore.csv")
SAMPLE_DATASET = os.path.join(BASE_PATH, "superstore_sample.csv")

if os.path.exists(FULL_DATASET):
    arquivo_entrada = FULL_DATASET
    print("Usando dataset completo: superstore.csv")

elif os.path.exists(SAMPLE_DATASET):
    arquivo_entrada = SAMPLE_DATASET
    print("superstore.csv não encontrado. Usando SAMPLE.")

else:
    raise FileNotFoundError(
        "Nenhum dataset encontrado!\n"
        "Coloque um dos arquivos abaixo em /data:\n"
        "- superstore.csv (completo)\n"
        "- superstore_sample.csv (exemplo)"
    )

df = pd.read_csv(arquivo_entrada)

# --- BAGUNÇANDO OS DADOS ---

# 2. Criando duplicidade de texto (Espaços e Case)
# Transforma algumas cidades em minúsculas e outras com espaços inúteis
df.loc[df.sample(frac=0.1).index, 'City'] = df['City'].str.lower()
df.loc[df.sample(frac=0.1).index, 'City'] = "  " + df['City'] + "  "

# 3. Criando inconsistência de ID (1 ID com nomes diferentes)
# Vamos pegar o ID de um produto e dar um nome totalmente diferente para ele em algumas linhas
target_product_id = df['Product ID'].iloc[0]
df.loc[df.sample(frac=0.05).index, 'Product Name'] = "NOME TOTALMENTE ERRADO"

# 4. Inserindo Valores Nulos (NaN)
# Apaga 5% das vendas e 5% dos nomes de clientes
for col in ['Sales', 'Customer Name']:
    df.loc[df.sample(frac=0.05).index, col] = np.nan

# 5. Bagunçando Datas (Transformando algumas em strings zoadas)
# Isso vai forçar seu pd.to_datetime a trabalhar
df.loc[df.sample(frac=0.05).index, 'Order Date'] = "01-01-1900" 

# 6. Salva a versão bagunçada
df.to_csv("../data/superstore_messy.csv", index=False)

print("Dataset 'superstore_messy.csv'")
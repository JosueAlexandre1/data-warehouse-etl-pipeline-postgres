import pandas as pd
import numpy as np
import os



BASE_PATH = "../data"
FULL_DATASET = os.path.join(BASE_PATH, "superstore.csv")
SAMPLE_DATASET = os.path.join(BASE_PATH, "superstore_sample.csv")
OUTPUT_DATASET = os.path.join(BASE_PATH, "superstore_messy.csv")

if os.path.exists(FULL_DATASET):
    arquivo_entrada = FULL_DATASET
    print("Usando dataset completo: superstore.csv")

elif os.path.exists(SAMPLE_DATASET):
    arquivo_entrada = SAMPLE_DATASET
    print("superstore.csv não encontrado. Usando SAMPLE.")

else:
    raise FileNotFoundError(
        "Nenhum dataset encontrado!\n"
        "Coloque em /data:\n"
        "- superstore.csv\n"
        "- superstore_sample.csv"
    )

df = pd.read_csv(arquivo_entrada)

print("Dataset carregado:", len(df), "linhas")




idx_city_lower = df.sample(frac=0.1, random_state=42).index
df.loc[idx_city_lower, "City"] = (
    df.loc[idx_city_lower, "City"].str.lower()
)


idx_city_space = df.sample(frac=0.1, random_state=7).index
df.loc[idx_city_space, "City"] = (
    "  " + df.loc[idx_city_space, "City"] + "  "
)


target_product_id = df["Product ID"].iloc[0]

mask = df["Product ID"] == target_product_id
idx_wrong_name = df[mask].sample(frac=0.3, random_state=10).index

df.loc[idx_wrong_name, "Product Name"] = "Nome errado teste"


for col in ["Customer Name", "City"]:
    idx_nulls = df.sample(frac=0.03, random_state=20).index
    df.loc[idx_nulls, col] = np.nan


idx_date_format = df.sample(frac=0.05, random_state=30).index

df.loc[idx_date_format, "Order Date"] = pd.to_datetime(
    df.loc[idx_date_format, "Order Date"],
    errors="coerce"
).dt.strftime("%d/%m/%Y")


df.to_csv(OUTPUT_DATASET, index=False)

print("\n✅ Dataset 'superstore_messy.csv' gerado com sucesso!")
print("Sujeiras aplicadas:")
print("- Inconsistência de escrita em City")
print("- Espaços extras")
print("- Product Name divergente para mesmo Product ID")
print("- Pequena taxa de valores nulos em dimensões")
print("- Datas em múltiplos formatos")

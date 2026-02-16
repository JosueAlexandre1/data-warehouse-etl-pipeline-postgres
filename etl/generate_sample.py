import pandas as pd
import os

BASE_PATH = "../data"
FULL_DATASET = os.path.join(BASE_PATH, "superstore.csv")
SAMPLE_DATASET = os.path.join(BASE_PATH, "superstore_sample.csv")

# Verifica se existe o dataset original
if not os.path.exists(FULL_DATASET):
    raise FileNotFoundError(
        "superstore.csv não encontrado.\n"
        "Baixe o dataset original antes de gerar o sample."
    )

print("Carregando dataset completo...")
df = pd.read_csv(FULL_DATASET)

# ==========================
# Criando sample
# ==========================
# 10% dos dados (ajuste se quiser)
sample_df = df.sample(frac=0.1, random_state=42)

# OU se preferir tamanho fixo:
# sample_df = df.sample(n=5000, random_state=42)

print(f"Sample criado com {len(sample_df)} linhas.")

sample_df.to_csv(SAMPLE_DATASET, index=False)

print("✅ superstore_sample.csv gerado com sucesso!")

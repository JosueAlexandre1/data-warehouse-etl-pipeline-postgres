import logging
from extract_staging import extract_to_staging, get_connection
from transform_data import transform, get_data
from load_dw import move_to_dw

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()]
)

def run_pipeline():
    try:
        engine = get_connection()
        logging.info("Iniciando conexão com Banco...")

        logging.info("Passo 1/3: Extraindo dados para o Staging...")
        extract_to_staging(engine)

        logging.info("Passo 2/3: Tratando e validando dados...")
        data = get_data(engine)
        transform(data)

        logging.info("Passo 3/3: Populando o Data Warehouse...")
        move_to_dw(engine)

        logging.info("Pipeline finalizado com sucesso!")

    except Exception as e:
        logging.error(f"Falha no Pipeline: {e}")

if __name__ == "__main__":
    run_pipeline()
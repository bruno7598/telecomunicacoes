from postgre_functions import chunk, insert
import pandas as pd
from config import PARQUET_PATH

# Endereço localizado no arquivo config.py
parquet_path = PARQUET_PATH

if __name__ == "__main__":
    try:
        print("Reading parquet")
        # Leitura arquivo parquet e conversao para dataframe pandas
        df_velocidade_contratada = pd.read_parquet(f"{parquet_path}/velocidade_contratada")
        df_velocidade_contratada['velocidade_contratada_mbps'] = df_velocidade_contratada['velocidade_contratada_mbps'].astype(float, errors = 'raise')
        
        print(">> Parquet read and converted to pandas dataframe")
        
        print(df_velocidade_contratada)
        print(df_velocidade_contratada.info())
        
        # Fatiamento dos dados em blocos para insercao
        chunked_velocidade_contratada = chunk(df_velocidade_contratada, 50000)
        
        # Insercao no Postgre
        insert(chunked_velocidade_contratada, f"INSERT INTO velocidade_contratada (ano, mes, razao_social, cnpj, velocidade_contratada_mbps, uf, municipio, codigo_ibge, acessos, tipo, municipio_uf) values %s;")
        
        print("Ingested data into Postgre")
        
    except Exception as e:
        print(str(e))
from postgre_functions import read_parquet, chunk, insert
import pandas as pd
from config import PARQUET_PATH

# Endereço localizados no arquivo config.py 
parquet_path = PARQUET_PATH

if __name__ == "__main__":
    try:
        print("Reading parquet")
        # Leitura arquivo parquet e conversao para dataframe pandas
        df_banda_larga = pd.read_parquet(f"{parquet_path}/banda_larga")
                
        print(">> Created Pandas Dataframe")
              
        print(df_banda_larga)
        print(df_banda_larga.info())              
  
        # Fatiamento dos dados em blocos para insercao     
        chunked_banda_larga = chunk(df_banda_larga, 50000)

        # Insercao no Postgre
        insert(chunked_banda_larga, f"INSERT INTO banda_larga (ano, mes, grupo_economico, empresa, cnpj, porte_da_prestadora, uf, municipio, codigo_ibge_municipio, faixa_de_velocidade, tecnologia, meio_de_acesso, tipo_de_pessoa, acessos) values %s;")
                
        print("Ingested data into Postgre")
        
    except Exception as e:
        print(str(e))
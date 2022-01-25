from config import PARQUET_PATH
from postgre_functions import read_parquet, chunk, insert

# Endereço localizado no arquivo config.py
parquet_path = PARQUET_PATH

if __name__ == "__main__":
    try:
        
        # Leitura arquivo parquet
        parquet_municipio_acessos = read_parquet(f"{parquet_path}/municipio_acessos")
        
        print(">> Parquet file read successfully")
        
        # Conversao do dataframe parquet para dataframe pandas
        df_municipio_acessos = parquet_municipio_acessos.toPandas()
        
        print(">> Created Pandas Dataframe")

        # Fatiamento dos dados em blocos para insercao
        chunked_municipio_acessos = chunk(df_municipio_acessos, 50000)
        
        # Insercao no Postgre
        insert(chunked_municipio_acessos, f"INSERT INTO municipios_acessos(ano, mes, acessos, servico, densidade, codigo_ibge, municipio, uf, nome_uf, regiao, codigo_nacional) values %s;")
        
        print("Ingested data into Postgre")
        
    except Exception as e:
        print(str(e))
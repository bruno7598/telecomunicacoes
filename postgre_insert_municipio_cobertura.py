from config import PARQUET_PATH
from postgre_functions import read_parquet, chunk, insert

# Endereço localizado no arquivo config.py
parquet_path = PARQUET_PATH

if __name__ == "__main__":
    try:
        
        # Leitura arquivo parquet
        parquet_municipio_cobertura = read_parquet(f"{parquet_path}/municipio_cobertura")
        
        print(">> Parquet file read successfully")
        
        # Conversao do dataframe parquet para dataframe pandas
        df_municipio_cobertura = parquet_municipio_cobertura.toPandas()
        
        print(">> Created Pandas Dataframe")

        # Fatiamento dos dados em blocos para insercao
        chunked_municipio_cobertura = chunk(df_municipio_cobertura, 50000)
        
        # Insercao no Postgre
        insert(chunked_municipio_cobertura, f"INSERT INTO municipios_cobertura(operadora, tecnologia_cobertura, moradores_cobertos, domicilios_cobertos, area_coberta, moradores_municipio, domicilios_municipio, area_municipio, ano, codigo_ibge, municipio, uf, nome_uf, regiao, codigo_nacional) values %s;")
        
        print("Ingested data into Postgre")
        
    except Exception as e:
        print(str(e))
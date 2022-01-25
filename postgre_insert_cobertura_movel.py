from config import PARQUET_PATH
from postgre_functions import read_parquet, chunk, insert

# Endereço localizados no arquivo config.py 
parquet_path = PARQUET_PATH

if __name__ == "__main__":
    try:

        # Leitura arquivo parquet
        parquet_cobertura_movel = read_parquet(f"{parquet_path}/cobertura_movel")
        
        print(">> Parquet file read successfully")

        # Conversao do dataframe parquet para dataframe pandas       
        df_cobertura_movel = parquet_cobertura_movel.toPandas()
        
        print(">> Created Pandas Dataframe")

        # Fatiamento dos dados em blocos para insercao    
        chunked_cobertura_movel = chunk(df_cobertura_movel, 50000)

        # Insercao no Postgre
        insert(chunked_cobertura_movel, f"INSERT INTO cobertura_movel (ano, operadora, tecnologia, codigo_setor_censitario, bairro, tipo_setor, codigo_localidade, nome_localidade, categoria_localidade, localidade_agregadora, codigo_municipio, municipio, uf, regiao, area, domicilios, moradores, percentual_cobertura) values %s;")
        
        print("Ingested data into Postgre")
        
    except Exception as e:
        print(str(e))
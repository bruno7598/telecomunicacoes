from config import PARQUET_PATH
from postgre_functions import read_parquet, chunk, insert

# Endereço localizado no arquivo config.py
parquet_path = PARQUET_PATH

if __name__ == "__main__":
    try:
        
        # Leitura arquivo parquet
        parquet_tv_assinatura = read_parquet(f"{parquet_path}/tv_assinatura")
        
        print(">> Parquet file read successfully")
                
        # Conversao do dataframe parquet para dataframe pandas
        df_tv_assinatura = parquet_tv_assinatura.toPandas()
        
        print(">> Created Pandas Dataframe")

        # Fatiamento dos dados em blocos para insercao
        chunked_tv_assinatura = chunk(df_tv_assinatura, 50000)
        
        # Insercao no Postgre
        insert(chunked_tv_assinatura, f"INSERT INTO tv_assinatura (ano, mes, grupo_economico, empresa, cnpj, porte_prestadora, uf, municipio, codigo_ibge_municipio, tecnologia, meio_acesso, tipo_pessoa, acessos) values %s;")
        
        print("Ingested data into Postgre")
        
    except Exception as e:
        print(str(e))
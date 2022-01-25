from pyspark.sql import SparkSession
import numpy as np
from psycopg2.extras import execute_values
from connector_postgre import Interface_db_postgre, get_db_info

def session():
    """Funcao inicia sessão com Spark
    
    Returns:
            spark: retorna a conexão do spark   
    """
    try:
        spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()
        return spark
    except Exception as e:
        print("Error cannot start session: ", str(e))
        
def read_parquet(path):
    """Funcao para ler arquivo parquet
    
    Returns:
            df_parquet: retorna dataframe parquet
    """
    try:
        df_parquet = session().read.parquet(path)
    except Exception as e:
        print("Error in def readParquet: ", str(e))
    else:
        return df_parquet

def chunk(dataframe, n):
    """Funcao que separa os dados em n dados
    """
    for i in range(0, len(dataframe), n): 
        yield dataframe[i:i + n]

def connection():
    """Funcao para conectar no banco Postgre usando a classe Interface_db_postgre

    Returns:
        con : conector sql
        cursor : cursor para leitura do banco
        db: conexao com o sql
    """
    try:
        user, password, host, database = get_db_info()
        db = Interface_db_postgre(user, password, host, database)                    
        con, cursor = db.connect()
        return con, cursor, db
    except Exception as e:
        print(">> Error cannot connect to Postgre: ", str(e)) 
        
def insert(chunked_banda_larga, query):
    """Funcao para inserir os dados no Postgre

    Args:
        chunked_banda_larga: dataframe
        query: query de insercao dos dados
    """
    try:
        con, cursor, db = connection()
        print(">> Successfully connected to Postgre")
        for df in chunked_banda_larga:
            data = np.array(df)
            list_postgre = []
            for item in data:
                value = tuple(item)
                list_postgre.append(value)   
            sql = query
            execute_values(cursor, sql, list_postgre)
            con.commit()
            print(">> Committing ")
        db.disconnect(con, cursor)        
    except Exception as e:
        print(">> Error entering data ", str(e)) 
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from connector_postgre import Interface_db_postgre, get_db_info


sheet_path = "C:/Users/thais/Desktop/AULA/Atividade_Final/data"

def chunk(dataframe):
    """
    Funcao que separa os dados em 50000 dados
    """
    for i in range(0, len(dataframe), 50000): 
        yield dataframe[i:i + 50000]

if __name__ == "__main__":
    try:
        # ----------------------------------------------------------
        # -- PLANILHA BANDA LARGA
        # ----------------------------------------------------------

        banda_larga = pd.read_csv(f"{sheet_path}/Acessos_Banda_Larga_Fixa_2021.csv", sep=";")
        
        print("leu planilha")

        banda_larga['Empresa'] = banda_larga['Empresa'].str.replace("'", "")
        banda_larga['Grupo Econômico'] = banda_larga['Grupo Econômico'].str.replace("'", "")
        banda_larga['Município'] = banda_larga['Município'].str.replace("'", "")
        banda_larga['Porte da Prestadora'] = banda_larga['Porte da Prestadora'].str.replace("'", "")
        banda_larga['Faixa de Velocidade'] = banda_larga['Faixa de Velocidade'].str.replace("'", "")
        banda_larga['Meio de Acesso'] = banda_larga['Meio de Acesso'].str.replace("'", "")
        banda_larga['Tecnologia'] = banda_larga['Tecnologia'].str.replace("'", "")
        banda_larga['Tipo de Pessoa'] = banda_larga['Tipo de Pessoa'].str.replace("'", "")

        print("tratou planilha")
        
        user, password, host, database = get_db_info()
        db = Interface_db_postgre(user, password, host, database)  
                              
        
        print("conexao ok")

        chunked_banda_larga = chunk(banda_larga)
        
        # INSERCAO DOS DADOS
        for df in chunked_banda_larga:
            try:
                values_list = []
                for _ , row in df.iterrows():
                    values = (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13])  
                    query = "INSERT INTO banda_larga (Ano, Mes, Grupo_Economico, Empresa, CNPJ, Porte_da_Prestadora, UF, Municipio, Codigo_IBGE_Municipio, Faixa_de_Velocidade, Tecnologia, Meio_de_Acesso, Tipo_de_Pessoa, Acessos) values %s "
                    values_list.append(values)
                execute_values(db, query, values_list)
                print("passou pelo execute_values")
                db.commit;
                print("aplicando commit")
            except Exception as e:
                print("Erro ao inserir dados ", str(e))
            
        db.disconnect()
        
        print("inseriu tudo :)")
        
    except Exception as e:
        print(str(e))







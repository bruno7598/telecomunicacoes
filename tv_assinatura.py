import os
from unittest import result
from cassandra.query import BatchStatement
from cassandra.cluster import Cluster, ProfileManager
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession
from modules.Tv_assinatura import Tv_assinatura
from modules.conector_postgree import Interface_db_postgree
from modules.conector_cassandra import Interface_db_cassandra
from psycopg2.extras import execute_values
import numpy as np
from datetime import datetime


def chunk(dataframe, size):

    for i in range(0, len(dataframe), size): 
        yield dataframe[i:i + size]



a = datetime.now()
if __name__ == "__main__":
    
    
    # lendo = Tv_assinatura(r"C:\Users\isa66\Desktop\Visualcode\.vscode\Telecomunicações\arquivos\tv_assinatura\Acessos_TV_Assinatura.csv")
    # lendo.tratamento_geral()
    # df_tv_assinatura = lendo.get_resultado()
    # print('tratado')    
    # chunked_banda_larga = chunk(df_tv_assinatura)
    
    
    # con = psycopg2.connect(user='postgres', password='Eugostode@55', host='localhost', database='telecomunicacao')
    # cursor = con.cursor()
    
    
    # print('conexao')
    # for df in chunked_banda_larga:
    #     try:
    #         data = np.array(df)
    #         list_postgre = []
    #         for item in data:
    #             value = tuple(item)
    #             list_postgre.append(value)   
    #         # list_postgre = str(list_postgre)[1:-1]
    #         sql = f"INSERT INTO tv_assinatura (ano ,mes ,grupo_economico ,Empresa ,CNPJ,porte_prestadora ,uf ,municipio ,codigo_ibge_municipio ,Tecnologia ,Meio_acesso ,tipo_pessoa ,acessos) values %s;"
    #         execute_values(cursor, sql, list_postgre)
    #         con.commit()
    #         print('inerindo')
    #     except Exception as e:
    #         print("Erro ao inserir dados ", str(e))
    
    conect_post = Interface_db_postgree('postgres','Eugostode@55', 'localhost', 'telecomunicacao')
    dados_cobertura = conect_post.select(query="select t1.*, t2.*, t3.*, t4.* from Operadoras t1 inner join AreaCobertura t2 on t2.id_cobertura = t1.id_cobertura inner join AreaMunipios t3 on t3.id_cobertura = t2.id_cobertura inner join LocaisCobertura t4 on t4.id_cobertura = t3.id_cobertura order by t1.id_cobertura;")
    df_cobertura = pd.DataFrame(dados_cobertura)
    df_cobertura.columns = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18']
    
    tips = df_cobertura.drop(columns=['4'])
    tips_1 = tips.drop(columns=['8'])
    resultado = tips_1.drop(columns=['13'])
    resultado.columns = ['id_cobertura','operadora','tecnologia_cobertura','Moradores_cobertos','Domicilios_cobertos','Area_coberta','Moradores_municipio','domicilios_Municipio','Area_municipio','Ano','codigo_ibge','municipio','uf','nome_uf','regiao','codigo_nacional']
    print(resultado)

    
    conect_cassa = Interface_db_cassandra("telecomunicacoes")
    
    
    query = """
            INSERT INTO municipio_cobertura (
               id_cobertura,
                operadora,
                tecnologia_cobertura,
                Moradores_cobertos,
                Domicilios_cobertos,
                Area_coberta,
                Moradores_municipio,
                domicilios_Municipio,
                Area_municipio,
                Ano,
                codigo_ibge,
                municipio,
                uf,
                nome_uf,
                regiao,
                codigo_nacional
            ) values (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """
        
    insert = conect_cassa.connector().prepare(query)
    chunked_df = chunk(resultado, 100)
    for df in chunked_df:
        batch = BatchStatement()
        for _, row in df.iterrows():
            batch.add(insert, tuple(row))
        print(">> executando batch")
        conect_cassa.inserir(batch)
    print(">> dados inseridos")
        
print("inseriu tudo")
        


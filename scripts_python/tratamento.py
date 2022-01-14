from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from unidecode import unidecode
import pyspark.sql.functions as tt
import re
from pyspark.sql.types import DecimalType, FloatType, StringType, IntegerType

""" 
Fazer cometário em todas as funções
Trocar nomes para ingles (switch,changeColumnName)

"""

def session():
       
    try:
        """Inicia sessão com Spark"""
        spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()
        return spark
    except Exception as e:
        print("Error in class ReadFormatCsv, def session: ", str(e))

def pathDatalake():
    try:
        """Caminho padrão a datalake"""
        #path = f"C:\Soul_Code_Eng_Dados\Soul_Code_Org\Python\\"
        path = f"C:\Soul_Code_Eng_Dados\Soul_Code_Org\Ativ_Final\Dataset\\"
        return path
    except Exception as e:
        print("Error in class ReadFormatCsv, def pathDatalake: ", str(e))

def file(file="curto_Meu_Municipio_Acessos.csv"):
    try:    
        """Usuário insere nome do arquivo para leitura"""
        #file = input("\nInsira o nome do arquivo e extensão: ")
        return file
    except Exception as e:
        print("Error in class ReadFormatCsv, def file: ", str(e))
        
def pathWarehouse():
    try:
        """Caminho padrão warehouse"""
        path = f"C:\Soul_Code_Eng_Dados\Soul_Code_Org\Ativ_Final\Dataset\Warehouse"
        return path
    except Exception as e:
        print("Error in class ReadFormatCsv, def pathWarehouse: ", str(e))

def readFormatCsv():
    try:
        """Local - Busca arquivo, aceita cabeçalho, aceita separador, aceita schema, aceita formatação de caracteres"""
        ses = session()
        #df = ses.read.csv(f"{pathDatalake()}{file()}", header=True, sep = ";", inferSchema=True, encoding='ISO-8859-1')
        df = ses.read.csv(f"{pathDatalake()}{file()}", header=True, sep = ";", inferSchema=True, encoding='UTF-8')
        return df
    except Exception as e:
        print("Error in class ReadFormatCsv, def readFormatCsv: ", str(e))
        
def mudaNome(df):
    try:
        lista_header_original=df.columns
        for header in lista_header_original:  
            str_temp=unidecode(header)
            
            list_words_split=str_temp.split(" ")
            
            str_temp="_".join(word for word in list_words_split)
            
            df=df.withColumnRenamed(header,str_temp)
        return df    
    except Exception as e:
        print(str(e)) 
          
def mudaSimbolo(df,nome_coluna,simbolo_antigo,simbolo_novo):
    
    try:
   
        df=df.withColumn(nome_coluna,tt.regexp_replace(nome_coluna, simbolo_antigo, simbolo_novo))
        return df  
    except Exception as e:
        print(str(e))
        
def casaDecimal(df,coluna_a_mudar,novo_tipo='teste'):
    """[summary]

    Args:
        df ([dataframe]): [description]
        coluna_a_mudar ([string]): [description]
        novo_tipo ([string]): [inteiro, decimal ou string]

    Returns:
        [type]: [description]
    """

    try:
        df=df.withColumn(coluna_a_mudar, tt.col(coluna_a_mudar).cast(DecimalType(38,2)))
        return df  
    except Exception as e:
        print(str(e))
        
def contNull(df):
    try:
        df.select([tt.count(tt.when(tt.isnull(c),c))\
            .alias(c) for c in df.columns])\
                .show(truncate=False)    
    except Exception as e:
        print(str(e))
    else:
        #df.show(truncate=False) retirar pós verificação
        return df

def fillNull(df,insert_value):
    for column in df.columns:
        df=df.na.fill(value=insert_value, subset=[column])
    return df

def createParquet(df):
    try:
        df.write.mode("overwrite").parquet(pathWarehouse())
    except Exception as e:
        print(str(e))

def readParquet(): 
    try:
        df_Parquet = session().read.parquet(pathWarehouse())       
    except Exception as e:
        print(str(e))
    else:
        return df_Parquet
        
#def mudaTipo(df,nome_coluna_mudar,tipo_novo):

#def mudaTamanhoLetra():

if __name__=="__main__":
    # ok retirar apostrofe e vírguls de todas as colunas
    # ok preencher nulls com valor
    # ok usar overwrite do parquet
    # fazer função para mudar tipo da coluna
    # contar quantidade de linhas e colunas
    try: 
        df=readFormatCsv()
        
        df=mudaNome(df)
        df.show()
        for coluna in df.columns:
            df=mudaSimbolo(df,coluna, "'","")
            df=mudaSimbolo(df,coluna, ",",".")
        
        
        df=casaDecimal(df,'Densidade')
        
        df=contNull(df)
        df=fillNull(df,'NULO')
        df=fillNull(df,-1)
        
        createParquet(df)
        
        df_Parquet=readParquet()
        
        print("Imprime parquet 10")
        print("------------------")
        df_Parquet.show(5, truncate=False)   
       
    except Exception as e:
        print(str(e))
        
    """
    * continumamos fazendo o tratamento de forma a criar rotina para tratar qualquer planilha
        * renomeação automática das colunas
        * preenchimento dos nulos
    * adequamos para funções em vez de classe
    * Discutir questão maiúscula minúscula
    * Colocar código no git
    
    Amanhã
    
    Meta: Entregar os códigos do tratamento de todas as planilhas
    Meta2: Buscar tratamento sofisticado de duplicatas
    
    * [1] Fazer tratamento de aspas, vírgula
    * [1] Fazer função alterar tipo de dado da coluna
    * [2] Contar colunas(shape)
    * [2] Mudar nomes funções para inglês
    * [3] Pesquisar sobre tratamentos de dados duplicados
    * [3] Discutir questão maiúscula/minúscula (regex)

    """

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from unidecode import unidecode
import pyspark.sql.functions as tt
from pyspark.sql.types import DecimalType, FloatType, StringType, IntegerType

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
        path = f"C:\Soul_Code_Eng_Dados\Soul_Code_Org\Ativ_Final\Dataset\\"
        return path
    except Exception as e:
        print("Error in class ReadFormatCsv, def pathDatalake: ", str(e))

def file(file="Meu_Municipio_Acessos.csv"):
    try:    
        """Usuário insere nome do arquivo para leitura"""
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
        df = ses.read.csv(f"{pathDatalake()}{file()}", header=True, sep = ";", inferSchema=True, encoding='UTF-8')
        return df
    except Exception as e:
        print("Error in class ReadFormatCsv, def readFormatCsv: ", str(e))
        
def changeName(df):
    """Adequação de nomes de colunas"""
    try:
        lista_header_original=df.columns
        for header in lista_header_original:  
            str_temp=unidecode(header)
            
            list_words_split=str_temp.split(" ")
            
            str_temp="_".join(word for word in list_words_split)
            
            df=df.withColumnRenamed(header,str_temp)
               
    except Exception as e:
        print("Error in def changeName: ", str(e)) 
    else:
        return df
          
def changeSymbol(df,columnName,ancientSymbol,newSymbol):
    """Altera caracteres e espaços vazios"""
    try:
        df=df.withColumn(columnName,tt.regexp_replace(columnName, ancientSymbol, newSymbol))
    except Exception as e:
        print("Error in def cahngeSymbol: ", str(e))
    else:
        return df
        
def changeType(df, columnChange, new_type):
    """Altera tipo de coluna"""
    try:        
        new_type=str(new_type)
        
        if new_type == '1':
            df=df.withColumn(columnChange, tt.col(columnChange).cast(IntegerType()))
            
        elif new_type == '2':
            df=df.withColumn(columnChange, tt.col(columnChange).cast(DecimalType(38,2)))
            
        elif new_type == '3':
            df=df.withColumn(columnChange, tt.col(columnChange).cast(StringType()))
         
        elif new_type == '4':
            df=df.withColumn(columnChange, tt.col(columnChange).cast(FloatType()))
            
    except Exception as e:
        print("Error in def changeType: ", str(e))
    else:
        return df
        
        
def contNull(df):
    """Conta campos nulos"""
    try:
        df.select([tt.count(tt.when(tt.isnull(c),c)).alias(c) for c in df.columns]).show(truncate=False)
    except Exception as e:
        print("Error in def contNull: ", str(e))
    else:
        return df

def fillNull(df, insert_value):
    """Inseri string(NULO) ou inteiro(-1) em campos nulos"""
    try:
        for column in df.columns:
            df=df.na.fill(value=insert_value, subset=[column])
        return df
    except Exception as e:
        print("Error in def fillNull: ", str(e))

def createParquet(df):
    """Cria parquet"""
    try:
        df.write.mode("overwrite").parquet(pathWarehouse())
    except Exception as e:
        print("Error in def createParquet: ", str(e))

def readParquet():
    """Lê parquet""" 
    try:
        df_Parquet = session().read.parquet(pathWarehouse())       
    except Exception as e:
        print("Error in def readParquet: ", str(e))
    else:
        return df_Parquet
    

if __name__=="__main__":
  
    try: 
        df=readFormatCsv()
        df.show(10, truncate=False)
        
        df=changeName(df)
        df.show(10, truncate=False)
        
        for coluna in df.columns:
            df=changeSymbol(df,coluna, "'","")
            df=changeSymbol(df,coluna, ",",".")
        
        df=changeType(df, 'Densidade', '2')
        
        df=contNull(df)
        df=fillNull(df,'NULO')
        df=fillNull(df,-1)
        df.show(10, truncate=False)
        
        createParquet(df)
        
        df_Parquet=readParquet()
        
        print("Imprime parquet 10")
        print("------------------")
        df_Parquet.show(5, truncate=False)   
       
    except Exception as e:
        print(str(e))
 
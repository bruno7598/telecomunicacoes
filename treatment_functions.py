from pyspark.sql import SparkSession
from unidecode import unidecode
import pyspark.sql.functions as tt
from pyspark.sql.types import DecimalType, FloatType, StringType, IntegerType, LongType

def session():
    """Inicia sessão com Spark"""
    try:
        spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()
        return spark
    except Exception as e:
        print("Error cannot start Spark session: ", str(e))

def read_format_csv(path):
    """Local - Busca arquivo, aceita cabeçalho, aceita separador, aceita schema, aceita formatação de caracteres"""
    try:
        ses = session()
        df = ses.read.csv(path, header=True, sep = ";", inferSchema=True, encoding='UTF-8')
        return df
    except Exception as e:
        print("Error in def read_format_csv: ", str(e))
        
def change_name(df):
    """Adequação de nomes de colunas, no header substitui os espaços por underscore"""
    try:
        header_list=df.columns
        for header in header_list:  
            str_temp=unidecode(header)
            list_words_split=str_temp.split(" ")
            str_temp="_".join(word for word in list_words_split)
            df=df.withColumnRenamed(header,str_temp)
        header_list=df.columns
        for header in header_list:  
            str_temp=unidecode(header)
            list_words_split=str_temp.split("(")
            str_temp="".join(word for word in list_words_split)
            df=df.withColumnRenamed(header,str_temp)
        header_list=df.columns
        for header in header_list:  
            str_temp=unidecode(header)
            list_words_split=str_temp.split(")")
            str_temp="".join(word for word in list_words_split)
            df=df.withColumnRenamed(header,str_temp)
    except Exception as e:
        print("Error in def change_name: ", str(e)) 
    else:
        return df
          
def change_symbol(df, column_name, ancient_symbol, new_symbol):
    """Altera caracteres e espaços vazios"""
    try:
        df=df.withColumn(column_name,tt.regexp_replace(column_name, ancient_symbol, new_symbol))
    except Exception as e:
        print("Error in def cahnge_symbol: ", str(e))
    else:
       return df
        
def change_type(df, column_change, new_type):
    """Altera tipo de coluna para integer, decimal, string e float"""
    try:
        new_type=str(new_type)

        if new_type == '1':
            df=df.withColumn(column_change, tt.col(column_change).cast(IntegerType()))
            
        elif new_type == '2':
            df=df.withColumn(column_change, tt.col(column_change).cast(DecimalType(38,4)))
            
        elif new_type == '3':
            df=df.withColumn(column_change, tt.col(column_change).cast(StringType()))
         
        elif new_type == '4':
            df=df.withColumn(column_change, tt.col(column_change).cast(FloatType()))
        
        elif new_type == '5':
            df=df.withColumn(column_change, tt.col(column_change).cast(LongType()))
            
    except Exception as e:
        print("Error in def change_type: ", str(e))
    else:
        return df
        
def cont_null(df):
    """Conta campos nulos e apresenta os resultados"""
    try:
        df.select([tt.count(tt.when(tt.isnull(c),c)).alias(c) for c in df.columns]).show(truncate=False)
    except Exception as e:
        print ("Error in def cont_null: ", str(e))
    else:
        return df

def fill_null(df, insert_value):
    """Insere string(NULO) ou inteiro(-1) em campos nulos"""
    try:
        for column in df.columns:
            df=df.na.fill(value=insert_value, subset=[column])
        return df
    except Exception as e:
        print("Error in def fill_null: ", str(e))

def create_parquet(df, path):
    """Cria arquivo parquet"""
    try:
        df.write.mode("overwrite").parquet(path)
    except Exception as e:
        print("Error in def create_parquet: ", str(e))  
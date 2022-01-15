from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from unidecode import unidecode
import pyspark.sql.functions as tt
from pyspark.sql.types import DecimalType, FloatType, StringType, IntegerType

""" 
Fazer cometário em todas as funções
Nomes de Metodos e atributos em ingles

"""

def session():
    """Inicia sessão com Spark"""   
    try:
        spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()
    except Exception as e:
        print("Error in class ReadFormatCsv, def session: ", str(e))
    else:
        return spark

def pathDatalake():
    """Caminho padrão a datalake"""
    try:
        path = f"C:\Soul_Code_Eng_Dados\Soul_Code_Org\Ativ_Final\Dataset\\"
    except Exception as e:
        print("Error in class ReadFormatCsv, def pathDatalake: ", str(e))
    else:
        return path

def file():
    """Usuário insere nome do arquivo para leitura"""
    try: 
        file = f"nome do arquivo"
    except Exception as e:
        print("Error in class ReadFormatCsv, def file: ", str(e))
    else:
        return file
        
def pathWarehouse():
    try:
        """Caminho padrão warehouse"""
        path = f"C:\Soul_Code_Eng_Dados\Soul_Code_Org\Ativ_Final\Dataset\Warehouse"
    except Exception as e:
        print("Error in class ReadFormatCsv, def pathWarehouse: ", str(e))
    else:
        return path

def readFormatCsv():
    try:
        """Local - Busca arquivo, aceita cabeçalho, aceita separador, aceita schema, aceita formatação de caracteres"""
        ses = session()
        df = ses.read.csv(f"{pathDatalake()}{file()}", header=True, sep = ";", inferSchema=True, encoding='UTF-8')
    except Exception as e:
        print("Error in class ReadFormatCsv, def readFormatCsv: ", str(e))
    else:
        return df
        
def changeName(df):
    """Adequação de nomes de colunas"""
    try:            
        lista_header_original=df.columns
        for header in lista_header_original:
            str_temp=unidecode(header)
            list_words_split=str_temp.replace("(", "")
            str_temp="^".join(word for word in list_words_split)
            df=df.withColumnRenamed(header,list_words_split)
            
        lista_header_original=df.columns
        for header in lista_header_original:
            str_temp=unidecode(header)
            list_words_split=str_temp.replace(")", "")
            str_temp="^".join(word for word in list_words_split)
            df=df.withColumnRenamed(header,list_words_split)
            
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

def informCharacter(df):
    try:
         for column in df.columns:
            df=changeSymbol(df,column, "'","")
            df=changeSymbol(df,column, ",",".")
            #df=changeSymbol(df,column, "caracter a ser removido","caracter desejddo")
    except Exception as e:
        print("Error in def changeCharacter: ", str(e))
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
    
def informType(df):
    """Estancia coluna para mudança de tipos
    TIPOS:
    [1] INTEIRO
    [2] DECIMAL
    [3] STRING
    [4] FLOAT
    """
    try:
        #df=changeType(df, "nome da coluna a ser alterado o tipo de dado", "cod tipo desejado" (consultar em def changeType))
        df=changeType(df, "Ano", 1)
    except Exception as e:
        print("Error in def changeCharacter: ", str(e))
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
    except Exception as e:
        print("Error in def fillNull: ", str(e))
    else:
        return df

def createParquet(df):
    """Cria e reescreve parquet"""
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
         
def kitShow(df):
    try: 
        df.show(5, truncate=False)
        print(file())
        df.printSchema()
        print("QUANTIDADE DE LINHAS: ",df.count())
        print("QUANTIDADE DE COLUNAS: ", len(df.columns)) 
    except Exception as e:
        print("Error in def kitShow: ", str(e))
    else:
        return df

if __name__=="__main__":
    
    try: 
        """Insira o nome do arquivo na 'def file'"""
        """Insira o caminho do arquivo na 'def path'"""
        """LEITURA DE CSV"""
        df=readFormatCsv()
        kitShow(df)

        df=changeName(df)
        """\nPROVA ALTERAÇÃO DE NOME DE COLUNAS"""
        kitShow(df)
        
        """Informa os caracteres que serão alterados"""
        df=informCharacter(df)
       
        """Informa as colunas e o tipo de dado desejado"""
        df=informType(df)
 
        """\nPROVA ALTERAÇÃO DE TIPO DE COLUNAS"""
        kitShow(df)

        df=contNull(df)
        df=fillNull(df,'NULO')
        df=fillNull(df,-1)
        
        """\nPROVA CONTAGEM E PREENCHIMENTO DE CAMPOS NULOS"""
        kitShow(df)
        
        """CRIA E REESCREVE PARQUET"""
        createParquet(df)
        
        """LEIRUTA DE PARQUET"""
        df_Parquet=readParquet()
        
        print("\nPROVA CRIAÇÃO DE PARQUET 5")
        print("------------------")
        kitShow(df_Parquet)
            
    except Exception as e:
        print(str(e))
        
  
    """
    * continumamos fazendo o tratamento de forma a criar rotina para tratar qualquer planilha
        * renomeação automática das colunas
        * preenchimento dos nulos
    * adequamos para funções em vez de classe
    * Discutir questão maiúscula minúscula
    * Colocar código no git
 
    """

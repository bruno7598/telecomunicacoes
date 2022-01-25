from treatment_functions import read_format_csv, change_name, change_symbol, cont_null, fill_null, create_parquet, change_type
from config import PARQUET_PATH, SHEET_PATH
    
# Endereços localizados no arquivo config.py
sheet_path = SHEET_PATH
parquet_path = PARQUET_PATH
   
if __name__=="__main__":
    try: 
        
        # Leitura do arquivo CSV    
        df=read_format_csv(f"{sheet_path}/Cobertura_Todas.csv")
        print(">> CSV file")
        df.show(10, truncate=False)

        # Correcao do header
        df=change_name(df)
        print(">> Header changed")
        df.show(10, truncate=False)

        df.printSchema()
        
        # Altera caracteres
        for column in df.columns:
            df=change_symbol(df,column, "'","")
            df=change_symbol(df,column, ",",".")
        
        # Altera tipo de dado
        df=change_type(df, "Ano", 1)
        df=change_type(df, "Codigo_Setor_Censitario", 5)
        df=change_type(df, "Codigo_Localidade", 5)
        df=change_type(df, "Codigo_Municipio", 1)
        df=change_type(df, "Area_km2", 2)
        df=change_type(df, "Domicilios", 1)
        df=change_type(df, "Moradores", 1)
        df=change_type(df, "Percentual_Cobertura", 2)

        # Conta os campos nulos e substitui
        print(">> Count of null fields")
        df=cont_null(df)
        df=fill_null(df,'NULO')
        df=fill_null(df,-1)
        
        print(">> CSV file treated to parquet file")
        df.printSchema()    
        df.show(10, truncate=False)            
    
        # Criacao do arquivo parquet
        create_parquet(df, f"{parquet_path}/cobertura_movel")
        print(">> Parquet file created")
        
    except Exception as e:
        print(str(e))
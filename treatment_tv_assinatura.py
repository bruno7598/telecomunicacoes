from treatment_functions import read_format_csv, change_name, change_symbol, cont_null, fill_null, create_parquet, change_type
from config import PARQUET_PATH, SHEET_PATH

# Endereços localizados no arquivo config.py
sheet_path = SHEET_PATH
parquet_path = PARQUET_PATH

if __name__=="__main__":
    try: 
        
        # Leitura do arquivo CSV  
        df=read_format_csv(f"{sheet_path}/Acessos_TV_Assinatura.csv")
        print(">> CSV file")
        df.show(10, truncate=False)
        
        # Correcao do header
        df=change_name(df)
        print(">> Header changed")
        df.show(10, truncate=False)
        
        df.printSchema()
        
        # Altera caracteres
        for coluna in df.columns:
            df=change_symbol(df,coluna, "'","")
            df=change_symbol(df,coluna, ",",".")
        
        # Altera tipo de dado
        df=change_type(df, 'Ano', 1)
        df=change_type(df, 'Mes', 1)
        df=change_type(df, 'CNPJ', 5)
        df=change_type(df, 'Codigo_IBGE_Municipio', 1)
        df=change_type(df, 'Acessos', 1)
        
        # Conta os campos nulos e substitui   
        print(">> Count of null fields")
        df=cont_null(df)
        df=fill_null(df,'NULO')
        df=fill_null(df,-1)
        
        print(">> CSV file treated to parquet file")
        df.printSchema()
        df.show(10, truncate=False)
        
        # Criacao do arquivo parquet
        create_parquet(df, f"{parquet_path}/tv_assinatura")
        print(">> Parquet file created")
       
    except Exception as e:
        print(str(e))
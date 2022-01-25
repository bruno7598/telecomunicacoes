import pandas as pd
from connector_postgre import Interface_db_postgre, get_db_info
from postgre_functions import connection, chunk
from connector_cassandra import Interface_db_cassandra, get_db_info
from cassandra.query import BatchStatement
from config import PARQUET_PATH, CASSANDRA_HOST

parquet_path = PARQUET_PATH
cassandra_host = CASSANDRA_HOST

if __name__ == "__main__":
    try:
        
        # Conexao Postgre
        con, cursor, db_postgre = connection()
        print(">> Successfully connected to Postgre")
        
        # Select nas views do Postgre
        query = "SELECT t1.id_acessos, t1.ano, t1.mes, t1.acessos, t1.servico, \
                t2.densidade, t2.codigo_ibge, t2.municipio, t2.uf, t2.nome_uf, t2.regiao, t2.codigo_nacional \
                FROM ServicosMunicipios t1 \
                INNER JOIN LocaisAcessos t2 ON t1.id_acessos = t2.id_acessos \
                ORDER BY t1.id_acessos;"
        municipios_acessos = db_postgre.select(query)
    
        # Conversao dataframe pandas
        df_municipios_acessos = pd.DataFrame(municipios_acessos)
        
        print(">> Created Pandas Dataframe")
        print(df_municipios_acessos.info())
        print(df_municipios_acessos)
        
        # Conexao Cassandra
        keyspace = get_db_info()
        db_cassandra = Interface_db_cassandra(keyspace=keyspace, cassandra_host=cassandra_host)  
        print(">> Successfully connected to Cassandra")
        
        # Insercao no Cassandra
        query = """
            INSERT INTO municipios_acessos (
                id_acessos,
                ano, 
                mes, 
                acesso, 
                servico, 
                densidade, 
                codigo_ibge, 
                municipio, 
                uf, 
                nome_uf, 
                regiao, 
                codigo_nacional
            ) values (
                ?,?,?,?,?,?,?,?,?,?,?,?
            )
        """
        insert = db_cassandra.connect().prepare(query)
        chunked_df = chunk(df_municipios_acessos, 100)
        for df in chunked_df:
            batch = BatchStatement()
            for _, row in df.iterrows():
                batch.add(insert, tuple(row))
            print(">> Inserting in batch")
            db_cassandra.execute(batch)
        print(">> Ingested data into Cassandra")

    except Exception as e:
        print(str(e))
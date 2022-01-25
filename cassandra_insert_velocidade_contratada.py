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
        query = "SELECT t1.id_velocidade_contratada, t1.ano, t1.mes, t1.razao_social, t1.cnpj, t1.velocidade_contratada_mbps, \
                t2.uf, t2.municipio, t2.codigo_ibge, t2.acessos, t2.tipo, t2.municipio_uf \
                FROM EmpresaVelocidade t1 INNER JOIN LocalidadeVelocidade t2 \
                ON t1.id_Velocidade_contratada = t2.id_Velocidade_contratada ORDER BY t1.id_velocidade_contratada;"
                
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
            INSERT INTO velocidade_contratada (
                id_velocidade_contratada,
                ano,
                mes,
                razao_social,
                cnpj,
                velocidade_contratada_mbps,
                uf,
                municipio,
                codigo_ibge,
                acessos,
                tipo,
                municipio_uf
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
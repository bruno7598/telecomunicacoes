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
        
        query = "SELECT t1.id_cobertura, t1.operadora, t1.tecnologia_cobertura,  \
                t2.moradores_cobertos, t2.domicilios_cobertos, t2.area_coberta, \
                t3.moradores_municipio, t3.domicilios_municipio, t3.area_municipio, t1.ano,\
                t4.codigo_ibge, t4.municipio, t4.uf, t4.nome_uf, t4.regiao, t4.codigo_nacional \
                FROM OperadorasCobertura t1 INNER JOIN AreaCobertura t2 \
                ON t2.id_cobertura = t1.id_cobertura \
                INNER JOIN AreaMunicipios t3 ON t3.id_cobertura = t2.id_cobertura \
                INNER JOIN LocaisCobertura t4 ON t4.id_cobertura = t3.id_cobertura ORDER BY t1.id_cobertura;"
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
            INSERT INTO municipios_cobertura (
                id_cobertura,
                operadora,
                tecnologia_cobertura,
                moradores_cobertos,
                domicilios_cobertos,
                area_coberta,
                moradores_municipio,
                domicilios_municipio,
                area_municipio,
                ano,
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
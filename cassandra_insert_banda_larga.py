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
        query = "SELECT t1.id_banda, t1.ano, t1.mes, t1.grupo_economico, t1.empresa, t1.cnpj, t1.porte_da_prestadora,\
                t2.uf, t2.municipio, t2.codigo_ibge_municipio,\
                t3.faixa_de_velocidade, t3.tecnologia, t3.meio_de_acesso,\
                t4.tipo_de_pessoa, t4.acessos \
                FROM EmpresaBandaLarga t1 INNER JOIN LocalidadeBandaLarga t2 \
                ON t2.id_banda = t1.id_banda \
                INNER JOIN TecnologiaBandaLarga t3 ON t3.id_banda = t2.id_banda \
                INNER JOIN AcessosBandaLarga t4 ON t4.id_banda = t3.id_banda;"
                
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
            INSERT INTO banda_larga (
                id_banda,
                ano,
                mes,
                grupo_economico,
                empresa,
                cnpj,
                porte_da_prestadora,
                uf,
                municipio,
                codigo_ibge_municipio,
                faixa_de_velocidade,
                tecnologia,
                meio_de_acesso,
                tipo_de_pessoa,
                acessos
                ) values (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
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
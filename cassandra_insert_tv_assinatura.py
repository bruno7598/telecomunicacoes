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
        query = "SELECT t1.id_tv_assinatura, t1.ano, t1.mes, t1.grupo_economico, t1.empresa, t1.cnpj, t1.porte_prestadora,\
                t2.uf, t2.municipio, t2.codigo_ibge_municipio, \
                t1.tecnologia, t1.meio_acesso, \
                t3.tipo_pessoa, t3.acessos \
                FROM EmpresaTVassinatura t1 INNER JOIN LocalidadeTvAssinatura t2 \
                ON t2.id_tv_assinatura = t1.id_tv_assinatura \
                INNER JOIN TipoPessoaTvAssinatura t3 ON t3.id_tv_assinatura = t2.id_tv_assinatura ORDER BY t1.id_tv_assinatura;"
                
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
            INSERT INTO tv_assinatura (
                id_tv_assinatura,
                ano,
                mes,
                grupo_economico,
                empresa,
                cnpj,
                porte_prestadora,
                uf,
                municipio,
                codigo_ibge_municipio,
                tecnologia,
                meio_acesso,
                tipo_pessoa,
                acessos
            ) values (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?
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
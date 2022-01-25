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
        query = "SELECT t1.id_movel, t1.ano, t1.operadora, t1.tecnologia, \
                t2.codigo_setor_censitario, t2.bairro, t2.tipo_setor, t2.codigo_localidade, t2.nome_localidade, t2.categoria_localidade, t2.localidade_agregadora, \
                t3.codigo_municipio, t3.municipio, t3.uf, t3.regiao, t3.area, \
                t4.domicilios, t4.moradores, t4.percentual_Cobertura \
                FROM OperadoraMovel t1 INNER JOIN TipoSetorMovel t2 \
                ON t2.id_movel = t1.id_movel \
                INNER JOIN MunicipioMovel t3 ON t3.id_movel = t2.id_movel \
                INNER JOIN MoradoresMovel t4 ON t4.id_movel = t3.id_movel \
                ORDER BY id_movel;"
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
            INSERT INTO cobertura_movel (
                id_movel,
                ano,
                operadora,
                tecnologia,
                codigo_setor_censitario,
                bairro,
                tipo_setor,
                codigo_localidade,
                nome_localidade,
                categoria_localidade,
                localidade_agregadora,
                codigo_municipio,
                municipio,
                uf,
                regiao,
                area,
                domicilios,
                moradores,
                percentual_cobertura
            ) values (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
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
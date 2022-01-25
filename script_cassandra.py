from connector_cassandra import Interface_db_cassandra, get_db_info

# SCRIPT CASSANDRA

# CREATE KEYSPACE IF NOT EXISTS telecom WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};
# USE TELECOM;

if __name__ == "__main__":
    try:  
        keyspace = get_db_info()
        session = Interface_db_cassandra(keyspace)

# -----------------------------------------------------------------------------------------------------
# CRIACAO DA TABELA BANDA_LARGA NO CASSANDRA
# -----------------------------------------------------------------------------------------------------

        table_banda_larga = "CREATE TABLE IF NOT EXISTS banda_larga (\
                            id_banda int primary key,\
                            ano bigint,\
                            mes bigint,\
                            grupo_economico text,\
                            empresa text,\
                            cnpj bigint,\
                            porte_da_prestadora text,\
                            uf text,\
                            municipio text,\
                            codigo_ibge_municipio bigint,\
                            faixa_de_velocidade text,\
                            tecnologia text,\
                            meio_de_acesso text,\
                            tipo_de_pessoa text,\
                            acessos bigint);"
        session.execute(table_banda_larga)
        print(">> Created table banda_larga")

# -----------------------------------------------------------------------------------------------------
# CRIACAO DA TABELA COBERTURA_MOVEL NO CASSANDRA
# -----------------------------------------------------------------------------------------------------
        
        table_cobertura_movel = "CREATE TABLE IF NOT EXISTS cobertura_movel (\
                                id_movel int primary key,\
                                ano int,\
                                operadora text,\
                                tecnologia text,\
                                codigo_setor_censitario bigint,\
                                bairro text,\
                                tipo_setor text,\
                                codigo_localidade bigint,\
                                nome_localidade text,\
                                categoria_localidade text,\
                                localidade_agregadora text,\
                                codigo_municipio int,\
                                municipio text,\
                                uf text,\
                                regiao text,\
                                area float,\
                                domicilios int,\
                                moradores int,\
                                percentual_cobertura float);"
        session.execute(table_cobertura_movel)
        print(">> Created table cobertura_movel")   

# -----------------------------------------------------------------------------------------------------
# CRIACAO DA TABELA MUNICIPIOS_ACESSOS NO CASSANDRA
# -----------------------------------------------------------------------------------------------------
        
        table_municipios_acessos = "CREATE TABLE IF NOT EXISTS municipios_acessos (\
                                    id_acessos int primary key,\
                                    ano int,\
                                    mes int,\
                                    acesso int,\
                                    servico text,\
                                    densidade float,\
                                    codigo_ibge bigint,\
                                    municipio text,\
                                    uf text,\
                                    nome_uf text,\
                                    regiao text,\
                                    codigo_nacional bigint);"
        session.execute(table_municipios_acessos)
        print(">> Created table municipios_acessos")    
            
# -----------------------------------------------------------------------------------------------------
# CRIACAO DA TABELA MUNICIPIO_COBERTURA NO CASSANDRA
# -----------------------------------------------------------------------------------------------------
        
        table_municipio_cobertura = "CREATE TABLE IF NOT EXISTS municipios_cobertura (\
                                    id_cobertura int primary key,\
                                    operadora text,\
                                    tecnologia_cobertura text,\
                                    moradores_cobertos float,\
                                    domicilios_cobertos float,\
                                    area_coberta float,\
                                    moradores_municipio int,\
                                    domicilios_municipio int,\
                                    area_municipio float,\
                                    ano int,\
                                    codigo_ibge int,\
                                    municipio text,\
                                    uf text,\
                                    nome_uf text,\
                                    regiao text,\
                                    codigo_nacional int);"
        session.execute(table_municipio_cobertura)
        print(">> Created table municipios_cobertura")  

# -----------------------------------------------------------------------------------------------------
# CRIACAO DA TABELA TV_ASSINATURA NO CASSANDRA
# -----------------------------------------------------------------------------------------------------
        
        table_tv_assinatura = "CREATE TABLE IF NOT EXISTS tv_assinatura (\
                                id_tv_assinatura int primary key,\
                                ano int,\
                                mes int,\
                                grupo_economico text,\
                                empresa text,\
                                cnpj bigint,\
                                porte_prestadora text,\
                                uf text,\
                                municipio text,\
                                codigo_ibge_municipio int,\
                                tecnologia text,\
                                meio_acesso text,\
                                tipo_pessoa text,\
                                acessos int);"
        session.execute(table_tv_assinatura)
        print(">> Created table tv_assinatura") 

# -----------------------------------------------------------------------------------------------------
# CRIACAO DA TABELA VELOCIDADE_CONTRATADA NO CASSANDRA
# -----------------------------------------------------------------------------------------------------
        
        table_velocidade_contratada = "CREATE TABLE IF NOT EXISTS velocidade_contratada (\
                                        id_velocidade_contratada int primary key,\
                                        ano int,\
                                        mes int,\
                                        razao_social text,\
                                        cnpj text,\
                                        velocidade_contratada_mbps float,\
                                        uf text,\
                                        municipio text,\
                                        codigo_ibge int,\
                                        acessos int,\
                                        tipo text,\
                                        municipio_uf text);"
        session.execute(table_velocidade_contratada)
        print(">> Created table velocidade_contratada") 
        
        print(">> Finished script!")
        
    except Exception as e:
         print("Error in cassandra script: ", str(e))
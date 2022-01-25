from connector_postgre import Interface_db_postgre, get_db_info

if __name__ == "__main__":
    try:  
        # CONEXAO COM O POSTGRE
        user, password, host, database = get_db_info()
        db = Interface_db_postgre(user, password, host, database)

# -----------------------------------------------------------------------------------------------------
# CRIACAO DAS TABELAS BANDA_LARGA E LOG_BANDA_LARGA NO POSTGRE
# -----------------------------------------------------------------------------------------------------
            
        table_banda_larga = "CREATE TABLE IF NOT EXISTS banda_larga(\
                            id_banda serial primary key,\
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
                            acessos bigint)"
        db.execute(table_banda_larga)
        print(">> Created table banda_larga")

        table_log_banda_larga = "CREATE TABLE IF NOT EXISTS log_banda_larga(\
                                id_log_banda serial primary key,\
                                usuario text,\
                                data_registro date,\
                                dados text)"
        db.execute(table_log_banda_larga)
        print(">> Created table log_banda_larga")

# -----------------------------------------------------------------------------------------------------
# FUNCTIONS E TRIGGERS DA TABELA BANDA_LARGA PARA LOG
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger para registrar os logs da tabela banda_larga. 
# Apos inserir, atualizar ou deletar, criará um log na tabela log_banda_larga,
# contendo usuario, data da execucao e quais dados foram atualizados.

        function_log_banda_larga = "CREATE OR REPLACE FUNCTION func_log_banda_larga() RETURNS TRIGGER AS $$ \
            BEGIN \
                IF (TG_OP = 'INSERT') THEN \
                INSERT INTO log_banda_larga(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Inclusão realizada. ' || NEW.* || ' .' ); \
                RETURN NEW; \
                ELSIF (TG_OP = 'UPDATE') THEN \
                INSERT INTO log_banda_larga(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Alteração realizada. Dados antigos: ' || OLD.* || ' para novos dados ' || NEW.* || ' .' ); \
                RETURN NEW; \
                ELSIF (TG_OP = 'DELETE') THEN \
                INSERT INTO log_banda_larga(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Deleção realizada. Dados deletados: ' || OLD.* || ' .' ); \
                RETURN OLD; \
                END IF; \
                RETURN NULLs; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_log_banda_larga)
        print(">> Function func_log_banda_larga created")

        trigger_log_banda_larga = "CREATE TRIGGER tr_log_banda_larga AFTER INSERT or UPDATE or DELETE ON banda_larga \
        FOR EACH ROW EXECUTE PROCEDURE func_log_banda_larga();"
        db.execute(trigger_log_banda_larga)
        print(">> Trigger tr_log_banda_larga created")

# -----------------------------------------------------------------------------------------------------
# CRIACAO DAS TABELAS COBERTURA_MOVEL E LOG_COBERTURA_MOVEL NO POSTGRE
# -----------------------------------------------------------------------------------------------------

        table_cobertura_movel = "CREATE TABLE IF NOT EXISTS cobertura_movel (\
                                id_movel serial primary key,\
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
                                area decimal(38,4),\
                                domicilios int,\
                                moradores int,\
                                percentual_cobertura decimal(38,4));"
        db.execute(table_cobertura_movel)
        print(">> Created table cobertura_movel")

        table_log_cobertura_movel = "CREATE TABLE IF NOT EXISTS log_cobertura_movel(\
                                    id_log_cobertura serial primary key,\
                                    usuario text,\
                                    data_registro date,\
                                    dados text);"
        db.execute(table_log_cobertura_movel)
        print(">> Created table log_cobertura_movel")

# -----------------------------------------------------------------------------------------------------
# FUNCTIONS E TRIGGERS DA TABELA COBERTURA_MOVEL PARA LOG
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger para registrar os logs da tabela cobertura_movel. 
# Apos inserir, atualizar ou deletar, criará um log na tabela log_cobertura_movel,
# contendo usuario, data da execucao e quais dados foram atualizados.

        function_log_cobertura_movel = "CREATE OR REPLACE FUNCTION func_log_cobertura_movel() RETURNS TRIGGER AS $$ \
            BEGIN \
                IF (TG_OP = 'INSERT') THEN \
                INSERT INTO log_cobertura_movel(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Inclusão realizada. ' || NEW.* || ' .' ); \
                RETURN NEW; \
                ELSIF (TG_OP = 'UPDATE') THEN \
                INSERT INTO log_cobertura_movel(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Alteração realizada. Operação antiga: ' || OLD.* || ' para nova operação ' || NEW.* || ' .' ); \
                RETURN NEW; \
                ELSIF (TG_OP = 'DELETE') THEN \
                INSERT INTO log_cobertura_movel(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Deleção realizada. Operação deletada: ' || OLD.* || ' .' ); \
                RETURN OLD; \
                END IF; \
                RETURN NULLs; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_log_cobertura_movel)
        print(">> Function func_log_cobertura_movel created")

        trigger_log_cobertura_movel = "CREATE TRIGGER tr_log_cobertura_movel AFTER INSERT or UPDATE or DELETE ON cobertura_movel \
        FOR EACH ROW EXECUTE PROCEDURE func_log_cobertura_movel();"
        db.execute(trigger_log_cobertura_movel)
        print(">> Trigger tr_log_cobertura_movel created")

# -----------------------------------------------------------------------------------------------------
# CRIACAO DAS TABELAS MUNICIPIOS_ACESSOS E LOG_MUNICIPIOS_ACESSOS NO POSTGRE
# -----------------------------------------------------------------------------------------------------

        table_municipios_acessos = "CREATE TABLE IF NOT EXISTS municipios_acessos(\
                                    id_acessos serial primary key,\
                                    ano int,\
                                    mes int,\
                                    acessos int,\
                                    servico text,\
                                    densidade decimal(38,4),\
                                    codigo_ibge int,\
                                    municipio text,\
                                    uf text,\
                                    nome_uf text,\
                                    regiao text,\
                                    codigo_nacional int);" 
        db.execute(table_municipios_acessos)
        print(">> Created table municipios_acessos")

        table_log_municipios_acessos = "CREATE TABLE IF NOT EXISTS log_municipios_acessos(\
                                        id_log_acessos serial primary key,\
                                        usuario text,\
                                        data_registro date,\
                                        dados text);"
        db.execute(table_log_municipios_acessos)
        print(">> Created table log_municipios_acessos")

# -----------------------------------------------------------------------------------------------------
# FUNCTIONS E TRIGGERS DA TABELA MUNICIPIOS_ACESSOS PARA LOG
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger para registrar os logs da tabela municipios_acessos. 
# Apos inserir, atualizar ou deletar, criará um log na tabela log_municipios_acessos,
# Contendo usuario, data da execucao e quais dados foram atualizados.

        function_log_municipios_acessos = "CREATE OR REPLACE FUNCTION func_log_municipios_acessos() RETURNS TRIGGER AS $$ \
            BEGIN \
                IF (TG_OP = 'INSERT') THEN \
                INSERT INTO log_municipios_acessos(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Inclusão realizada. ' || NEW.* || ' .' ); \
                RETURN NEW; \
                ELSIF (TG_OP = 'UPDATE') THEN \
                INSERT INTO log_municipios_acessos(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Alteração realizada. Operação antiga: ' || OLD.* || ' para nova operação ' || NEW.* || ' .' ); \
                RETURN NEW; \
                ELSIF (TG_OP = 'DELETE') THEN \
                INSERT INTO log_municipios_acessos(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Deleção realizada. Operação deletada: ' || OLD.* || ' .' ); \
                RETURN OLD; \
                END IF; \
                RETURN NULLs; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_log_municipios_acessos)
        print(">> Function func_log_municipios_acessos created")

        trigger_log_municipios_acessos = "CREATE TRIGGER tr_log_municipios_acessos AFTER INSERT or UPDATE or DELETE ON municipios_acessos \
        FOR EACH ROW EXECUTE PROCEDURE func_log_municipios_acessos();"
        db.execute(trigger_log_municipios_acessos)
        print(">> Trigger tr_log_municipios_acessos created")

# -----------------------------------------------------------------------------------------------------
# CRIACAO DAS TABELAS MUNICIPIOS_COBERTURA E LOG_MUNICIPIOS_COBERTURA NO POSTGRE
# -----------------------------------------------------------------------------------------------------

        table_municipios_cobertura = "CREATE TABLE IF NOT EXISTS municipios_cobertura (\
                                    id_cobertura serial primary key,\
                                    operadora text,\
                                    tecnologia_cobertura text,\
                                    moradores_cobertos decimal (38,4),\
                                    domicilios_cobertos decimal (38,4),\
                                    area_coberta decimal (38,4),\
                                    moradores_municipio int,\
                                    domicilios_municipio int,\
                                    area_municipio decimal (38,4),\
                                    ano int,\
                                    codigo_ibge int,\
                                    municipio text,\
                                    uf text,\
                                    nome_uf text,\
                                    regiao text,\
                                    codigo_nacional int);"
        db.execute(table_municipios_cobertura)
        print(">> Created table municipios_cobertura")

        table_log_municipios_cobertura = "CREATE TABLE IF NOT EXISTS log_municipios_cobertura(\
                                        id_log_acessos serial primary key,\
                                        usuario text,\
                                        data_registro date,\
                                        dados text);"
        db.execute(table_log_municipios_cobertura)
        print(">> Created table log_municipios_cobertura")

# -----------------------------------------------------------------------------------------------------
# FUNCTIONS E TRIGGERS DA TABELA MUNICIPIOS_COBERTURA PARA LOG
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger para registrar os logs da tabela municipios_cobertura. 
# Apos inserir, atualizar ou deletar, criará um log na tabela log_municipios_cobertura,
# Contendo usuario, data da execucao e quais dados foram atualizados.

        function_log_municipios_cobertura = "CREATE OR REPLACE FUNCTION func_log_municipios_cobertura() RETURNS TRIGGER AS $$ \
            BEGIN \
                IF (TG_OP = 'INSERT') THEN \
                INSERT INTO log_municipios_cobertura(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Inclusão realizada. ' || NEW.* || ' .' ); \
                RETURN NEW; \
                ELSIF (TG_OP = 'UPDATE') THEN \
                INSERT INTO log_municipios_cobertura(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Alteração realizada. Operação antiga: ' || OLD.* || ' para nova operação ' || NEW.* || ' .' ); \
                RETURN NEW; \
                ELSIF (TG_OP = 'DELETE') THEN \
                INSERT INTO log_municipios_cobertura(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Deleção realizada. Operação deletada: ' || OLD.* || ' .' ); \
                RETURN OLD; \
                END IF; \
                RETURN NULLs; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_log_municipios_cobertura)
        print(">> Function func_log_municipios_cobertura created")

        trigger_log_municipios_cobertura = "CREATE TRIGGER tr_log_municipios_cobertura AFTER INSERT or UPDATE or DELETE ON municipios_cobertura \
        FOR EACH ROW EXECUTE PROCEDURE func_log_municipios_cobertura();"
        db.execute(trigger_log_municipios_cobertura)
        print(">> Trigger tr_log_municipios_cobertura created")

# -----------------------------------------------------------------------------------------------------
# CRIACAO DAS TABELAS TV_ASSINATURA E LOG_TV_ASSINATURA NO POSTGRE
# -----------------------------------------------------------------------------------------------------

        table_tv_assinatura = "CREATE TABLE IF NOT EXISTS tv_assinatura(\
                                id_tv_assinatura serial primary key,\
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
        db.execute(table_tv_assinatura)
        print(">> Created table tv_assinatura")

        table_log_tv_assinatura = "CREATE TABLE IF NOT EXISTS log_tv_assinatura(\
                                    id_log_acessos serial primary key,\
                                    usuario text,\
                                    data_registro date,\
                                    dados text);"
        db.execute(table_log_tv_assinatura)
        print(">> Created table log_tv_assinatura")

# -----------------------------------------------------------------------------------------------------
# FUNCTIONS E TRIGGERS DA TABELA TV_ASSINATURA PARA LOG
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger para registrar os logs da tabela tv_assinatura. 
# Apos inserir, atualizar ou deletar, criará um log na tabela log_tv_assinatura,
# contendo usuario, data da execucao e quais dados foram atualizados.

        function_log_tv_assinatura = "CREATE OR REPLACE FUNCTION func_log_tv_assinatura() RETURNS TRIGGER AS $$ \
            BEGIN \
                IF (TG_OP = 'INSERT') THEN \
                INSERT INTO log_tv_assinatura(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Inclusão realizada. ' || NEW.* || ' .' ); \
                RETURN NEW; \
                ELSIF (TG_OP = 'UPDATE') THEN \
                INSERT INTO log_tv_assinatura(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Alteração realizada. Operação antiga: ' || OLD.* || ' para nova operação ' || NEW.* || ' .' ); \
                RETURN NEW; \
                ELSIF (TG_OP = 'DELETE') THEN \
                INSERT INTO log_tv_assinatura(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Deleção realizada. Operação deletada: ' || OLD.* || ' .' ); \
                RETURN OLD; \
                END IF; \
                RETURN NULLs; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_log_tv_assinatura)
        print(">> Function func_log_tv_assinatura created")

        trigger_log_tv_assinatura = "CREATE TRIGGER tr_log_tv_assinatura AFTER INSERT or UPDATE or DELETE ON tv_assinatura \
        FOR EACH ROW EXECUTE PROCEDURE func_log_tv_assinatura();"
        db.execute(trigger_log_tv_assinatura)
        print(">> Trigger tr_log_tv_assinatura created")

# -----------------------------------------------------------------------------------------------------
# CRIACAO DAS TABELAS VELOCIDADE_CONTRATADA E LOG_VELOCIDADE_CONTRATADA NO POSTGRE
# -----------------------------------------------------------------------------------------------------

        table_velocidade_contratada = "CREATE TABLE IF NOT EXISTS velocidade_contratada (\
                                        id_velocidade_contratada serial primary key,\
                                        ano int,\
                                        mes int,\
                                        razao_social text,\
                                        cnpj text,\
                                        velocidade_contratada_mbps decimal(38,4),\
                                        uf text,\
                                        municipio text,\
                                        codigo_ibge	int,\
                                        acessos	int,\
                                        tipo text,\
                                        municipio_uf text);"
        db.execute(table_velocidade_contratada)
        print(">> Created table velocidade_contratada")

        table_log_velocidade_contratada = "CREATE TABLE IF NOT EXISTS log_velocidade_contratada(\
                                            id_log_acessos serial primary key,\
                                            usuario text,\
                                            data_registro date,\
                                            dados text);"
        db.execute(table_log_velocidade_contratada)
        print(">> Created table log_velocidade_contratada")        
            
# -----------------------------------------------------------------------------------------------------
# FUNCTIONS E TRIGGERS DA TABELA VELOCIDADE_CONTRATADA PARA LOG
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger para registrar os logs da tabela velocidade_contratada. 
# Apos inserir, atualizar ou deletar, criará um log na tabela log_velocidade_contratada,
# contendo usuario, data da execucao e quais dados foram atualizados.

        function_log_velocidade_contratada = "CREATE OR REPLACE FUNCTION func_log_velocidade_contratada() RETURNS TRIGGER AS $$ \
            BEGIN \
                IF (TG_OP = 'INSERT') THEN \
                INSERT INTO log_Velocidade_contratada(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Inclusão realizada. ' || NEW.* || ' .' ); \
                RETURN NEW; \
                ELSIF (TG_OP = 'UPDATE') THEN \
                INSERT INTO log_Velocidade_contratada(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Alteração realizada. Operação antiga: ' || OLD.* || ' para nova operação ' || NEW.* || ' .' ); \
                RETURN NEW; \
                ELSIF (TG_OP = 'DELETE') THEN \
                INSERT INTO log_Velocidade_contratada(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Deleção realizada. Operação deletada: ' || OLD.* || ' .' ); \
                RETURN OLD; \
                END IF; \
                RETURN NULLs; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_log_velocidade_contratada)
        print(">> Function func_log_velocidade_contratada created")

        trigger_log_velocidade_contratada = "CREATE TRIGGER tr_log_velocidade_contratada AFTER INSERT or UPDATE or DELETE ON velocidade_contratada \
        FOR EACH ROW EXECUTE PROCEDURE func_log_velocidade_contratada();"
        db.execute(trigger_log_velocidade_contratada)
        print(">> Trigger tr_log_velocidade_contratada created")

        print(">> Finished script")
        
    except Exception as e:
        print("Error in script tables postgre: ", str(e))
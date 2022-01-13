-- ano 
-- mes 
-- Acesso 
-- Servico 
-- Densidade 
-- codigo_ibge 
-- municipio 
-- uf 
-- nome_uf 
-- regiao
-- codigo_nacional


-- TABELAS


NOME TABELAS = log_municipio_acessos, municipio_acesso
NOME DAS VIEWS = Servicos, LocaisAcesso 


CREATE TABLE IF NOT EXISTS log_municipio_acessos(
    id_log_acessos serial primary key,
    usuario text,
    data_registro date,
    dados text
);


create table if not exists municipio_acesso (
    id_acessos serial primary key,
    ano int,
    mes int,
    Acesso text,
    Servico text,
    Densidade text,
    codigo_ibge text,
    municipio text,
    uf text,
    nome_uf text,
    regiao text,
    codigo_nacional int
); 


-- FUNCTIONS E TRIGGERS


CREATE OR REPLACE FUNCTION FUNCTION_LOG_MUNICIPIO_ACESSO() RETURNS TRIGGER AS $$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
        INSERT INTO log_municipio_acessos(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Inclusão realizada. ' || NEW.* || ' .' );
        RETURN NEW;
        ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_municipio_acessos(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Alteração realizada. Operação antiga: ' || OLD.* || ' para nova operação ' || NEW.* || ' .' );
        RETURN NEW;
        ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO log_municipio_acessos(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Deleção realizada. Operação deletada: ' || OLD.* || ' .' );
        RETURN OLD;
        END IF;
        RETURN NULLs;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Tr_log_municipio_acesso AFTER INSERT or UPDATE or DELETE ON municipio_acesso
FOR EACH ROW EXECUTE PROCEDURE FUNCTION_LOG_MUNICIPIO_ACESSO();



CREATE OR REPLACE FUNCTION municipioacesso_function_normalizacao1() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view Servicos as 
            select id_acessos, ano, mes, acesso, Servico
            from municipio_acesso  
            order by Servico;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Municipio_acesso_1 AFTER INSERT ON municipio_acesso
FOR EACH ROW EXECUTE PROCEDURE municipioacesso_function_normalizacao1();




CREATE OR REPLACE FUNCTION municipioacesso_function_normalizacao2() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view LocaisAcesso as 
            select id_acessos, Densidade, codigo_ibge, municipio, uf, nome_uf, regiao, codigo_nacional
            from municipio_acesso  
            order by municipio;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Municipio_acesso_2 AFTER INSERT ON municipio_acesso
FOR EACH ROW EXECUTE PROCEDURE municipioacesso_function_normalizacao2();



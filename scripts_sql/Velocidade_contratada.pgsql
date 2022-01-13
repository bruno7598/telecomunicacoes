-- Ano	
-- Mês	
-- razao_social	
-- cnpj	
-- velocidade_contratada_mbps	
-- uf	
-- municipio	
-- codigo_ibge	
-- acessos	
-- tipo	
-- municipio_uf




CREATE TABLE IF NOT EXISTS log_Velocidade_contratada(
    id_log_acessos serial primary key,
    usuario text,
    data_registro date,
    dados text
);


create table if not exists Velocidade_contratada (
    id_Velocidade_contratada serial primary key,
    ano int,
    mes int,
    razao_social text,	
    cnpj text,	
    velocidade_contratada_mbps text,
    uf text,
    municipio text,	
    codigo_ibge	text,
    acessos	text,
    tipo text,	
    municipio_uf text
);


CREATE OR REPLACE FUNCTION FUNCTION_LOG_VELOCIDADE_CONTRATADA() RETURNS TRIGGER AS $$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
        INSERT INTO log_Velocidade_contratada(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Inclusão realizada. ' || NEW.* || ' .' );
        RETURN NEW;
        ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_Velocidade_contratada(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Alteração realizada. Operação antiga: ' || OLD.* || ' para nova operação ' || NEW.* || ' .' );
        RETURN NEW;
        ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO log_Velocidade_contratada(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Deleção realizada. Operação deletada: ' || OLD.* || ' .' );
        RETURN OLD;
        END IF;
        RETURN NULLs;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Tr_log_Velocidade_contratada AFTER INSERT or UPDATE or DELETE ON Velocidade_contratada
FOR EACH ROW EXECUTE PROCEDURE FUNCTION_LOG_VELOCIDADE_CONTRATADA();




CREATE OR REPLACE FUNCTION VelocidadeContratada_function_normalizacao1() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view EmpresaVelocidade as 
            select id_Velocidade_contratada, ano, mes, razao_social, cnpj, velocidade_contratada_mbps
            from Velocidade_contratada
            order by razao_social;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Velocidade_contratada_1 AFTER INSERT ON Velocidade_contratada
FOR EACH ROW EXECUTE PROCEDURE LogVelocidadeContratada_function_normalizacao1();



CREATE OR REPLACE FUNCTION VelocidadeContratada_function_normalizacao2() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view LocalidadeVelocidade as 
            select id_Velocidade_contratada, uf, municipio, codigo_ibge, acessos, tipo, municipio_uf
            from Velocidade_contratada
            order by municipio;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Velocidade_contratada_2 AFTER INSERT ON Velocidade_contratada
FOR EACH ROW EXECUTE PROCEDURE LogVelocidadeContratada_function_normalizacao2();




-- Operadora	
-- Tecnologia Cobertura	
-- Moradores Cobertos	
-- Domicílios Cobertos	
-- Área km2 Coberta	
-- Moradores Município	
-- Domicílios Município	
-- Área Município km2	
-- Ano	
-- Código IBGE	
-- Município	
-- UF	
-- Nome UF	
-- Região	
-- Código Nacional


-- TABELAS

NOME TABELAS = log_municipio_cobertura, municipio_cobertura
NOME DAS VIEWS = Operadoras, AreaCobertura, AreaMunipios, LocaisCobertura 


CREATE TABLE IF NOT EXISTS log_municipio_cobertura(
    id_log_acessos serial primary key,
    usuario text,
    data_registro date,
    dados text
);



create table if not exists municipio_cobertura (
    id_cobertura serial primary key,
    operadora text,
    tecnologia_cobertura text,
    Moradores_cobertos text,
    Domicilios_cobertos text,
    Area_coberta text,
    Moradores_municipio text,
    domicilios_Municipio text,
    Area_municipio text,
    Ano text,
    codigo_ibge text,
    municipio text,
    uf text,
    nome_uf text,
    regiao text,
    codigo_nacional text
); 


-- FUNCTIONS E TRIGGERS



CREATE OR REPLACE FUNCTION FUNCTION_LOG_MUNICIPIO_COBERTURA() RETURNS TRIGGER AS $$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
        INSERT INTO log_municipio_cobertura(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Inclusão realizada. ' || NEW.* || ' .' );
        RETURN NEW;
        ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_municipio_cobertura(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Alteração realizada. Operação antiga: ' || OLD.* || ' para nova operação ' || NEW.* || ' .' );
        RETURN NEW;
        ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO log_municipio_cobertura(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Deleção realizada. Operação deletada: ' || OLD.* || ' .' );
        RETURN OLD;
        END IF;
        RETURN NULLs;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Tr_log_municipio_cobertura AFTER INSERT or UPDATE or DELETE ON municipio_cobertura
FOR EACH ROW EXECUTE PROCEDURE FUNCTION_LOG_MUNICIPIO_COBERTURA();


    

CREATE OR REPLACE FUNCTION municipioCobertura_function_normalizacao1() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view Operadoras as 
            select id_cobertura, Ano, operadora, tecnologia_cobertura
            from municipio_cobertura  
            order by operadora;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Municipio_Cobertura_1 AFTER INSERT ON municipio_cobertura
FOR EACH ROW EXECUTE PROCEDURE municipioCobertura_function_normalizacao1();




CREATE OR REPLACE FUNCTION municipioCobertura_function_normalizacao2() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view AreaCobertura as 
            select id_cobertura, Moradores_cobertos, Domicilios_cobertos, Area_coberta 
            from municipio_cobertura  
            order by id_cobertura;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Municipio_Cobertura_2 AFTER INSERT ON municipio_cobertura
FOR EACH ROW EXECUTE PROCEDURE municipioCobertura_function_normalizacao2();



CREATE OR REPLACE FUNCTION municipioCobertura_function_normalizacao3() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view AreaMunipios as 
            select id_cobertura, Moradores_municipio, domicilios_Municipio, Area_municipio
            from municipio_cobertura  
            order by id_cobertura;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Municipio_Cobertura_3 AFTER INSERT ON municipio_cobertura
FOR EACH ROW EXECUTE PROCEDURE municipioCobertura_function_normalizacao3();



CREATE OR REPLACE FUNCTION municipioCobertura_function_normalizacao4() RETURNS TRIGGER as $$
BEGIN
    IF (tg_op = 'INSERT') THEN
        CREATE or REPLACE view LocaisCobertura as 
        select id_cobertura, codigo_ibge, municipio, uf, nome_uf, regiao, codigo_nacional
        from municipio_cobertura  
        order by municipio;   
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Municipio_Cobertura_4 AFTER INSERT ON municipio_cobertura
FOR EACH ROW EXECUTE PROCEDURE municipioCobertura_function_normalizacao4();


select t1.*, t2.*, t3.*, t4.* from Operadoras t1 inner join AreaCobertura t2 
on t2.id_cobertura = t1.id_cobertura 
inner join Area_municipio t3 on t3.id_cobertura = t2.id_cobertura
inner join LocaisCobertura t4 on t4.id_cobertura = t3.id_cobertura order by t1.id_cobertura;
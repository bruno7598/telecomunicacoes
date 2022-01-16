-- Ano	
-- Operadora	
-- Tecnologia	
-- Código Setor Censitário	
-- Bairro	
-- Tipo Setor	
-- Código Localidade	
-- Nome Localidade	
-- Categoria Localidade	
-- Localidade Agregadora	
-- Código Município	
-- Município	
-- UF	
-- Região	
-- Área (km2)	
-- Domicílios	
-- Moradores	
-- Percentual 
-- Cobertura


-- TABELAS

NOME TABELAS = log_cobertura_movel, cobertura_movel
NOME DAS VIEWS = OperadoraMovel, TipoSetorMovel, MunicipioMovel, MoradoresMovel


CREATE TABLE IF NOT EXISTS log_cobertura_movel(
    id_log_acessos serial primary key,
    usuario text,
    data_registro date,
    dados text
);


create table if not exists cobertura_movel (
    id_movel serial primary key,
    Ano int,
    Operadora text,
    Tecnologia text,
    codigo_setor_censitario text,
    Bairro text,
    tipo_setor text,
    codigo_localidade text,
    nome_localidade text,
    categoria_localidade text,
    localidade_agregadora text,
    codigo_municipio text,
    municipio text,
    uf text,
    regiao text,
    area text,
    domicilios text,
    Moradores text,
    Percentual_cobertura text
); 



-- FUNCTIONS E TRIGGERS

CREATE OR REPLACE FUNCTION FUNCTION_LOG_cobertura_movel() RETURNS TRIGGER AS $$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
        INSERT INTO log_cobertura_movel(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Inclusão realizada. ' || NEW.* || ' .' );
        RETURN NEW;
        ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_cobertura_movel(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Alteração realizada. Operação antiga: ' || OLD.* || ' para nova operação ' || NEW.* || ' .' );
        RETURN NEW;
        ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO log_cobertura_movel(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Deleção realizada. Operação deletada: ' || OLD.* || ' .' );
        RETURN OLD;
        END IF;
        RETURN NULLs;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Tr_log_cobertura_movel AFTER INSERT or UPDATE or DELETE ON cobertura_movel
FOR EACH ROW EXECUTE PROCEDURE FUNCTION_LOG_cobertura_movel();



CREATE OR REPLACE FUNCTION coberturaMovel_function_normalizacao1() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view OperadoraMovel as 
            select id_movel, Ano, operadora, Tecnologia
            from cobertura_movel  
            order by operadora;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Cobertura_movel_1 AFTER INSERT ON cobertura_movel
FOR EACH ROW EXECUTE PROCEDURE coberturaMovel_function_normalizacao1();



CREATE OR REPLACE FUNCTION coberturaMovel_function_normalizacao2() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view TipoSetorMovel as 
            select id_movel, codigo_setor_censitario, bairro, tipo_setor, codigo_localidade,
            nome_localidade, categoria_localidade, localidade_agregadora
            from cobertura_movel  
            order by tipo_setor;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Cobertura_movel_2 AFTER INSERT ON cobertura_movel
FOR EACH ROW EXECUTE PROCEDURE coberturaMovel_function_normalizacao2();


CREATE OR REPLACE FUNCTION coberturaMovel_function_normalizacao3() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view MunicipioMovel as 
            select id_movel, codigo_municipio, municipio, uf, regiao, area
            from cobertura_movel  
            order by municipio;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Cobertura_movel_3 AFTER INSERT ON cobertura_movel
FOR EACH ROW EXECUTE PROCEDURE coberturaMovel_function_normalizacao3();




CREATE OR REPLACE FUNCTION coberturaMovel_function_normalizacao4() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view MoradoresMovel as 
            select id_movel, Moradores, domicilios, Percentual_Cobertura
            from cobertura_movel  
            order by id_movel;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Cobertura_movel_4 AFTER INSERT ON cobertura_movel
FOR EACH ROW EXECUTE PROCEDURE coberturaMovel_function_normalizacao4();




select t1.*, 
t2.codigo_setor_censitario, t2.bairro, t2.tipo_setor, t2.codigo_localidade,
t2.nome_localidade, t2.categoria_localidade, t2.localidade_agregadora, 
t3.codigo_municipio, t3.municipio, t3.uf, t3.regiao, t3.area, 
t4.Moradores, t4.domicilios, t4.Percentual_Cobertura 
from OperadoraMovel t1 inner join TipoSetorMovel t2 
on t2.id_movel = t1.id_movel 
inner join MunicipioMovel t3 on t3.id_movel = t2.id_movel
inner join MoradoresMovel t4 on t4.id_movel = t3.id_movel order by id_movel;

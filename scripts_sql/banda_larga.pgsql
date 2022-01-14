-- Ano
-- Mês     
-- Grupo Econômico
-- Empresa
-- CNPJ
-- Porte da Prestadora
-- UF           
-- Município
-- Código IBGE Município
-- Faixa de Velocidade
-- Tecnologia|
-- Meio de Acesso
-- Tipo de Pessoa
-- Acessos


-- TABELAS

NOME TABELAS = log_banda_larga, banda_larga
NOME DAS VIEWS = Empresa, tecnologia, acesso, Localidade 


CREATE TABLE IF NOT EXISTS log_banda_larga(
    id_log_banda serial primary key,
    usuario text,
    data_registro date,
    dados text
);

create table if not exists banda_larga (
    id_banda serial primary key,
    ano bigint,
    mes bigint,
    grupo_economico text,
    empresa text,
    cnpj bigint,
    porte text,
    uf text,
    municipio text,
    codigo_ibge_municipio bigint,
    faixa_velocidade text,
    tecnologia text,
    meio_acesso text,
    tipo_pessoa text,
    acesso bigint
); 



-- FUNCTIONS E TRIGGERS

CREATE OR REPLACE FUNCTION FUNCTION_LOG_BANDA_LARGA() RETURNS TRIGGER AS $$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
        INSERT INTO log_banda_larga(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Inclusão realizada. ' || NEW.* || ' .' );
        RETURN NEW;
        ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_banda_larga(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Alteração realizada. Operação antiga: ' || OLD.* || ' para nova operação ' || NEW.* || ' .' );
        RETURN NEW;
        ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO log_banda_larga(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Deleção realizada. Operação deletada: ' || OLD.* || ' .' );
        RETURN OLD;
        END IF;
        RETURN NULLs;
    END;
$$
LANGUAGE 'plpgsql';


CREATE TRIGGER Tr_log_banda_larga AFTER INSERT or UPDATE or DELETE ON banda_larga
FOR EACH ROW EXECUTE PROCEDURE FUNCTION_LOG_BANDA_LARGA();


CREATE OR REPLACE FUNCTION bandalarga_function_normalizacao1() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view Empresa as 
            select id_banda,ano,mes, grupo_economico, empresa, cnpj, porte
            from banda_larga  
            order by empresa;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Banda_larga_1 AFTER INSERT ON banda_larga
FOR EACH ROW EXECUTE PROCEDURE bandalarga_function_normalizacao1();




CREATE OR REPLACE FUNCTION bandalarga_function_normalizacao2() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
			CREATE or REPLACE view Tecnologia as 
            select id_banda, tecnologia, faixa_velocidade,  meio_acesso
            from banda_larga  
            order by tecnologia; 
		END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';


CREATE TRIGGER Banda_larga_2 AFTER INSERT ON banda_larga
FOR EACH ROW EXECUTE PROCEDURE bandalarga_function_normalizacao2();



CREATE OR REPLACE FUNCTION bandalarga_function_normalizacao3() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
			CREATE or REPLACE view Localidade as 
            select id_banda, uf, municipio, codigo_ibge_municipio
            from banda_larga  
            order by municipio; 
		END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';


CREATE TRIGGER Banda_larga_3 AFTER INSERT ON banda_larga
FOR EACH ROW EXECUTE PROCEDURE bandalarga_function_normalizacao3();


CREATE OR REPLACE FUNCTION bandalarga_function_normalizacao4() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
			CREATE or REPLACE view Acessos as 
            select id_banda, tipo_pessoa, acesso
            from banda_larga  
            order by id_banda; 
		END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';


CREATE TRIGGER Banda_larga_4 AFTER INSERT ON banda_larga
FOR EACH ROW EXECUTE PROCEDURE bandalarga_function_normalizacao4();


select t1.*, t2.*, t3.*, t4.* from Empresa t1 inner join Localidade t2 
on t2.id_banda = t1.id_banda 
inner join tecnologia t3 on t3.id_banda = t2.id_banda
inner join acessos t4 on t4.id_banda = t3.id_banda;


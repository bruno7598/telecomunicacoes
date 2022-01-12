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

CREATE TABLE IF NOT EXISTS log_telecom(
    id_log serial primary key,
    empresa text,
    cnpj bigint,
    grupo_economico text,
    tipo_pessoa text
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

create table if not exists empresas (
	id_empresa serial,
	nome_empresa text,
	grupo_economico text,
	cnpj bigint
);


create table if not exists tecnologia (
	id_tecnologia serial,
	faixa_velocidade text,
	tecnologia_usada text,
	meio_acesso text
);

create table if not exists localidade (
	id_local serial,
	uf text,
	municipio text,
	ibge bigint
);


-- FUNCTIONS E TRIGGERS


CREATE OR REPLACE FUNCTION tgr_function_telecom() RETURNS TRIGGER as $$
    BEGIN
    -- Aqui temos um bloco IF que confirmará o tipo de operação de Inserção
        IF (tg_op = 'INSERT') THEN
            INSERT INTO log_telecom(empresa, cnpj, grupo_economico,tipo_pessoa)
            VALUES (NEW.empresa, NEW.cnpj, 'Operação de Inserção. A linha de Código' || NEW.grupo_economico || 'Foi inserido', NEW.tipo_pessoa);
            RETURN NEW;
    -- Aqui temos um bloco IF que confirmará o tipo de operação de Atualização
        ELSIF (tg_op = 'UPDATE') THEN
            INSERT INTO log_telecom(empresa, cnpj, grupo_economico,tipo_pessoa)
            VALUES (NEW.empresa, 'Operação de Atualização. 
            A linha de Código' || NEW.cnpj || 'Teve seus valores modificados' || OLD.* || ' com  ' || NEW.* || ' .', NEW.tipo_pessoa);
            RETURN NEW;
    -- A'qui temos um bloco IF que confirmará o tipo de operação de Exclusão
        ELSIF (tg_op = 'DELETE') THEN
            INSERT INTO log_telecom(empresa, cnpj, grupo_economico,tipo_pessoa)
            VALUES (OLD.*, 'Operação de Exclusão. Os seguintes dados foram excluidos: ' || OLD.* || ' .');
            RETURN OLD;
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';


CREATE TRIGGER Tr_log_telecommm AFTER INSERT or UPDATE or DELETE ON banda_larga
FOR EACH ROW EXECUTE PROCEDURE tgr_function_telecom();




CREATE OR REPLACE FUNCTION normalizacao1_function_telecom() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            INSERT INTO localidade(uf, municipio, ibge)
            VALUES (NEW.uf, NEW.municipio, NEW.codigo_ibge_municipio);
			RETURN NEW;
            
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER Tr_normalizacao_1 AFTER INSERT ON banda_larga
FOR EACH ROW EXECUTE PROCEDURE normalizacao1_function_telecom();




CREATE OR REPLACE FUNCTION normalizacao2_function_telecom() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
			INSERT INTO tecnologia(faixa_velocidade,tecnologia_usada,meio_acesso)
            VALUES (NEW.faixa_velocidade, NEW.tecnologia, NEW.acesso);
			RETURN NEW;
		END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';


CREATE TRIGGER Tr_normalizacao_2 AFTER INSERT ON banda_larga
FOR EACH ROW EXECUTE PROCEDURE normalizacao2_function_telecom();



CREATE OR REPLACE FUNCTION normalizacao3_function_telecom() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
			INSERT INTO empresas(nome_empresa, grupo_economico, cnpj)
            VALUES (NEW.empresa, NEW.grupo_economico, NEW.cnpj);
            RETURN NEW;
		END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';


CREATE TRIGGER Tr_normalizacao_3 AFTER INSERT ON banda_larga
FOR EACH ROW EXECUTE PROCEDURE normalizacao3_function_telecom();


-- VIEWS



CREATE or REPLACE view teste as 
    select empresa, tecnologia, faixa_velocidade from banda_larga where faixa_velocidade = '> 34Mbps' order by empresa;


CREATE or REPLACE view teste_1 as 
    select uf, municipio, tecnologia, faixa_velocidade from banda_larga where faixa_velocidade = '> 34Mbps' order by tecnologia;


CREATE or REPLACE view teste_2 as 
select tecnologia, count(tecnologia) from banda_larga group by tecnologia order by count(tecnologia) desc;


CREATE or REPLACE view teste_3 as 
select meio_acesso, count(meio_acesso) from banda_larga group by meio_acesso order by count(meio_acesso) desc;


CREATE or REPLACE view teste_4 as
select faixa_velocidade, count(faixa_velocidade) from banda_larga group by faixa_velocidade order by count(faixa_velocidade) desc;

CREATE or REPLACE view teste_5 as
select uf,count(empresa) from banda_larga group by uf order by count(empresa);

CREATE or REPLACE view teste_6 as
select grupo_economico, empresa, tipo_pessoa from banda_larga where tipo_pessoa in ('Pessoa Jurídica','Pessoa Física') order by empresa;

CREATE or REPLACE view teste_7 as
select tipo_pessoa, count(tipo_pessoa) from banda_larga group by tipo_pessoa order by count(tipo_pessoa) desc;

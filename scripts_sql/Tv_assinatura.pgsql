-- Ano	
-- Mês	
-- Grupo Econômico	
-- Empresa	
-- CNPJ	
-- Porte da Prestadora	
-- UF	
-- Município	
-- Código IBGE Município	
-- Tecnologia	
-- Meio de Acesso	
-- Tipo de Pessoa	
-- Acessos


NOME TABELAS = log_tv_assinatura, tv_assinatura
NOME DAS VIEWS = EmpresaTVassinatura, LocalidadeTvAssinatura, TipoPessoaTvAssinatura


CREATE TABLE IF NOT EXISTS log_tv_assinatura(
    id_log_acessos serial primary key,
    usuario text,
    data_registro date,
    dados text
);


create table if not exists tv_assinatura (
    id_cobertura serial primary key,
    ano int,
    mes int,
    grupo_economico text,
    Empresa text,
    CNPJ bigint,
    porte_prestadora text,
    uf text,
    municipio text,
    codigo_ibge_municipio text,
    Tecnologia text,
    Meio_acesso text,
    tipo_pessoa text,
    acessos int    
); 



CREATE OR REPLACE FUNCTION FUNCTION_LOG_TV_ASSINATURA() RETURNS TRIGGER AS $$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
        INSERT INTO log_tv_assinatura(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Inclusão realizada. ' || NEW.* || ' .' );
        RETURN NEW;
        ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO log_tv_assinatura(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Alteração realizada. Operação antiga: ' || OLD.* || ' para nova operação ' || NEW.* || ' .' );
        RETURN NEW;
        ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO log_tv_assinatura(usuario, data_registro, dados) VALUES (CURRENT_USER, CURRENT_TIMESTAMP, 'Deleção realizada. Operação deletada: ' || OLD.* || ' .' );
        RETURN OLD;
        END IF;
        RETURN NULLs;
    END;
$$
LANGUAGE 'plpgsql';



CREATE TRIGGER Tr_log_tv_assinatura AFTER INSERT or UPDATE or DELETE ON tv_assinatura
FOR EACH ROW EXECUTE PROCEDURE FUNCTION_LOG_TV_ASSINATURA();



CREATE OR REPLACE FUNCTION TvAssinatura_function_normalizacao1() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view EmpresaTVassinatura as 
            select id_tv_assinatura, Empresa, Ano, mes, grupo_economico, 
            cnpj, porte_prestadora, Tecnologia, Meio_acesso 
            from tv_assinatura
            order by Empresa;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER tv_assinatura_1 AFTER INSERT ON tv_assinatura
FOR EACH ROW EXECUTE PROCEDURE TvAssinatura_function_normalizacao1();




CREATE OR REPLACE FUNCTION TvAssinatura_function_normalizacao2() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view LocalidadeTvAssinatura as 
            select id_tv_assinatura, uf, municipio, codigo_ibge_municipio
            from tv_assinatura
            order by municipio;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER tv_assinatura_2 AFTER INSERT ON tv_assinatura
FOR EACH ROW EXECUTE PROCEDURE TvAssinatura_function_normalizacao2();




CREATE OR REPLACE FUNCTION TvAssinatura_function_normalizacao3() RETURNS TRIGGER as $$
    BEGIN
    	IF (tg_op = 'INSERT') THEN
            CREATE or REPLACE view TipoPessoaTvAssinatura as 
            select id_tv_assinatura, tipo_pessoa, acessos
            from tv_assinatura
            order by municipio;   
        END IF;
        RETURN NULL;
    END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER tv_assinatura_3 AFTER INSERT ON tv_assinatura
FOR EACH ROW EXECUTE PROCEDURE TvAssinatura_function_normalizacao3();

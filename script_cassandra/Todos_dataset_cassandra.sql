-- SCRIPT CASSANDRA

CREATE KEYSPACE IF NOT EXISTS telecomunicacoes
    WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

use telecomunicacoes;
id_banda INT, 
CREATE TABLE IF NOT EXISTS banda_larga (
    id_banda int primary key,
    ano INT, 
    mes INT, 
    grupo_economico TEXT, 
    empresa TEXT, 
    cnpj BIGINT, 
    porte_da_prestadora TEXT, 
    uf TEXT, 
    municipio TEXT, 
    codigo_ibge_municipio BIGINT, 
    faixa_de_velocidade TEXT, 
    tecnologia TEXT, 
    meio_de_acesso TEXT, 
    tipo_de_pessoa TEXT, 
    acessos INT,
);


create table if not exists municipio_acesso (
    id_acessos int primary key,
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



create table if not exists cobertura_movel (
    id_movel INT primary key,
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



create table if not exists municipio_cobertura (
    id_cobertura INT primary key,
    Ano TEXT,
    operadora text,
    tecnologia_cobertura text,
    Moradores_cobertos text,
    Domicilios_cobertos text,
    Area_coberta text,
    Moradores_municipio TEXT,
    domicilios_Municipio text,
    Area_municipio text,
    codigo_ibge text,
    municipio text,
    uf text,
    nome_uf text,
    regiao text,
    codigo_nacional TEXT
); 





create table if not exists tv_assinatura (
    id_tv_assinatura int PRIMARY KEY ,
    Empresa TEXT,
    ano INT,
    mes INT,
    grupo_economico TEXT,
    CNPJ BIGINT,
    porte_prestadora TEXT,
    Tecnologia TEXT,
    Meio_acesso TEXT,
    uf TEXT,
    municipio TEXT,
    codigo_ibge_municipio INT,
    tipo_pessoa TEXT,
    acessos INT 
); 



create table if not exists Velocidade_contratada (
    id_Velocidade_contratada INT primary key,
    ano int,
    mes int,
    razao_social text,	
    cnpj BIGINT,	
    velocidade_contratada_mbps text,
    uf text,
    municipio text,	
    codigo_ibge BIGINT,
    acessos INT,
    tipo text,	
    municipio_uf text
);
from connector_postgre import Interface_db_postgre, get_db_info


if __name__ == "__main__":
    try:  
        
        # CONEXAO COM O POSTGRE
        user, password, host, database = get_db_info()
        db = Interface_db_postgre(user, password, host, database)

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA BANDA_LARGA QUE CRIA VIEW EmpresaBandaLarga
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados de Empresa.
# Apos inserir algum dado na tabela primaria banda_larga criará uma view EmpresaBandaLarga,
# contendo id_banda, ano, mes, grupo_economico, empresa, cnpj e porte_da_prestadora.

        function_normalizacao_empresa_banda_larga = "CREATE OR REPLACE FUNCTION func_banda_larga_normalizacao_empresa() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view EmpresaBandaLarga as \
                    select id_banda, ano, mes, grupo_economico, empresa, cnpj, porte_da_prestadora \
                    from banda_larga \
                    order by empresa; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_empresa_banda_larga)
        print(">> Function func_banda_larga_normalizacao_empresa created")

        trigger_empresa_banda_larga = "CREATE TRIGGER tr_banda_larga_empresa AFTER INSERT ON banda_larga \
        FOR EACH ROW EXECUTE PROCEDURE func_banda_larga_normalizacao_empresa();"
        db.execute(trigger_empresa_banda_larga)
        print(">> Trigger tr_banda_larga_empresa created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA BANDA_LARGA QUE CRIA VIEW TecnologiaBandaLarga
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados por Tecnologia.
# Apos inserir algum dado na tabela primaria banda_larga criará uma view TecnologiaBandaLarga,
# contendo id_banda, tecnologia, faixa_de_velocidade e meio_de_acesso.

        function_normalizacao_tecnologia_banda_larga = "CREATE OR REPLACE FUNCTION func_banda_larga_normalizacao_tecnologia() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view TecnologiaBandaLarga as \
                    select id_banda, tecnologia, faixa_de_velocidade, meio_de_acesso \
                    from banda_larga \
                    order by tecnologia; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_tecnologia_banda_larga)
        print(">> Function func_banda_larga_normalizacao_tecnologia created")

        trigger_tecnologia_banda_larga = "CREATE TRIGGER tr_banda_larga_tecnologia AFTER INSERT ON banda_larga \
        FOR EACH ROW EXECUTE PROCEDURE func_banda_larga_normalizacao_tecnologia();"
        db.execute(trigger_tecnologia_banda_larga)
        print(">> Trigger tr_banda_larga_tecnologia created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA BANDA_LARGA QUE CRIA VIEW LocalidadeBandaLarga
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados por Localidade.
# Apos inserir algum dado na tabela primaria banda_larga criará uma view LocalidadeBandaLarga,
# contendo id_banda, uf, municipio e codigo_ibge_municipio.

        function_normalizacao_localidade_banda_larga = "CREATE OR REPLACE FUNCTION func_banda_larga_normalizacao_localidade() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view LocalidadeBandaLarga as \
                    select id_banda, uf, municipio, codigo_ibge_municipio \
                    from banda_larga \
                    order by municipio; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_localidade_banda_larga)
        print(">> Function func_banda_larga_normalizacao_localidade created")

        trigger_localidade_banda_larga = "CREATE TRIGGER tr_banda_larga_localidade AFTER INSERT ON banda_larga \
        FOR EACH ROW EXECUTE PROCEDURE func_banda_larga_normalizacao_localidade();"
        db.execute(trigger_localidade_banda_larga)
        print(">> Trigger tr_banda_larga_localidade created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA BANDA_LARGA QUE CRIA VIEW AcessosBandaLarga
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados por quantidade de Acessos.
# Apos inserir algum dado na tabela primaria banda_larga criará uma view AcessosBandaLarga,
# contendo id_banda, tipo_de_pessoa e acessos.

        function_normalizacao_acessos_banda_larga = "CREATE OR REPLACE FUNCTION func_banda_larga_normalizacao_acessos() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view AcessosBandaLarga as \
                    select id_banda, tipo_de_pessoa, acessos \
                    from banda_larga \
                    order by id_banda; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_acessos_banda_larga)
        print(">> Function func_banda_larga_normalizacao_acessos created")

        trigger_acessos_banda_larga = "CREATE TRIGGER tr_banda_larga_acessos AFTER INSERT ON banda_larga \
        FOR EACH ROW EXECUTE PROCEDURE func_banda_larga_normalizacao_acessos();"
        db.execute(trigger_acessos_banda_larga)
        print(">> Trigger tr_banda_larga_acessos created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA COBERTURA_MOVEL QUE CRIA VIEW OperadoraMovel
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados de operadora movel.
# Apos inserir algum dado na tabela primaria cobertura_movel criará uma view OperadoraMovel,
# contendo id_movel, ano, operadora e tecnologia.

        function_normalizacao_operadora_movel_cobertura_movel = "CREATE OR REPLACE FUNCTION func_cobertura_movel_normalizacao_operadora_movel() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view OperadoraMovel as \
                    select id_movel, ano, operadora, tecnologia \
                    from cobertura_movel \
                    order by operadora; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_operadora_movel_cobertura_movel)
        print(">> Function func_cobertura_movel_normalizacao_operadora_movel created")

        trigger_opradora_movel_cobertura_movel = "CREATE TRIGGER tr_cobertura_movel_operadora_movel AFTER INSERT ON cobertura_movel \
        FOR EACH ROW EXECUTE PROCEDURE func_cobertura_movel_normalizacao_operadora_movel();"
        db.execute(trigger_opradora_movel_cobertura_movel)
        print(">> Trigger tr_cobertura_movel_operadora_movel created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA COBERTURA_MOVEL QUE CRIA VIEW TipoSetorMovel
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados do tipo de setor.
# Apos inserir algum dado na tabela primaria cobertura_movel criará uma view TipoSetorMovel,
# contendo id_movel, codigo_setor_censitario, bairro, tipo_setor, codigo_localidade,
# nome_localidade, categoria_localidade, localidade_agregadora.

        function_normalizacao_tipo_setor_cobertura_movel = "CREATE OR REPLACE FUNCTION func_cobertura_movel_normalizacao_tipo_setor() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view TipoSetorMovel as \
                    select id_movel, codigo_setor_censitario, bairro, tipo_setor, codigo_localidade, \
                    nome_localidade, categoria_localidade, localidade_agregadora \
                    from cobertura_movel \
                    order by tipo_setor; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_tipo_setor_cobertura_movel)
        print(">> Function func_cobertura_movel_normalizacao_tipo_setor created")

        trigger_tipo_setor_cobertura_movel = "CREATE TRIGGER tr_cobertura_movel_tipo_setor AFTER INSERT ON cobertura_movel \
        FOR EACH ROW EXECUTE PROCEDURE func_cobertura_movel_normalizacao_tipo_setor();"
        db.execute(trigger_tipo_setor_cobertura_movel)
        print(">> Trigger tr_cobertura_movel_tipo_setor created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA COBERTURA_MOVEL QUE CRIA VIEW MunicipioMovel
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados por municipio.
# Apos inserir algum dado na tabela primaria cobertura_movel criará uma view MunicipioMovel,
# contendo id_movel, codigo_municipio, municipio, uf, regiao e area.

        function_normalizacao_municipios_cobertura_movel = "CREATE OR REPLACE FUNCTION func_cobertura_movel_normalizacao_municipio() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view MunicipioMovel as \
                    select id_movel, codigo_municipio, municipio, uf, regiao, area \
                    from cobertura_movel \
                    order by municipio; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_municipios_cobertura_movel)
        print(">> Function func_cobertura_movel_normalizacao_municipio created")

        trigger_municipios_cobertura_movel = "CREATE TRIGGER tr_cobertura_movel_municipios AFTER INSERT ON cobertura_movel \
        FOR EACH ROW EXECUTE PROCEDURE func_cobertura_movel_normalizacao_municipio();"
        db.execute(trigger_municipios_cobertura_movel)
        print(">> Trigger tr_cobertura_movel_municipios created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA COBERTURA_MOVEL QUE CRIA VIEW MoradoresMovel
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados por moradores.
# Apos inserir algum dado na tabela primaria cobertura_movel criará uma view MoradoresMovel,
# contendo id_movel, moradores, domicilios e percentual_cobertura.

        function_normalizacao_moradores_cobertura_movel = "CREATE OR REPLACE FUNCTION func_cobertura_movel_normalizacao_moradores() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view MoradoresMovel as \
                    select id_movel, moradores, domicilios, percentual_cobertura \
                    from cobertura_movel \
                    order by id_movel; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_moradores_cobertura_movel)
        print(">> Function func_cobertura_movel_normalizacao_moradores created")

        trigger_moradores_cobertura_movel = "CREATE TRIGGER tr_cobertura_movel_moradores AFTER INSERT ON cobertura_movel \
        FOR EACH ROW EXECUTE PROCEDURE func_cobertura_movel_normalizacao_moradores();"
        db.execute(trigger_moradores_cobertura_movel)
        print(">> Trigger tr_cobertura_movel_moradores created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA MUNICIPIOS_ACESSOS QUE CRIA VIEW ServicosMunicipios
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados por tipo de serviços e acessos.
# Apos inserir algum dado na tabela primaria municipios_acessos criará uma view ServicosMunicipios,
# contendo id_acessos, ano, mes, acesso e servico.

        function_normalizacao_servicos_municipios_acessos = "CREATE OR REPLACE FUNCTION func_municipios_acessos_normalizacao_municipios() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view ServicosMunicipios as \
                    select id_acessos, ano, mes, acessos, servico \
                    from municipios_acessos \
                    order by servico;   \
                END IF;\
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_servicos_municipios_acessos)
        print(">> Function func_municipios_acessos_normalizacao_municipios created")

        trigger_servicos_municipios_acessos = "CREATE TRIGGER tr_municipios_acessos_servicos AFTER INSERT ON municipios_acessos \
        FOR EACH ROW EXECUTE PROCEDURE func_municipios_acessos_normalizacao_municipios();"
        db.execute(trigger_servicos_municipios_acessos)
        print(">> Trigger tr_municipios_acessos_servicos created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA MUNICIPIOS_ACESSOS QUE CRIA VIEW LocaisAcessos
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados por local dos acessos.
# Apos inserir algum dado na tabela primaria municipios_acessos criará uma view LocaisAcessos,
# contendo id_acessos, Densidade, codigo_ibge, municipio, uf, nome_uf, regiao e codigo_nacional.

        function_normalizacao_locais_municipios_acessos = "CREATE OR REPLACE FUNCTION func_municipios_acessos_normalizacao_locais() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view LocaisAcessos as \
                    select id_acessos, densidade, codigo_ibge, municipio, uf, nome_uf, regiao, codigo_nacional \
                    from municipios_acessos \
                    order by municipio; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_locais_municipios_acessos)
        print(">> Function func_municipios_acessos_normalizacao_locais created")

        trigger_locais_municipios_acessos = "CREATE TRIGGER tr_municipios_acessos_locais AFTER INSERT ON municipios_acessos \
        FOR EACH ROW EXECUTE PROCEDURE func_municipios_acessos_normalizacao_locais();"
        db.execute(trigger_locais_municipios_acessos)
        print(">> Trigger tr_municipios_acessos_locais created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA MUNICIPIOS_COBERTURA QUE CRIA VIEW OperadorasCobertura
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados da cobertura por municipio.
# Apos inserir algum dado na tabela primaria municipios_cobertura criará uma view OperadorasCobertura,
# contendo id_cobertura, ano, operadora e tecnologia_cobertura.

        function_normalizacao_operadoras_municipios_cobertura = "CREATE OR REPLACE FUNCTION func_municipios_cobertura_normalizacao_operadoras() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view OperadorasCobertura as \
                    select id_cobertura, ano, operadora, tecnologia_cobertura \
                    from municipios_cobertura \
                    order by operadora; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_operadoras_municipios_cobertura)
        print(">> Function func_municipios_cobertura_normalizacao_operadoras created")

        trigger_operadoras_cobertura_municipios_cobertura = "CREATE TRIGGER tr_municipios_cobertura_operadoras AFTER INSERT ON municipios_cobertura \
        FOR EACH ROW EXECUTE PROCEDURE func_municipios_cobertura_normalizacao_operadoras();"
        db.execute(trigger_operadoras_cobertura_municipios_cobertura)
        print(">> Trigger tr_municipios_cobertura_operadoras created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA MUNICIPIOS_COBERTURA QUE CRIA VIEW AreaCobertura
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados da area coberta por municipio.
# Apos inserir algum dado na tabela primaria municipios_cobertura criará uma view AreaCobertura,
# contendo id_cobertura, moradores_cobertos, domicilios_cobertos e area_coberta.

        function_normalizacao_area_cobertura_municipios_cobertura = "CREATE OR REPLACE FUNCTION func_municipios_cobertura_normalizacao_area() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view AreaCobertura as \
                    select id_cobertura, moradores_cobertos, domicilios_cobertos, area_coberta \
                    from municipios_cobertura \
                    order by id_cobertura; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_area_cobertura_municipios_cobertura)
        print(">> Function func_municipios_cobertura_normalizacao_area created")

        trigger_operadoras_area_cobertura_municipios_cobertura = "CREATE TRIGGER tr_municipios_cobertura_area AFTER INSERT ON municipios_cobertura \
        FOR EACH ROW EXECUTE PROCEDURE func_municipios_cobertura_normalizacao_area();"
        db.execute(trigger_operadoras_area_cobertura_municipios_cobertura)
        print(">> Trigger tr_municipios_cobertura_area created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA MUNICIPIOS_COBERTURA QUE CRIA VIEW AreaMunicipios
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados da area por municipio.
# Apos inserir algum dado na tabela primaria municipios_cobertura criará uma view AreaMunicipios,
# contendo id_cobertura, moradores_municipio, domicilios_municipio earea_municipio.

        function_normalizacao_area_municipios_cobertura = "CREATE OR REPLACE FUNCTION func_municipios_cobertura_normalizacao_municipios() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view AreaMunicipios as \
                    select id_cobertura, moradores_municipio, domicilios_municipio, area_municipio \
                    from municipios_cobertura \
                    order by id_cobertura; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_area_municipios_cobertura)
        print(">> Function func_municipios_cobertura_normalizacao_municipios created")

        trigger_operadoras_areas_municipios_cobertura = "CREATE TRIGGER tr_municipios_cobertura_area_municipio AFTER INSERT ON municipios_cobertura \
        FOR EACH ROW EXECUTE PROCEDURE func_municipios_cobertura_normalizacao_municipios();"
        db.execute(trigger_operadoras_areas_municipios_cobertura)
        print(">> Trigger tr_municipios_cobertura_area_municipio created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA MUNICIPIOS_COBERTURA QUE CRIA VIEW LocaisCobertura
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados por locais de cobertura.
# Apos inserir algum dado na tabela primaria municipios_cobertura criará uma view LocaisCobertura,
# contendo id_cobertura, codigo_ibge, municipio, uf, nome_uf, regiao e codigo_nacional.

        function_normalizacao_locais_municipios_cobertura = "CREATE OR REPLACE FUNCTION func_municipios_cobertura_normalizacao_locais() RETURNS TRIGGER as $$ \
        BEGIN \
            IF (tg_op = 'INSERT') THEN \
                CREATE or REPLACE view LocaisCobertura as \
                select id_cobertura, codigo_ibge, municipio, uf, nome_uf, regiao, codigo_nacional \
                from municipios_cobertura \
                order by municipio; \
            END IF; \
            RETURN NULL; \
        END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_normalizacao_locais_municipios_cobertura)
        print(">> Function func_municipios_cobertura_normalizacao_locais created")

        trigger_operadoras_locais_municipios_cobertura = "CREATE TRIGGER tr_municipios_cobertura_locais AFTER INSERT ON municipios_cobertura \
        FOR EACH ROW EXECUTE PROCEDURE func_municipios_cobertura_normalizacao_locais();"
        db.execute(trigger_operadoras_locais_municipios_cobertura)
        print(">> Trigger tr_municipios_cobertura_locais created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA TV_ASSINATURA QUE CRIA VIEW EmpresaTVassinatura
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados de Empresa por TV por assinatura.
# Apos inserir algum dado na tabela primaria tv_assinatura criará uma view EmpresaTVassinatura,
# contendo id_tv_assinatura, empresa, ano, mes, grupo_economico, cnpj, porte_prestadora,
# tecnologia e meio_acesso.

        function_empresa_tv_assinatura = "CREATE OR REPLACE FUNCTION func_tv_assinatura_normalizacao_empresa() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view EmpresaTVassinatura as \
                    select id_tv_assinatura, ano, mes, grupo_economico, empresa, cnpj, porte_prestadora, tecnologia, meio_acesso \
                    from tv_assinatura \
                    order by Empresa; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_empresa_tv_assinatura)
        print(">> Function func_tv_assinatura_normalizacao_empresa created")


        trigger_empresa_tv_assinatura = "CREATE TRIGGER tr_tv_assinatura_empresa AFTER INSERT ON tv_assinatura \
        FOR EACH ROW EXECUTE PROCEDURE func_tv_assinatura_normalizacao_empresa();"
        db.execute(trigger_empresa_tv_assinatura)
        print(">> Trigger tr_tv_assinatura_empresa created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA TV_ASSINATURA QUE CRIA VIEW LocalidadeTvAssinatura
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados de localidade de TV por assinatura.
# Apos inserir algum dado na tabela primaria tv_assinatura criará uma view LocalidadeTvAssinatura,
# contendo id_tv_assinatura, uf, municipio e codigo_ibge_municipio.

        function_localidade_tv_assinatura = "CREATE OR REPLACE FUNCTION func_tv_assinatura_normalizacao_localidade() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view LocalidadeTvAssinatura as \
                    select id_tv_assinatura, uf, municipio, codigo_ibge_municipio \
                    from tv_assinatura \
                    order by municipio; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_localidade_tv_assinatura)
        print(">> Function func_tv_assinatura_normalizacao_localidade created")

        trigger_localidade_tv_assinatura = "CREATE TRIGGER tr_tv_assinatura_localidade AFTER INSERT ON tv_assinatura \
        FOR EACH ROW EXECUTE PROCEDURE func_tv_assinatura_normalizacao_localidade();"
        db.execute(trigger_localidade_tv_assinatura)
        print(">> Trigger tr_tv_assinatura_localidade created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA TV_ASSINATURA QUE CRIA VIEW TipoPessoaTvAssinatura
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados por tipo de pessoa e TV por assinatura.
# Apos inserir algum dado na tabela primaria tv_assinatura criará uma view TipoPessoaTvAssinatura,
# contendo id_tv_assinatura, tipo_pessoa e acessos.

        function_tipo_pessoa_tv_assinatura = "CREATE OR REPLACE FUNCTION func_tv_assinatura_normalizacao_tipo_pessoa() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view TipoPessoaTvAssinatura as \
                    select id_tv_assinatura, tipo_pessoa, acessos \
                    from tv_assinatura \
                    order by municipio; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_tipo_pessoa_tv_assinatura)
        print(">> Function func_tv_assinatura_normalizacao_tipo_pessoa created")

        trigger_tipo_pessoa_tv_assinatura = "CREATE TRIGGER tr_tv_assinatura_tipo_pessoa AFTER INSERT ON tv_assinatura \
        FOR EACH ROW EXECUTE PROCEDURE func_tv_assinatura_normalizacao_tipo_pessoa();"
        db.execute(trigger_tipo_pessoa_tv_assinatura)
        print(">> Trigger tr_tv_assinatura_tipo_pessoa created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA VELOCIDADE_CONTRATADA QUE CRIA VIEW EmpresaVelocidade
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados das velocidades por empresa.
# Apos inserir algum dado na tabela primaria velocidade_contratada criará uma view EmpresaBandaLarga,
# contendo id_velocidade_contratada, ano, mes, razao_social, cnpj e velocidade_contratada_mbps.

        function_empresa_velocidade_contratada = "CREATE OR REPLACE FUNCTION func_velocidade_contratada_normalizacao_empresa() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view EmpresaVelocidade as \
                    select id_velocidade_contratada, ano, mes, razao_social, cnpj, velocidade_contratada_mbps \
                    from velocidade_contratada \
                    order by razao_social; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_empresa_velocidade_contratada)
        print(">> Function func_velocidade_contratada_normalizacao_empresa created")

        trigger_empresa_velocidade_contratada = "CREATE TRIGGER tr_velocidade_contratada_empresa AFTER INSERT ON velocidade_contratada \
        FOR EACH ROW EXECUTE PROCEDURE func_velocidade_contratada_normalizacao_empresa();"
        db.execute(trigger_empresa_velocidade_contratada)
        print(">> Trigger tr_velocidade_contratada_empresa created")

# -----------------------------------------------------------------------------------------------------
# FUNCAO PARA NORMALIZACAO DA TABELA VELOCIDADE_CONTRATADA QUE CRIA VIEW LocalidadeVelocidade
# -----------------------------------------------------------------------------------------------------

# Funcao e trigger que cria uma tabela temporaria com visualizacao dos dados das velocidades por localidade.
# Apos inserir algum dado na tabela primaria velocidade_contratada criará uma view LocalidadeVelocidade,
# contendo id_velocidade_contratada, uf, municipio, codigo_ibge, acessos, tipo e municipio_uf.

        function_localidade_velocidade_contratada = "CREATE OR REPLACE FUNCTION func_velocidade_contratada_normalizacao_localidade() RETURNS TRIGGER as $$ \
            BEGIN \
                IF (tg_op = 'INSERT') THEN \
                    CREATE or REPLACE view LocalidadeVelocidade as \
                    select id_velocidade_contratada, uf, municipio, codigo_ibge, acessos, tipo, municipio_uf \
                    from Velocidade_contratada \
                    order by municipio; \
                END IF; \
                RETURN NULL; \
            END; \
        $$ \
        LANGUAGE 'plpgsql';"
        db.execute(function_localidade_velocidade_contratada)
        print(">> Function func_velocidade_contratada_normalizacao_localidade created")

        trigger_localidade_velocidade_contratada = "CREATE TRIGGER tr_velocidade_contratada_localidade AFTER INSERT ON velocidade_contratada \
        FOR EACH ROW EXECUTE PROCEDURE func_velocidade_contratada_normalizacao_localidade();"
        db.execute(trigger_localidade_velocidade_contratada)
        print(">> Trigger tr_velocidade_contratada_localidade created")

        print(">> Script finished")
        
    except Exception as e:
        print("Error in normalization script postgre: ", str(e))
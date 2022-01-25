Trabalho final do curso de Engenharia de Dados da SoulCode Academy - Turma BC8

Integrantes: Bruno Miranda Magalhaes
             Edney Moreira de Carvalho
             Edson Sabino da Silva
             Leila Moreira Gomes Roque
             Thaís Cristina de Aguiar

Orientador: Felipe Muylaert

Tema:
Telecomunicações
Fonte dos dados: https://dados.gov.br/dataset/ 

6 bases de dados: Acessos_Banda_Larga_Fixa_2021.csv
                  Acessos_TV_Assinatura.csv
                  Cobertura_Todas.csv
                  Meu_Municipio_Acessos.csv
                  Meu_Municipio_Cobertura.csv
                  Velocidade_Contratada_SCM.csv

Configurações necessarias:
    arquivo config - IP Cassandra, caminho arquivo parquet e caminho arquivos csv
    connector_postgre - user, password, host, database
    connector_cassandra - keyspace

*Necessário criar bancos de dados

Ordem de execução dos scripts:
    script_tables_postgre.py
    treatment_banda_larga.py
    postgre_insert_banda_larga.py
    treatment_cobertura_movel.py
    postgre_insert_cobertura_movel.py
    treatment_municipio_acessos.py
    postgre_insert_municipio_acessos.py
    treatment_municipio_cobertura.py
    postgre_insert_municipio_cobertura.py
    treatment_tv_assinatura.py
    postgre_insert_tv_assinatura.py
    treatment_velocidade_contratada.py
    postgre_insert_velocidade_contratada.py
    script_normalization_postgre.py
    script_cassandra.py
    cassandra_insert_banda_larga.py
    cassandra_insert_cobertura_movel.py
    cassandra_insert_municipios_acessos.py
    cassandra_insert_municipios_cobertura.py
    cassandra_insert_tv_assinatura.py
    cassandra_insert_velocidade_contratada.py                            



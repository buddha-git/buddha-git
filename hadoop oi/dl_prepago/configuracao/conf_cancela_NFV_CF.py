from conf_geral import *
from valida_lib import *

dir_processamento = dir_base+"/processamento/sap01/"
dir_rejeitado = dir_base+"/rejeitado/sap01/"
arquivo = ""
nome = "Cancela NFV CF"
modulo = "cancela_NFV_CF.py"
cod_programa = "103"
tipo = "cancela"
create_TMP_STG_PP_NFV_CF = "create table "+db_stg+".TMP_STG_PP_NFV_CF(DH_EXTRACAO_SAP timestamp,DH_EXTRACAO_INTEGRACAO timestamp,NUMERO_NF string,NUM_ORDEM_VENDA string,DATA_HORA_NF timestamp,ID_ITEM_NF string,VALOR_ITEM_NF decimal(10,2),VALOR_NF decimal(10,2),VALOR_MATERIAL decimal(10,2),COD_DISTRIBUIDOR_SAP string,ORGANIZACAO_VENDA string,SETOR_ATIVIDADE string,CANAL_DISTRIBUICAO string,UF string,TIPO_ORDEM string,MATERIAL string,DESC_MATERIAL string,SERIE string,SUBSERIE string,CFOP string,NUM_PEDIDO string,NUMERO_NF_CAN string,NUM_ORDEM_VENDA_CAN string,DATA_HORA_NF_CAN string,TIPO_ORDEM_CAN string,STATUS string,LISTA_SERIES_CAN array<string>,LISTA_SERIES array<string>,LISTA_SERIES_VAL array<string>,VALOR_FACE decimal(10,2),DT_EXECUCAO timestamp) PARTITIONED BY (dt string) STORED AS parquet"
create_TMP_STG_PP_NFV_CF_CAN = "create table "+db_stg+".TMP_STG_PP_NFV_CF_CAN(DH_EXTRACAO_SAP timestamp,DH_EXTRACAO_INTEGRACAO timestamp,DATA_HORA_NF timestamp,NUMERO_NF string,NUM_ORDEM_VENDA string,TIPO_ACAO string,TIPO_ORDEM string,NUMERO_NF_ORIGINAL string,NUMERO_ORDEM_VENDA_ORIGINAL string,LISTA_SERIES array<string>,CANCELOU boolean,DT_EXECUCAO timestamp) PARTITIONED BY (dt string) STORED AS parquet"

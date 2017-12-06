from valida_lib import *
from conf_geral import *

dir_processamento = dir_base+"/processamento/sap01/"
dir_rejeitado = dir_base+"/rejeitado/sap01/"
arquivo = ""
nome = "Cancela NFV"
modulo = "cancela_NFV.py"
cod_programa = "102"
tipo = "cancela"
create_tmp_stg_pp_nfv_ncf_can = "create table "+db_stg+".tmp_stg_pp_nfv_ncf_can(DH_EXTRACAO_SAP timestamp,DH_EXTRACAO_INTEGRACAO timestamp,DATA_HORA_NF timestamp,NUMERO_NF string,NUM_ORDEM_VENDA string,TIPO_ACAO string,TIPO_ORDEM string,NUMERO_NF_ORIGINAL string,NUMERO_ORDEM_VENDA_ORIGINAL string,CANCELOU boolean,DT_EXECUCAO timestamp) PARTITIONED BY (dt string) STORED AS parquet"

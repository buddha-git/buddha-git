from valida_lib import *
from conf_geral import *

arquivo = ""
nome = "Associa Numero Fatura"
modulo = "assoc_num_fatura.py"
cod_programa = "109"
tipo = "assoc"
create_TMP_TB_RECARGA_FATURA = "create table "+db_stg+".TMP_TB_RECARGA_FATURA(DH_EXTRACAO timestamp,NUMERO_FAT string,MSISDN string,DATA_HORA_REC timestamp,DATA_HORA_FAT timestamp,VALOR_RECARGA decimal(10,2),NSU_REQUISICAO string,DT_EXECUCAO timestamp) PARTITIONED BY (atualizou string,dt string) STORED AS PARQUET"

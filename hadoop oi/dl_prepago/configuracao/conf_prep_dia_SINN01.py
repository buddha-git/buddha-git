from valida_lib import *
from conf_geral import *

dir_processamento = dir_base+"/processamento/sinn01/"
dir_processado = dir_base+"/processado/sinn01/"
dir_rejeitado = dir_base+"/rejeitado/sinn01/"
arquivo = "SINN_CONSUMO_dataArquivo_*.txt"
nome = "Valida SINN01"
modulo = "prep_dia_SINN01.py"
cod_programa = "111"
tipo = "prep_dia"
interface = "sinn01"
create_TMP_TB_NFC_OITV = "create table "+db_stg+".TMP_TB_NFC_OITV(DATA_HORA_CONSUMO timestamp,DATA_NF string,NUMERO_NF string,HASH_CODE string,SERIE string,SUBSERIE string,CFOP string,MSISDN string,NOME string,CPF_CNPJ string,VALOR_BOLSO decimal(10,2),VALOR_NF decimal(10,2),VALOR_ICMS decimal(10,2),ALIQUOTA string,UF string,ID_CONSUMIDOR string,ENCONTROU boolean,DT_EXECUCAO timestamp) PARTITIONED BY (dt string)STORED AS PARQUET"

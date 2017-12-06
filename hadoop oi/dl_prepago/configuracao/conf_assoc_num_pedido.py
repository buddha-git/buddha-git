from valida_lib import *
from conf_geral import *

arquivo = ""
nome = "Associa Numero Pedido"
modulo = "assoc_num_pedido.py"
cod_programa = "108"
tipo = "assoc"
create_TMP_TB_RECARGA_PEDIDO = "create table "+db_stg+".TMP_TB_RECARGA_PEDIDO(DH_EXTRACAO timestamp,TP_DOCUMENTO string,COD_DISTRIBUIDOR_SAP string,ORG_VENDAS string,CANAL_DISTRIBUICAO string,SETOR_ATIVIDADE string,CENTRO string,DATA_SOLIC_ORDEM_VENDA timestamp,NUMERO_PEDIDO string,MATERIAL string,TP_CONDICAO string,ID_RECARGA string,CD_ORIGEM string,DT_EXECUCAO timestamp) PARTITIONED BY (atualizou string,dt string) STORED AS PARQUET"

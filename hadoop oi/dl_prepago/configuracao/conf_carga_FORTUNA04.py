from conf_geral import * 
from valida_lib import *

dir_processamento = dir_base+"/processamento/fortuna04/"
dir_processado = dir_base+"/processado/fortuna04/"
dir_rejeitado = dir_base+"/rejeitado/fortuna04/"
arquivo = "FF_PRODUTO_RECARGA_PREPAGO_dataArquivo*.txt"
nome = "Valida FORTUNA04"
modulo = "carga_FORTUNA04.py"
cod_programa = "126"
tipo = "carga"
interface = "fortuna04"
create_tmp_tb_produto_recarga = "create table "+db_stg+".tmp_tb_produto_recarga(DH_EXTRACAO timestamp,CD_PRODUTO_RECARGA string,NO_PRODUTO string,DT_CRIACAO_PRODUTO timestamp,DH_ATUALIZACAO timestamp,DT_INICIO_VIGENCIA timestamp,DT_FIM_VIGENCIA timestamp,NU_VALIDADE string,NU_VALOR_FACE string,DS_TIPO_RECARGA string,IN_MULTIPLO_USO string,CD_DIREITO string,NU_VALOR_DIREITO string,TIPO_USO string,VALOR_FACE_FINAL string,PERCENTUAL string,VALOR_CONTABIL string,CATEG_CONTABIL string,COD_ID_DEBITO string,DT_EXECUCAO timestamp) stored as parquet"


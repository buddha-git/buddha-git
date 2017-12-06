from valida_lib import *
from conf_geral import *

dir_processamento = dir_base+"/processamento/sap03/" 
dir_processado = dir_base+"/processado/sap03/" 
dir_rejeitado = dir_base+"/rejeitado/sap03/" 
arquivo = "SAP_DISTRIBUIDOR_dataArquivo*.txt" 
nome = "Valida SAP03" 
modulo = "prep_dia_SAP03.py" 
cod_programa = "104" 
tipo = "prep_dia"
interface = "sap03"
create_TMP_TB_DISTRIBUIDORES = "create table "+db_stg+".TMP_TB_DISTRIBUIDORES(DH_EXTRACAO timestamp,CD_DISTRIBUIDOR_SAP string,NO_FANTASIA string,CD_CNPJ string,CD_ORGANIZACAO_VENDA string,DS_ORGANIZACAO_VENDA string,CD_CANAL_SAP string,DS_CANAL_SAP string,CD_SETOR_ATIVIDADE string,DS_SETOR_ATIVIDADE string,DS_TP_DISTRIBUIDOR string,DS_LOGRADOURO string,NU_ENDERECO string,DS_COMPLEMENTO string,DS_CIDADE string,DS_BAIRRO string,DS_ESTADO string,NU_CEP string,IN_ELIMINACAO string,DH_ELIMINACAO string,DH_ATUALIZACAO string,Modelo_de_Contrato string,Centro_de_Distribuidor string,Empresa string,DT_EXECUCAO timestamp) STORED AS PARQUET".format(db = db_stg)

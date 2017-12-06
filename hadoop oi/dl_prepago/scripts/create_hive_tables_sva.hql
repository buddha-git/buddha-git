CREATE table dl_prepago.tb_ctl_arq_proc 
(DH_PROCESSAMENTO TIMESTAMP, 
NOME_ARQUIVO STRING, 
CHECKSUM STRING, 
MODULO STRING) 
CLUSTERED BY (MODULO) 
SORTED BY (CHECKSUM) 
INTO 32 BUCKETS;

create table dl_stg_prepago.STG_PP_RECARGA_PS
(
DH_EXTRACAO timestamp,
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
NSU_REQUISICAO string,
ID_RECARGA string,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
CANAL_RECARGA string,
SUB_CANAL string,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_CAMPANHA string,
CD_TIPO_PRODUTO string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
FLAG_ACEITE_UPSELL string,
FLAG_UPSELL_HABILITADO string,
VALOR_ORIGINAL string,
MEIO_PAGAMENTO string,
MSISDN_PAGADOR string,
CD_CELL_ID string,
CD_AREA_CELL_ID string,
ID_PLANO_FAT string,
NUMERO_PEDIDO string,
DATA_SOLIC_ORDEM_VENDA timestamp,
NUMERO_FATURA string,
DATA_HORA_FATURA timestamp,
CNPJ_EMISSOR string,
NU_SERIE string,
NU_VOUCHER string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table dl_stg_prepago.STG_PP_RECARGA_CV
(
DH_EXTRACAO timestamp,
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
NSU_REQUISICAO string,
ID_RECARGA string,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
CANAL_RECARGA string,
SUB_CANAL string,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_CAMPANHA string,
CD_TIPO_PRODUTO string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
FLAG_ACEITE_UPSELL string,
FLAG_UPSELL_HABILITADO string,
VALOR_ORIGINAL string,
MEIO_PAGAMENTO string,
MSISDN_PAGADOR string,
CD_CELL_ID string,
CD_AREA_CELL_ID string,
ID_PLANO_FAT string,
NUMERO_PEDIDO string,
DATA_SOLIC_ORDEM_VENDA timestamp,
NUMERO_FATURA string,
DATA_HORA_FATURA timestamp,
CNPJ_EMISSOR string,
NU_SERIE string,
NU_VOUCHER string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table dl_stg_prepago.STG_PP_RECARGA_REC
(
DH_EXTRACAO timestamp,
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
NSU_REQUISICAO string,
ID_RECARGA string,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
CANAL_RECARGA string,
SUB_CANAL string,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_CAMPANHA string,
CD_TIPO_PRODUTO string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
FLAG_ACEITE_UPSELL string,
FLAG_UPSELL_HABILITADO string,
VALOR_ORIGINAL string,
MEIO_PAGAMENTO string,
MSISDN_PAGADOR string,
CD_CELL_ID string,
CD_AREA_CELL_ID string,
ID_PLANO_FAT string,
NUMERO_PEDIDO string,
DATA_SOLIC_ORDEM_VENDA timestamp,
NUMERO_FATURA string,
DATA_HORA_FATURA timestamp,
CNPJ_EMISSOR string,
NU_SERIE string,
NU_VOUCHER string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table dl_stg_prepago.STG_PP_RECARGA_FIS
(
DH_EXTRACAO timestamp,
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
NSU_REQUISICAO string,
ID_RECARGA string,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
CANAL_RECARGA string,
SUB_CANAL string,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_CAMPANHA string,
CD_TIPO_PRODUTO string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
FLAG_ACEITE_UPSELL string,
FLAG_UPSELL_HABILITADO string,
VALOR_ORIGINAL string,
MEIO_PAGAMENTO string,
MSISDN_PAGADOR string,
CD_CELL_ID string,
CD_AREA_CELL_ID string,
ID_PLANO_FAT string,
NUMERO_PEDIDO string,
DATA_SOLIC_ORDEM_VENDA timestamp,
NUMERO_FATURA string,
DATA_HORA_FATURA timestamp,
CNPJ_EMISSOR string,
NU_SERIE string,
NU_VOUCHER string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table sva.TB_RECARGA_REC
(
DH_EXTRACAO timestamp,
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
NSU_REQUISICAO string,
ID_RECARGA string,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
CANAL_RECARGA string,
SUB_CANAL string,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_CAMPANHA string,
CD_TIPO_PRODUTO string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
FLAG_ACEITE_UPSELL string,
FLAG_UPSELL_HABILITADO string,
VALOR_ORIGINAL string,
MEIO_PAGAMENTO string,
MSISDN_PAGADOR string,
CD_CELL_ID string,
CD_AREA_CELL_ID string,
ID_PLANO_FAT string,
NUMERO_PEDIDO string,
DATA_SOLIC_ORDEM_VENDA timestamp,
NUMERO_FATURA string,
DATA_HORA_FATURA timestamp,
CNPJ_EMISSOR string,
NU_SERIE string,
NU_VOUCHER string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table sva.TB_RECARGA_FIS
(
DH_EXTRACAO timestamp,
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
NSU_REQUISICAO string,
ID_RECARGA string,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
CANAL_RECARGA string,
SUB_CANAL string,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_CAMPANHA string,
CD_TIPO_PRODUTO string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
FLAG_ACEITE_UPSELL string,
FLAG_UPSELL_HABILITADO string,
VALOR_ORIGINAL string,
MEIO_PAGAMENTO string,
MSISDN_PAGADOR string,
CD_CELL_ID string,
CD_AREA_CELL_ID string,
ID_PLANO_FAT string,
NUMERO_PEDIDO string,
DATA_SOLIC_ORDEM_VENDA timestamp,
NUMERO_FATURA string,
DATA_HORA_FATURA timestamp,
CNPJ_EMISSOR string,
NU_SERIE string,
NU_VOUCHER string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table sva.TB_RECARGA_CV
(
DH_EXTRACAO timestamp,
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
NSU_REQUISICAO string,
ID_RECARGA string,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
CANAL_RECARGA string,
SUB_CANAL string,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_CAMPANHA string,
CD_TIPO_PRODUTO string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
FLAG_ACEITE_UPSELL string,
FLAG_UPSELL_HABILITADO string,
VALOR_ORIGINAL string,
MEIO_PAGAMENTO string,
MSISDN_PAGADOR string,
CD_CELL_ID string,
CD_AREA_CELL_ID string,
ID_PLANO_FAT string,
NUMERO_PEDIDO string,
DATA_SOLIC_ORDEM_VENDA timestamp,
NUMERO_FATURA string,
DATA_HORA_FATURA timestamp,
CNPJ_EMISSOR string,
NU_SERIE string,
NU_VOUCHER string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS parquet ;

create table sva.TB_RECARGA_PS
(
DH_EXTRACAO timestamp,
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
NSU_REQUISICAO string,
ID_RECARGA string,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
CANAL_RECARGA string,
SUB_CANAL string,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_CAMPANHA string,
CD_TIPO_PRODUTO string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
FLAG_ACEITE_UPSELL string,
FLAG_UPSELL_HABILITADO string,
VALOR_ORIGINAL string,
MEIO_PAGAMENTO string,
MSISDN_PAGADOR string,
CD_CELL_ID string,
CD_AREA_CELL_ID string,
ID_PLANO_FAT string,
NUMERO_PEDIDO string,
DATA_SOLIC_ORDEM_VENDA timestamp,
NUMERO_FATURA string,
DATA_HORA_FATURA timestamp,
CNPJ_EMISSOR string,
NU_SERIE string,
NU_VOUCHER string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table dl_prepago.TB_DISTRIBUIDORES
(
DH_EXTRACAO timestamp,
CD_DISTRIBUIDOR_SAP string,
NO_FANTASIA string,
CD_CNPJ string,
CD_ORGANIZACAO_VENDA string,
DS_ORGANIZACAO_VENDA string,
CD_CANAL_SAP string,
DS_CANAL_SAP string,
CD_SETOR_ATIVIDADE string,
DS_SETOR_ATIVIDADE string,
DS_TP_DISTRIBUIDOR string,
DS_LOGRADOURO string,
NU_ENDERECO string,
DS_COMPLEMENTO string,
DS_CIDADE string,
DS_BAIRRO string,
DS_ESTADO string,
NU_CEP string,
IN_ELIMINACAO string,
DH_ELIMINACAO string,
DH_ATUALIZACAO string,
Modelo_de_Contrato string,
Centro_de_Distribuidor string,
Empresa string,
DT_EXECUCAO timestamp
)
STORED AS PARQUET ;

create table sva.TB_NFV_RECARGA_CV
(
DATA_EVENTO timestamp,
TIPO_REGISTRO string,
NUMERO_NF string,
NUM_ORDEM_VENDA string,
DATA_HORA_NF timestamp,
ID_ITEM_NF string,
VALOR_ITEM_NF decimal(10,2),
VALOR_NF decimal(10,2),
VALOR_MATERIAL decimal(10,2),
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
UF string,
TIPO_ORDEM string,
MATERIAL string,
DESC_MATERIAL string,
SERIE string,
SUBSERIE  string,
CFOP string,
NUM_PEDIDO string,
STATUS string,
IN_EVT_SISTEMICO_NFV boolean,
ID_RECARGA string,
NSU_REQUISICAO string,
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
DT_EFETIVACAO_RECARGA timestamp,
VALOR_DEBITADO decimal(10,2),
SALDO decimal(10,2),
TIPO_RECARGA string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
partitioned by (dt string)
stored as parquet;

create table sva.tb_nfv_recarga_ps
(
NUMERO_NF string,
NUM_ORDEM_VENDA string,
DATA_HORA_NF timestamp,
ID_ITEM_NF string,
VALOR_ITEM_NF decimal(10,2),
VALOR_NF decimal(10,2),
VALOR_MATERIAL decimal(10,2),
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
UF string,
TIPO_ORDEM string,
MATERIAL string,
DESC_MATERIAL string,
SERIE string,
SUBSERIE string,
CFOP string,
NUM_PEDIDO string,
STATUS string,      
ID_RECARGA string,
NSU_REQUISICAO string,
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
CANAL_RECARGA string,
SUB_CANAL string,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_CAMPANHA string,
CD_TIPO_PRODUTO string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
FLAG_ACEITE_UPSELL string,
FLAG_UPSELL_HABILITADO string,
VALOR_ORIGINAL string,
MEIO_PAGAMENTO string,
MSISDN_PAGADOR string,
CD_CELL_ID string,
CD_AREA_CELL_ID string,
NUMERO_PEDIDO string,
DATA_SOLIC_ORDEM_VENDA string,
ID_BOLSO string,
DATA_EXECUCAO timestamp
)
partitioned by (dt string)
stored as parquet;

create table sva.tb_aux_recarga_ps
(
DH_EXTRACAO timestamp,
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
NSU_REQUISICAO string,
ID_RECARGA string,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
CANAL_RECARGA string,
SUB_CANAL string,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_CAMPANHA string,
CD_TIPO_PRODUTO string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
FLAG_ACEITE_UPSELL string,
FLAG_UPSELL_HABILITADO string,
VALOR_ORIGINAL string,
MEIO_PAGAMENTO string,
MSISDN_PAGADOR string,
CD_CELL_ID string,
CD_AREA_CELL_ID string,
ID_PLANO_FAT string,
NUMERO_PEDIDO string,
DATA_SOLIC_ORDEM_VENDA timestamp,
NUMERO_FATURA string,
DATA_HORA_FATURA timestamp,
CNPJ_EMISSOR string,
NU_SERIE string,
NU_VOUCHER string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
partitioned by (dt string)
stored as parquet;

create table sva.tb_aux_nfv_fis
(
DH_EXTRACAO_SAP timestamp,
DH_EXTRACAO_INTEGRACAO string,
NUMERO_NF string,
NUM_ORDEM_VENDA string,
DATA_HORA_NF timestamp,
ID_ITEM_NF string,
VALOR_ITEM_NF decimal(10,2),
VALOR_NF decimal(10,2),
VALOR_MATERIAL decimal(10,2),
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
UF string,
TIPO_ORDEM string,
MATERIAL string,
DESC_MATERIAL string,
SERIE string,
SUBSERIE string,
CFOP string,
NUM_PEDIDO string,
NUMERO_NF_CAN string,
NUM_ORDEM_VENDA_CAN string,
DATA_HORA_NF_CAN string,
TIPO_ORDEM_CAN string,
STATUS string,
LISTA_SERIES_CAN array<string>,
LISTA_SERIES array<string>,
LISTA_SERIES_VAL array<string>,
VALOR_FACE decimal(10,2),
NU_SERIE string,
dt_execucao timestamp
)
partitioned by (dt string)
CLUSTERED BY(nu_serie) INTO 8 BUCKETS
stored as parquet;

create table dl_prepago.TB_NFC
(
DATA_HORA_CONSUMO timestamp,
DATA_HORA_NF string,
NUMERO_NF string,
HASH_CODE string,
SERIE string,
SUBSERIE string,
CFOP string,
MSISDN string,
NOME string,
CPF_CNPJ string,
IND_TELECOM string,
TIPO_CDR string,
TGC string,
ORIGEM string,
VALOR decimal(10,2),
VALOR_ICMS decimal(10,2),
ALIQUOTA string,
UF string,
ID_RECARGA string,
MSISDN_B string,
NUMERO_ITEM_NF string,
ID_CONSUMIDOR string,
DATA_HORA_EXTRACAO string,
TIPO_CONSUMO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET;

create table dl_prepago.tb_produto_recarga
(
DH_EXTRACAO timestamp,
CD_PRODUTO_RECARGA string,
NO_PRODUTO string,
DT_CRIACAO_PRODUTO timestamp,
DH_ATUALIZACAO timestamp,
DT_INICIO_VIGENCIA timestamp,
DT_FIM_VIGENCIA timestamp,
NU_VALIDADE string,
NU_VALOR_FACE string,
DS_TIPO_RECARGA string,
IN_MULTIPLO_USO string,
CD_DIREITO string,
NU_VALOR_DIREITO string,
TIPO_USO string,
VALOR_FACE_FINAL string,
PERCENTUAL string,
VALOR_CONTABIL string,
CATEG_CONTABIL string,
COD_ID_DEBITO string,
DT_EXECUCAO timestamp
)
stored as parquet;

create table dl_prepago.tb_nfc_oitv
(
DATA_HORA_CONSUMO timestamp,
DATA_NF string,
NUMERO_NF string,
HASH_CODE string,
SERIE string,
SUBSERIE string,
CFOP string,
MSISDN string,
NOME string,
CPF_CNPJ string,
VALOR_BOLSO decimal(10,2),
VALOR_NF decimal(10,2),
VALOR_ICMS decimal(10,2),
ALIQUOTA string,
UF string,
ID_CONSUMIDOR string,
ENCONTROU boolean,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET;

create table dl_prepago.TB_RECARGA_FATURA
(
DH_EXTRACAO timestamp,
NUMERO_FAT string,
MSISDN string,
DATA_HORA_REC timestamp,
DATA_HORA_FAT timestamp,
VALOR_RECARGA decimal(10,2),
NSU_REQUISICAO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (atualizou string,dt string)
STORED AS PARQUET;

create table dl_prepago.TB_AJUSTE
(
DH_EXTRACAO timestamp,
DT_AJUSTE timestamp,
LOGIN_OPERADOR string,
MSISDN string,
VALOR decimal(10,2),
NO_BOLSO string,
ID_RECARGA string,
NUM_DIAS_BONUS string,
MOTIVO_AJUSTE string,
PLANO_PRECO string,
CD_TIPO_PRODUTO string,
CD_PRODUTO string,
NR_SERIAL string,
STATUS string,
TP_RECARGA_CONCEDIDA string,
NU_REG_SIEBEL string,
SALDO decimal(10,2),
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET;

create table dl_stg_prepago.STG_PP_NFV_CF
(
DH_EXTRACAO_SAP timestamp,
DH_EXTRACAO_INTEGRACAO timestamp,
NUMERO_NF string,
NUM_ORDEM_VENDA string,
DATA_HORA_NF timestamp,
ID_ITEM_NF string,
VALOR_ITEM_NF decimal(10,2),
VALOR_NF decimal(10,2),
VALOR_MATERIAL decimal(10,2),
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
UF string,
TIPO_ORDEM string,
MATERIAL string,
DESC_MATERIAL string,
SERIE string,
SUBSERIE string,
CFOP string,
NUM_PEDIDO string,
NUMERO_NF_CAN string,
NUM_ORDEM_VENDA_CAN string,
DATA_HORA_NF_CAN string,
TIPO_ORDEM_CAN string,
STATUS string,
LISTA_SERIES_CAN array<string>,
LISTA_SERIES array<string>,
LISTA_SERIES_VAL array<string>,
VALOR_FACE decimal(10,2),
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS parquet;

create table dl_prepago.TB_LOG
(
DH_OCORRENCIA timestamp,
TIPO_OCORRENCIA string,
COD_OCORRENCIA string,
MODULO string,
MENSAGEM string,
NOME_ARQUIVO string
)
PARTITIONED BY (cod_programa string,dt string)
STORED AS textfile ;

create table dl_stg_prepago.STG_PP_NFV_NCF_CAN
(
DH_EXTRACAO_SAP timestamp,
DH_EXTRACAO_INTEGRACAO timestamp,
DATA_HORA_NF timestamp,
NUMERO_NF string,
NUM_ORDEM_VENDA string,
TIPO_ACAO string,
TIPO_ORDEM string,
NUMERO_NF_ORIGINAL string,
NUMERO_ORDEM_VENDA_ORIGINAL string,
CANCELOU boolean,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS parquet ;

create table dl_stg_prepago.STG_PP_NFV_CF_CAN
(
DH_EXTRACAO_SAP timestamp,
DH_EXTRACAO_INTEGRACAO timestamp,
DATA_HORA_NF timestamp,
NUMERO_NF string,
NUM_ORDEM_VENDA string,
TIPO_ACAO string,
TIPO_ORDEM string,
NUMERO_NF_ORIGINAL string,
NUMERO_ORDEM_VENDA_ORIGINAL string,
LISTA_SERIES array<string>,
CANCELOU boolean,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS parquet;

create table dl_stg_prepago.STG_PP_NFV_NCF
(
DH_EXTRACAO_SAP timestamp,
DH_EXTRACAO_INTEGRACAO timestamp,
NUMERO_NF string,
NUM_ORDEM_VENDA string,
DATA_HORA_NF timestamp,
ID_ITEM_NF string,
VALOR_ITEM_NF decimal(10,2),
VALOR_NF decimal(10,2),
VALOR_MATERIAL decimal(10,2),
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
UF string,
TIPO_ORDEM string,
MATERIAL string,
DESC_MATERIAL string,
SERIE string,
SUBSERIE string,
CFOP string,
NUM_PEDIDO string,
NUMERO_NF_CAN string,
NUM_ORDEM_VENDA_CAN string,
DATA_HORA_NF_CAN string,
TIPO_ORDEM_CAN string,
STATUS string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS parquet;

create table sva.TB_NFV_NCF
(
DH_EXTRACAO_SAP timestamp,
DH_EXTRACAO_INTEGRACAO timestamp,
NUMERO_NF string,
NUM_ORDEM_VENDA string,
DATA_HORA_NF timestamp,
ID_ITEM_NF string,
VALOR_ITEM_NF decimal(10,2),
VALOR_NF decimal(10,2),
VALOR_MATERIAL decimal(10,2),
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
UF string,
TIPO_ORDEM string,
MATERIAL string,
DESC_MATERIAL string,
SERIE string,
SUBSERIE string,
CFOP string,
NUM_PEDIDO string,
NUMERO_NF_CAN string,
NUM_ORDEM_VENDA_CAN string,
DATA_HORA_NF_CAN string,
TIPO_ORDEM_CAN string,
STATUS string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS parquet;

create table dl_prepago.TB_RECARGA_PEDIDO
(
DH_EXTRACAO timestamp,
TP_DOCUMENTO string,
COD_DISTRIBUIDOR_SAP string,
ORG_VENDAS string,
CANAL_DISTRIBUICAO string,
SETOR_ATIVIDADE string,
CENTRO string,
DATA_SOLIC_ORDEM_VENDA timestamp,
NUMERO_PEDIDO string,
MATERIAL string,
TP_CONDICAO string,
ID_RECARGA string,
CD_ORIGEM string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (atualizou string,dt string)
STORED AS PARQUET ;

create table sva.TB_AUX_NFV_CV			
(
VALOR_REMANESCENTE decimal(10,2),
NUMERO_NF string,
NUM_ORDEM_VENDA string,
DATA_HORA_NF timestamp,
ID_ITEM_NF string,
VALOR_ITEM_NF decimal(10,2),
VALOR_NF decimal(10,2),
VALOR_MATERIAL decimal(10,2),
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
UF string,
TIPO_ORDEM string,
MATERIAL string,
DESC_MATERIAL string,
SERIE string,
SUBSERIE string,
CFOP string,
NUM_PEDIDO string,
STATUS string,
IN_EVT_SISTEMICO_NFV boolean,
DT_EXECUCAO timestamp  
)
partitioned by (dt string)
stored as parquet;

create table dl_prepago.TB_GPRS
(
MSISDN string,
MSISDN_B string,
DT_INICIO timestamp,
DT_FIM string,
DURACAO string,
CD_PLANO_PRECO string,
TP_TARIFACAO string,
VL_CHAMADA decimal(10,2),
TP_PERIODO string,
TP_DIA_SEMANA string,
CD_ZONA_ORIGEM string,
DS_DESTINO string,
DS_ROAMING string,
CD_OPLD string,
TP_CHAMADA string,
VL_SALDO decimal(10,2),
AREA_TARIFACAO string,
CD_CATEGORIA string,
NO_BOLSO string,
AOS string,
VL_VOLUME_TRAFEGADO string,
CD_VELOCIDADE_ACESSO string,
IN_TIPO_VELOCIDADE string,
ddd_cell_id string,
cell_id string,
DT_EXTRACAO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
CLUSTERED BY(msisdn) INTO 16 BUCKETS
STORED AS PARQUET;

create table dl_prepago.TB_VOZ_FIXA
(
MSISDN string,
DT_INICIO timestamp,
TP_CHAMADA string,
DURACAO_REAL string,
DS_PLANO_PRECO string,
CD_OPLD string,
CD_PLANO_PRECO string,
CD_ZONA_ORIGEM_A string,
CD_ZONA_ORIGEM_B string,
TP_TARIFACAO string,
VL_CHAMADA decimal(10,2),
VL_SALDO decimal(10,2),
NO_BOLSO string,
DURACAO string,
TP_PERIODO string,
AREA_TARIFACAO string,
DS_DESTINO string,
DT_MSC string,
CD_CONCAT string,
MSISDN_B string,
ICMS string,
AOS string,
DT_EXTRACAO timestamp,
DT_FIM string,
CD_CATEGORIA string,
CD_GRUPO string,
CD_TIPO_PRODUTO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table dl_prepago.TB_APROPRIACAO
(
MSISDN string,
DT_REQUISICAO timestamp,
VL_SALDO decimal(10,2),
TP_REQUISICAO string,
PLANO_PRECO string,
VL_EMPRESTIMO string,
DT_EXTRACAO timestamp,
NO_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table dl_stg_prepago.STG_PP_TAXAS
(
MSISDN string,
VL_DIREITO decimal(10,2),
DH_SOLICITACAO timestamp,
ID_RECARGA string,
CD_TP_VALOR string,
DH_EFETIVACAO timestamp,
CD_ERRO string,
CD_TP_REQUISICAO string, 
CD_ID_DEBITO string,
NU_CANAL string, 
DH_EXTRACAO timestamp,
VL_SALDO decimal(10,2),
CD_PLANO_PRECO string,
CD_TIPO_PRODUTO string,
CD_PRODUTO_RECARGA string,
CD_DIREITO string
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table dl_prepago.TB_TAXAS_BLL
(
MSISDN string,
VALOR decimal(10,2),
DT_REQUISICAO timestamp,
NSU_REQUISICAO string,
TP_VALOR string,
DT_PROCESSAMENTO timestamp,
CD_ERRO string,
TP_REQUISICAO string,
CD_ORIGEM string,
CD_CANAL string,
DT_EXTRACAO timestamp,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_TIPO_PRODUTO string,
NO_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table sva.TB_NFV_CF
(
DH_EXTRACAO_SAP timestamp,
DH_EXTRACAO_INTEGRACAO timestamp,
NUMERO_NF string,
NUM_ORDEM_VENDA string,
DATA_HORA_NF timestamp,
ID_ITEM_NF string,
VALOR_ITEM_NF decimal(10,2),
VALOR_NF decimal(10,2),
VALOR_MATERIAL decimal(10,2),
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
UF string,
TIPO_ORDEM string,
MATERIAL string,
DESC_MATERIAL string,
SERIE string,
SUBSERIE string,
CFOP string,
NUM_PEDIDO string,
NUMERO_NF_CAN string,
NUM_ORDEM_VENDA_CAN string,
DATA_HORA_NF_CAN string,
TIPO_ORDEM_CAN string,
STATUS string,
LISTA_SERIES_CAN array<string>,
LISTA_SERIES array<string>,
LISTA_SERIES_VAL array<string>,
VALOR_FACE decimal(10,2),
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS parquet;

create table sva.TB_NFV_RECARGA_FIS
(
NUMERO_NF string,
NUM_ORDEM_VENDA string,
DATA_HORA_NF timestamp,
ID_ITEM_NF string,
VALOR_ITEM_NF decimal(10,2),
VALOR_NF decimal(10,2),
VALOR_MATERIAL decimal(10,2),
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
UF string,
TIPO_ORDEM string,
LISTA_SERIES array<string>,
VALOR_FACE_NFV decimal(10,2),
MATERIAL string,
DESC_MATERIAL string,
SERIE string,
SUBSERIE string,
CFOP string,
NUM_PEDIDO string,
STATUS string,      
ID_RECARGA string,
NSU_REQUISICAO string,
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
CANAL_RECARGA string,
SUB_CANAL string,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_CAMPANHA string,
CD_TIPO_PRODUTO string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
FLAG_ACEITE_UPSELL string,
FLAG_UPSELL_HABILITADO string,
VALOR_ORIGINAL string,
MEIO_PAGAMENTO string,
MSISDN_PAGADOR string,
NU_SERIE string,
NU_VOUCHER string,
CD_CELL_ID string,
CD_AREA_CELL_ID string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
partitioned by (dt string)
stored as parquet;

CREATE TABLE  dl_stg_prepago.STG_PP_MMS
     (
      MSISDN string,
      MSISDN_B string,
      DT_INICIO timestamp,
      DT_FIM string,
      DURACAO string,
      CD_PLANO_PRECO string,
      TP_TARIFACAO string,
      VL_CHAMADA decimal(10,2),
      TP_PERIODO string,
      TP_DIA_SEMANA string,
      CD_ZONA_ORIGEM string,
      DS_DESTINO string,
      DS_ROAMING string,
      CD_OPLD string,
      TP_CHAMADA string,
      VL_SALDO decimal(10,2),
      AREA_TARIFACAO string,
      CD_CATEGORIA string,
      NO_BOLSO string,
      AOS string,
      DT_EXTRACAO string
       )
PARTITIONED BY (dt string) 
STORED AS PARQUET;

create table dl_prepago.tb_voz
(
MSISDN string,
msisdn_b string,
dt_inicio timestamp,
dt_fim string,
duracao string,
cd_plano_preco string,
tp_tarifacao string,
vl_chamada decimal(10,2),
tp_periodo string,
tp_dia_semana string,
cd_zona_origem string,
ds_destino string,
ds_roaming string,
cd_opld string,
tp_chamada string,
vl_saldo decimal(10,2),
area_tarifacao string,
cd_categoria string,
ds_acobrar string,
dt_msc string,
cd_concat string,
duracao_real string,
c_opld_b string,
operadora string,
msisdn_a string,
calling_cat string,
no_bolso string,
aos string,
cd_tipo_produto string,
ddd_cell_id string,
cell_id string,
dt_extracao string,
dt_execucao timestamp
)
PARTITIONED BY (dt string)
CLUSTERED BY(msisdn) INTO 16 BUCKETS
STORED AS PARQUET;

create table sva.tb_nfc_uso
(
NU_NF string,
NU_SERIE string,
NU_SUBSERIE string,
DT_HORA_NF timestamp,
CD_CFOP string,
CD_HASH_CODE string,
NU_ITEM_NF string,
MSISDN string,
DT_HORA_CONSUMO timestamp,
NO_CLIENTE string,
CD_CPF_CNPJ string,
IN_TELECOM string,
CD_TIPO_CDR string,
CD_TGC string,
CD_ORIGEM string,
MSISDN_B string,
VL_CONSUMO_NF decimal(10,2),
VL_ICMS_NF decimal(10,2),
NU_ALIQUOTA_NF string,
SG_UF string,
ID_RECARGA string,
ID_CONSUMIDOR string,
TIPO_CONSUMO string,
IN_EXISTE_NFC boolean,
CD_TIPO_USO string,
MSISDN_B_USO string,
DT_INICIO_USO timestamp,
DT_FIM_USO string,
TP_TARIFACAO_USO string,
CD_ORIGEM_USO string,
VL_CHAMADA decimal(10,2),
VL_SALDO decimal(10,2),
NO_BOLSO string,
IN_EXISTE_USO boolean,
DT_EXECUCAO timestamp,
NF_MODELO string
)
partitioned by (dt string, grupo_uso string)
stored as parquet;

CREATE TABLE dl_prepago.tb_sms 
     (
      MSISDN string,
      MSISDN_B string,
      DT_INICIO timestamp,
      DT_FIM string,
      DURACAO string,
      CD_PLANO_PRECO string,
      TP_TARIFACAO string,
      VL_CHAMADA decimal(10,2),
      TP_PERIODO string,
      TP_DIA_SEMANA string,
      CD_ZONA_ORIGEM string,
      DS_DESTINO string,
      DS_ROAMING string,
      CD_OPLD string,
      TP_CHAMADA string,
      VL_SALDO decimal(10,2),
      AREA_TARIFACAO string,
      CD_CATEGORIA string,
      NO_BOLSO int,
      AOS string,
      DT_EXTRACAO string,
      DT_EXECUCAO timestamp
      )
PARTITIONED BY (dt string) 
STORED AS PARQUET;

CREATE TABLE  dl_prepago.tb_mms
     (
      MSISDN string,
      MSISDN_B string,
      DT_INICIO timestamp,
      DT_FIM string,
      DURACAO string,
      CD_PLANO_PRECO string,
      TP_TARIFACAO string,
      VL_CHAMADA decimal(10,2),
      TP_PERIODO string,
      TP_DIA_SEMANA string,
      CD_ZONA_ORIGEM string,
      DS_DESTINO string,
      DS_ROAMING string,
      CD_OPLD string,
      TP_CHAMADA string,
      VL_SALDO decimal(10,2),
      AREA_TARIFACAO string,
      CD_CATEGORIA string,
      NO_BOLSO string,
      AOS string,
      DT_EXTRACAO string,
      DT_EXECUCAO timestamp
       )
PARTITIONED BY (dt string) 
STORED AS PARQUET;

create table dl_prepago.tb_taxas
(
MSISDN string,
VL_DIREITO decimal(10,2),
DH_SOLICITACAO timestamp,
ID_RECARGA string,
DH_EFETIVACAO timestamp,
CD_ID_DEBITO string,
DH_EXTRACAO timestamp,
VL_SALDO decimal(10,2),
CD_PLANO_PRECO string,
CD_TIPO_PRODUTO string,
CD_PRODUTO_RECARGA string,
CD_DIREITO string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table dl_prepago.tb_saldo_distribuidor
(
CD_DISTRIBUIDOR_SAP string,
CD_ORGANIZACAO_VENDA string,
CD_CANAL_SAP string,
CD_SETOR_ATIVIDADE string,
VL_SALDO decimal(10,2),
DH_EXTRACAO timestamp,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS parquet;

create table dl_prepago.tb_saldo_cliente
(
MSISDN string,
STATUS string,
PLANO_PRECO string,
DT_ATIVACAO timestamp,
DT_EXPIRACAO_CREDITO timestamp,
DT_DESATIVACAO timestamp,
CD_OPLD string,
SALDOATUAL decimal(10,2),
DT_LAD timestamp,
STATUS_DEDUZIDO string,
DT_MOVIMENTO timestamp,
DT_ULTIMA_RECARGA timestamp,
FG_EMPRESTIMO string,
QT_SALDO_OI_PREDILETO string,
VL_REC_MES_ANTERIOR string,
VL_REC_MES_ATUAL string,
SALDOFRANQUIA string,
VL_TAXA_PENDENTE string,
DT_TAXA_PENDENTE string,
IND_LIGADOR string,
ID_BENEFICIO_IN string,
ID_PROGRAMA_IN string,
CD_TIPO_PRODUTO string,
QTE_VEZES_NSE string,
DT_ADESAO_OFERTA_IN timestamp,
NODE_IN string,
DT_EXPIRACAO_OFERTA_IN timestamp,
DS_PLANO_PRECO_FIXA string,
DT_BOLSO_FRANQUIA_CTRL timestamp,
SALDO_FRANQUIA_CTRL decimal(10,2),
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS parquet;

create table sva.tb_aux_recarga
(
SALDO_RM_REC decimal(10,2),
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
NSU_REQUISICAO string,
ID_RECARGA string,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
SALDO_CLIENTE decimal(10,2),
TIPO_RECARGA string,
CD_PONTO_VENDA string,
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
NUMERO_PEDIDO string,
DATA_SOLIC_ORDEM_VENDA timestamp,
NUMERO_FATURA string,
DATA_HORA_FATURA timestamp,
CNPJ_EMISSOR string,
NU_SERIE string,
NU_VOUCHER string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
CLUSTERED BY(msisdn) INTO 16 BUCKETS
STORED AS PARQUET ;

create table sva.tb_nfc_uso_recarga
(
DT_EVENTO timestamp,
DT_HORA_CONSUMO timestamp,
MSISDN string,
NU_NF string,
NU_SERIE string,
NU_SUBSERIE string,
DT_HORA_NF timestamp,
CD_CFOP string,
CD_HASH_CODE string,
NU_ITEM_NF string,
NO_CLIENTE string,
CD_CPF_CNPJ string,
IN_TELECOM string,
CD_TIPO_CDR string,
CD_TGC string,
CD_ORIGEM string,
MSISDN_B string,
VL_ICMS_NF string,
NU_ALIQUOTA_NF string,
SG_UF string,
ID_CONSUMIDOR string,
TIPO_CONSUMO string,
IN_EXISTE_NFC boolean,
CD_TIPO_USO string,
MSISDN_B_USO string,
DT_INICIO_USO timestamp,
DT_FIM_USO string,
TP_TARIFACAO_USO string,
CD_ORIGEM_USO string,
NO_BOLSO string,
IN_EXISTE_USO boolean,
DT_SOLICITACAO_RECARGA timestamp,
NSU_REQUISICAO string,
ID_RECARGA string,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
NUMERO_PEDIDO string,
DATA_SOLIC_ORDEM_VENDA timestamp,
NUMERO_FATURA string,
DATA_HORA_FATURA timestamp,
CNPJ_EMISSOR string,
NU_SERIE_CARTAO string,
NU_VOUCHER string,
TIPO_REGISTRO string,
IN_EVT_SISTEMICO_REC boolean,
VL_CHAMADA decimal(10,2),
VL_CONSUMO_NF decimal(10,2),
VL_SALDO decimal(10,2),
VL_SALDO_CLI_CALCULADO decimal(10,2),
VL_SALDO_RECARGA decimal(10,2),
VL_DEBITADO_RECARGA decimal(10,2),
VALOR_FACE decimal(10,2),
VL_AJUSTE_SISTEMICO decimal(10,2),
DT_EXECUCAO timestamp,
NF_MODELO string
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table sva.tb_nfc_nfv
(
TIPO_DE_EVENTO string,
DATA_DO_EVENTO timestamp,
EMPRESA_EMISSORA string,
DATA_EMISSAO_NF_CONSUMO timestamp,
NOTA_FISCAL_CONSUMO string,
NU_SERIE_NF_CONSUMO string,
NU_SUBSERIE_NF_CONSUMO string,
CD_CFOP_NF_CONSUMO string,
CD_HASH_CODE_NF_CONSUMO string,
NU_ITEM_NF_CONSUMO string,
CD_TIPO_CONSUMO string,
IN_TELECOM string,
CD_TIPO_CDR string,
CD_TGC string,
CD_ORIGEM string,
DATA_CONSUMO timestamp,
VL_CONSUMO_NF decimal(10,2),
NU_ALIQUOTA_NF string,
VL_ICMS_NF decimal(10,2),
VALOR_CONSUMIDO_DA_RECARGA decimal(10,2),
SALDO_FINAL_CLIENTE decimal(10,2),
ID_CONSUMIDOR string,
SG_UF_CLIENTE string,
CD_CPF_CNPJ_CLIENTE string,
NO_CLIENTE string,
MSISDN string,
IN_EVT_SISTEMICO_RECARGA boolean,
MODALIDADE_RECARGA string,
ID_RECARGA string,
PRODUTO_RECARGA string,
TIPO_RECARGA string,
NU_SERIE string,                
NU_VOUCHER string,
DT_SOLICITACAO_RECARGA timestamp,
VL_SALDO_RECARGA decimal(10,2),
VALOR_RECARGA decimal(10,2),
VALOR_DEBITADO decimal(10,2),
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
UF_NF_VENDA string,
NOME_DISTRIBUIDOR string,
UF_DISTRIBUIDOR string,
CD_CNPJ_CPF_DISTRIBUIDOR string,
DATA_HORA_NF_VENDA timestamp,
NUMERO_NF_VENDA string,
SERIE_NF_VENDA string,
SUBSERIE_NF_VENDA string,
CFOP_NF_VENDA string,
VALOR_NF decimal(10,2),
TIPO_DE_ORDEM string,
MATERIAL string,
DESC_MATERIAL string,
NUM_ORDEM_DE_VENDA string,
SALDO_FINAL_DISTRIBUIDOR decimal(10,2),
NF_MODELO string,
IN_EXISTE_USO boolean,
IN_EXISTE_NFC boolean,
IN_EVT_SISTEMICO_NFV boolean,
NO_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (ano_mes string,dt string)
STORED AS PARQUET;

create table dl_stg_prepago.stg_pp_nfv_can
(
DH_EXTRACAO_SAP timestamp,
DH_EXTRACAO_INTEGRACAO string,
DATA_HORA_NF timestamp,
NUMERO_NF string,
NUM_ORDEM_VENDA string,
TIPO_ACAO string,
TIPO_ORDEM string,
NUMERO_NF_ORIGINAL string,
NUMERO_ORDEM_VENDA_ORIGINAL string,
CANCELOU boolean,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS parquet ;

create table dl_prepago.tb_recarga
(
DH_EXTRACAO timestamp, 
MSISDN string,
VALOR_FACE decimal(10,2),
DT_SOLICITACAO_RECARGA timestamp,
NSU_REQUISICAO string,
ID_RECARGA string,
DT_EFETIVACAO_RECARGA timestamp,
PRODUTO_RECARGA string,
CANAL_RECARGA string,
SUB_CANAL string,
SALDO decimal(10,2),
CD_PRODUTO string,
CD_CAMPANHA string,
CD_TIPO_PRODUTO string,
TIPO_RECARGA string,
CD_PONTO_VENDA string,
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
FLAG_ACEITE_UPSELL string,
FLAG_UPSELL_HABILITADO string,
VALOR_ORIGINAL string,
MEIO_PAGAMENTO string,
MSISDN_PAGADOR string,
CD_CELL_ID string,
CD_AREA_CELL_ID string,
NUMERO_FATURA string,
DATA_HORA_FATURA timestamp,
CNPJ_EMISSOR string,
NU_SERIE string,
NU_VOUCHER string,
ID_BOLSO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET ;

create table dl_prepago.tb_nfv
(
DH_EXTRACAO_SAP timestamp,
DH_EXTRACAO_INTEGRACAO timestamp,
NUMERO_NF string,
NUM_ORDEM_VENDA string,
DATA_HORA_NF timestamp,
ID_ITEM_NF string,
VALOR_ITEM_NF decimal(10,2),
VALOR_NF decimal(10,2),
VALOR_MATERIAL decimal(10,2),
COD_DISTRIBUIDOR_SAP string,
ORGANIZACAO_VENDA string,
SETOR_ATIVIDADE string,
CANAL_DISTRIBUICAO string,
UF string,
TIPO_ORDEM string,
NUM_SERIE_INICIAL string,
NUM_SERIE_FINAL string,
MATERIAL string,
DESC_MATERIAL string,
SERIE string,
SUBSERIE string,
CFOP string,
NUM_PEDIDO string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS parquet;

create table dl_prepago.tb_nfv_can
(
DH_EXTRACAO_SAP timestamp,
DH_EXTRACAO_INTEGRACAO string,
DATA_HORA_NF timestamp,
NUMERO_NF string,
NUM_ORDEM_VENDA string,
TIPO_ACAO string,
TIPO_ORDEM string,
NUMERO_NF_ORIGINAL string,
NUMERO_ORDEM_VENDA_ORIGINAL string,
NUM_SERIE_INICIAL string, 
NUM_SERIE_FINAL string,
MANDT string,
DT_EXECUCAO timestamp
)
PARTITIONED BY (dt string)
STORED AS parquet;
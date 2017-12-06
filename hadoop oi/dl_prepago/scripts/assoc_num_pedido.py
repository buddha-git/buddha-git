from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext, Row
from pyspark.sql.types import * 
import sys

conf = SparkConf()

sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

data_processa = sys.argv[2]
conf_path = sys.argv[1] 

sc.addPyFile(conf_path+"/conf_assoc_num_pedido.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_assoc_num_pedido import *

for chave in sqlContext_conf_list:
	sqlContext.setConf(chave, sqlContext_conf_list[chave])

dt_execucao = str(datetime.now()).split(".")[0] 
dt_execucao_formatada = dt_execucao.replace("-","").replace(":","").replace(" ","").split(".")[0] 

def grava_log(tp_ocorrencia,cod_ocorrencia,mensagem):
	dh_ocorrencia = dt_execucao
	nome_arquivo = arquivo
	dt = str(dh_ocorrencia)[:10]
	dup_rdd = sc.parallelize([[dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo, cod_programa, dt]])
	dup_df = sqlContext.createDataFrame(dup_rdd).withColumnRenamed("_7","cod_programa").withColumnRenamed("_8","dt")
	dup_df.write.mode("append").partitionBy("cod_programa","dt").saveAsTable(db_prepago+".TB_LOG", format="parquet")


def registra_erros(rej_rdd, nome_campo, tipo_erro):
	n_rej = rej_rdd.count()
	if n_rej > 0:
		if tipo_erro == "REGISTRO_ANTIGO":
			mensagem = str(n_rej) + " Registros Antigos"
		elif tipo_erro == "REGISTRO_FORA_TEMPO_GUARDA":
			mensagem = str(n_rej) + " Registros fora do tempo de guarda"
		else:
			mensagem = str(n_rej) + " Registros com o campo "+nome_campo+" invalido"
		grava_log("ERRO",cod_programa + cod_erro[tipo_erro],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
		grava_rej(rej_rdd, dir_rejeicao+arquivo+"."+dt_execucao_formatada+"."+cod_programa + cod_erro[tipo_erro])

def to_tb_log(linha):
	#[dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo,dt]
	dh_ocorrencia = dt_execucao
	
	tp_ocorrencia = "ERRO"
	
	cod_ocorrencia = cod_programa + cod_erro["LISTA_PEDIDOS_NAO_ENCONTRARAM_ID_RECARGA"]
	
	mensagem = "Recarga "+linha[0]+" nao encontrada para associacao ao pedido "+linha[46]
	
	nome_arquivo = arquivo
	
	dt = str(dh_ocorrencia)[:10]
	
	return (dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo, cod_programa,dt)


fieldsLog = [StructField(field_name, StringType(), True) for field_name in schemaLog.split(';')]

fieldsLog[0].dataType = TimestampType()

schemaLog = StructType(fieldsLog)

conf.setAppName(nome)

#schema_for01 = "DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,PRODUTO_RECARGA,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,VALOR_ORIGINAL,MEIO_PAGAMENTO,MSISDN_PAGADOR,CD_CELL_ID,CD_AREA_CELL_ID,ID_PLANO_FAT,NUMERO_PEDIDO,DATA_SOLIC_ORDEM_VENDA,NUMERO_FATURA,DATA_HORA_FATURA,CNPJ_EMISSOR,NU_SERIE,NU_VOUCHER,ID_BOLSO,DT_EXECUCAO,dt"

schema_for01 = "DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,\
if(TIPO_RECARGA='ELT',NSU_REQUISICAO,substr(ID_RECARGA,length(ID_RECARGA)-5,6)) as ID_RECARGA_JOIN, \
if(TIPO_RECARGA='ELT','0',PRODUTO_RECARGA) as CD_ORIGEM_JOIN, \
NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,\
PRODUTO_RECARGA as CD_ORIGEM,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,\
COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,VALOR_ORIGINAL,\
MEIO_PAGAMENTO,MSISDN_PAGADOR,CD_CELL_ID,CD_AREA_CELL_ID,ID_PLANO_FAT,NUMERO_PEDIDO,DATA_SOLIC_ORDEM_VENDA,NUMERO_FATURA,DATA_HORA_FATURA,\
CNPJ_EMISSOR,NU_SERIE,NU_VOUCHER,ID_BOLSO,DT_EXECUCAO,dt"

schema_for02 = "DH_EXTRACAO,TP_DOCUMENTO,COD_DISTRIBUIDOR_SAP,ORG_VENDAS,CANAL_DISTRIBUICAO,SETOR_ATIVIDADE,\
 CENTRO,DATA_SOLIC_ORDEM_VENDA,NUMERO_PEDIDO,MATERIAL,TP_CONDICAO, \
 ID_RECARGA,CD_ORIGEM, \
 ID_RECARGA as ID_RECARGA_JOIN, CD_ORIGEM as CD_ORIGEM_JOIN, \
 DT_EXECUCAO,atualizou,dt"

rec_ps_df = sqlContext.sql("select "+schema_for01+" from " +db_stg+".stg_pp_RECARGA_PS")

ped_df = sqlContext.sql("select "+schema_for02+" from " +db_prepago+".TB_RECARGA_PEDIDO where atualizou = 0").withColumnRenamed("DH_EXTRACAO", "DH_EXTRACAO02").withColumnRenamed("COD_DISTRIBUIDOR_SAP", "COD_DISTRIBUIDOR_SAP02").withColumnRenamed("CANAL_DISTRIBUICAO", "CANAL_DISTRIBUICAO02").withColumnRenamed("SETOR_ATIVIDADE", "SETOR_ATIVIDADE02").withColumnRenamed("dt", "dt02").withColumnRenamed("DATA_SOLIC_ORDEM_VENDA","DATA_SOLIC_ORDEM_VENDA02").withColumnRenamed("NUMERO_PEDIDO","NUMERO_PEDIDO02").withColumnRenamed("ID_RECARGA","ID_RECARGA02").withColumnRenamed("CD_ORIGEM","CD_ORIGEM02").withColumnRenamed("DT_EXECUCAO","DT_EXECUCAO02")

join_df = rec_ps_df.join(ped_df, ["ID_RECARGA_JOIN","CD_ORIGEM_JOIN"], "fullouter")

join_so_ped = join_df.filter("MSISDN is null")

so_ped_final = join_so_ped.select(["DH_EXTRACAO02","TP_DOCUMENTO","COD_DISTRIBUIDOR_SAP02","ORG_VENDAS",\
"CANAL_DISTRIBUICAO02","SETOR_ATIVIDADE02","CENTRO","DATA_SOLIC_ORDEM_VENDA02","NUMERO_PEDIDO02","MATERIAL",\
"TP_CONDICAO","ID_RECARGA02","CD_ORIGEM02","DT_EXECUCAO02","atualizou","dt02"]).withColumnRenamed("dt02","dt")

join_so_rec = join_df.filter("NUMERO_PEDIDO02 is null")

#join_so_rec.registerTempTable("join_so_rec")

#join_so_rec_final = join_so_rec.select([name for name in schema_for01.split(",")])
join_so_rec_final = join_so_rec.select(["DH_EXTRACAO","MSISDN","VALOR_FACE","DT_SOLICITACAO_RECARGA","NSU_REQUISICAO",\
"ID_RECARGA","DT_EFETIVACAO_RECARGA","CD_ORIGEM","CANAL_RECARGA","SUB_CANAL","SALDO","CD_PRODUTO","CD_CAMPANHA","CD_TIPO_PRODUTO",\
"TIPO_RECARGA","CD_PONTO_VENDA","COD_DISTRIBUIDOR_SAP","ORGANIZACAO_VENDA","SETOR_ATIVIDADE","CANAL_DISTRIBUICAO","FLAG_ACEITE_UPSELL",\
"FLAG_UPSELL_HABILITADO","VALOR_ORIGINAL","MEIO_PAGAMENTO","MSISDN_PAGADOR","CD_CELL_ID","CD_AREA_CELL_ID","ID_PLANO_FAT","NUMERO_PEDIDO",\
"DATA_SOLIC_ORDEM_VENDA","NUMERO_FATURA","DATA_HORA_FATURA","CNPJ_EMISSOR","NU_SERIE","NU_VOUCHER","ID_BOLSO","DT_EXECUCAO","dt"])

join_feito = join_df.filter("NUMERO_PEDIDO02 is not null and MSISDN is not null")

join_feito.registerTempTable("join_feito")

#rec_ps_atualizado = sqlContext.sql("select DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,PRODUTO_RECARGA,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,NUMERO_FATURA,DATA_HORA_FATURA,CNPJ_EMISSOR,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,VALOR_ORIGINAL,MEIO_PAGAMENTO,MSISDN_PAGADOR,NU_SERIE,NU_VOUCHER,CD_CELL_ID,CD_AREA_CELL_ID,ID_PLANO_FAT,NUMERO_PEDIDO02,DATA_SOLIC_ORDEM_VENDA02,dt from join_feito")

rec_ps_atualizado = sqlContext.sql("select DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,\
ID_RECARGA,DT_EFETIVACAO_RECARGA,CD_ORIGEM,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,\
TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,FLAG_ACEITE_UPSELL,\
FLAG_UPSELL_HABILITADO,VALOR_ORIGINAL,MEIO_PAGAMENTO,MSISDN_PAGADOR,CD_CELL_ID,CD_AREA_CELL_ID,ID_PLANO_FAT,NUMERO_PEDIDO02,\
DATA_SOLIC_ORDEM_VENDA02,NUMERO_FATURA,DATA_HORA_FATURA,CNPJ_EMISSOR,NU_SERIE,NU_VOUCHER,ID_BOLSO,DT_EXECUCAO,dt from join_feito")

rec_ps_final = rec_ps_atualizado.unionAll(join_so_rec_final)

rec_ps_final.registerTempTable("rec_ps_final")

join_so_ped_mapped = join_so_ped.map(to_tb_log)

if join_so_ped_mapped.count() > 0:
	lista_so_pedidos = sqlContext.createDataFrame(join_so_ped_mapped).withColumnRenamed("_8", "dt").withColumnRenamed("_7", "cod_programa")
	
	lista_so_pedidos.write.mode("append").partitionBy("cod_programa","dt").saveAsTable(db_prepago+".tb_log", format="parquet")

sqlContext.sql(create_TMP_TB_RECARGA_PEDIDO)

#ped_ja_atualizaram = sqlContext.sql("select "+schema_for02+" from " +db_prepago+".TB_RECARGA_PEDIDO where atualizou = 1")

ped_atualizaram_hoje = sqlContext.sql("select DH_EXTRACAO02,TP_DOCUMENTO,COD_DISTRIBUIDOR_SAP02,ORG_VENDAS,\
CANAL_DISTRIBUICAO02,SETOR_ATIVIDADE02,CENTRO,DATA_SOLIC_ORDEM_VENDA02,NUMERO_PEDIDO02,MATERIAL,TP_CONDICAO,\
ID_RECARGA02,CD_ORIGEM02,DT_EXECUCAO02,1 as atualizou,dt02 from join_feito")

ped_nao_atualizaram = so_ped_final

ped_nao_atualizaram = ped_nao_atualizaram.cache()
ped_nao_atualizaram.count() #checkpoint

ped_atualizaram_hoje = ped_atualizaram_hoje.cache()
ped_atualizaram_hoje.count() #checkpoint

#ped_ja_atualizaram = ped_ja_atualizaram.cache()
#ped_ja_atualizaram.count() #checkpoint

ped_df_final = ped_nao_atualizaram.unionAll(ped_atualizaram_hoje)#.unionAll(ped_ja_atualizaram)

ped_df_final.write.mode("append").partitionBy("atualizou","dt").saveAsTable(db_stg+".TMP_TB_RECARGA_PEDIDO", format="parquet")

#sqlContext.sql("insert overwrite table "+db_stg+".stg_pp_RECARGA_PS partition (dt) select * from rec_ps_final")
sqlContext.sql("create table {db_stg}.TMP_STG_PP_RECARGA_PS like {db_stg}.STG_PP_RECARGA_PS".format(db_stg = db_stg))

sqlContext.sql("insert into table "+db_stg+".TMP_STG_PP_RECARGA_PS partition (dt) select * from rec_ps_final")

sqlContext.sql("drop table "+db_stg+".STG_PP_RECARGA_PS");

sqlContext.sql("alter table {db}.TMP_STG_PP_RECARGA_PS rename to {db}.STG_PP_RECARGA_PS".format(db = db_stg))

#sqlContext.sql("drop table "+db_prepago+".TB_RECARGA_PEDIDO")
sqlContext.sql("alter table "+db_prepago+".TB_RECARGA_PEDIDO drop partition (atualizou = 0)")

#sqlContext.sql("ALTER TABLE "+db_stg+".TMP_TB_RECARGA_PEDIDO RENAME TO "+db_prepago+".TB_RECARGA_PEDIDO")
sqlContext.sql("insert into table "+db_prepago+".TB_RECARGA_PEDIDO partition(atualizou,dt) select * from "+db_stg+".TMP_TB_RECARGA_PEDIDO")

sqlContext.sql("drop table "+db_stg+".TMP_TB_RECARGA_PEDIDO")

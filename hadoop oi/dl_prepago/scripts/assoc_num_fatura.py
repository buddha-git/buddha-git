###   Nome do programa: assoc_num_fatura
###
###   Programa responsavel atualizacao das recargas da tabela hive STG_PP_RECARGA_REC com os numeros de fatura provenientes do Arbor
###
###   Os erros geram registros na tabela de log com a data de ocorrencia

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext, Row
from pyspark.sql.types import * 
from pyspark import SparkFiles
import sys

conf = SparkConf()

#conf.setMaster("yarn-client")

sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

data_processa = sys.argv[2]
conf_path = sys.argv[1] 

sc.addPyFile(conf_path+"/conf_assoc_num_fatura.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_assoc_num_fatura import *

for chave in sqlContext_conf_list:
	sqlContext.setConf(chave, sqlContext_conf_list[chave])

dt_execucao = str(datetime.now()).split(".")[0] 
		
##Realiza a gravacao de um registro na tabela de log

def grava_log(tp_ocorrencia,cod_ocorrencia,mensagem):
	dh_ocorrencia = dt_execucao
	nome_arquivo = arquivo
	dt = str(dh_ocorrencia)[:10]
	dup_rdd = sc.parallelize([[dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo, cod_programa, dt]])
	dup_df = sqlContext.createDataFrame(dup_rdd).withColumnRenamed("_7","cod_programa").withColumnRenamed("_8","dt")
	dup_df.write.mode("append").partitionBy("cod_programa","dt").saveAsTable(db_prepago+".TB_LOG", format="parquet")

##Registra na tabela de log e grava no diretorio de rejeicao erros encontrados

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
		grava_rej(rej_rdd, dir_rejeicao+arquivo+"."+cod_programa + cod_erro[tipo_erro])

def to_tb_log(linha):
	#[dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo,dt]
	dh_ocorrencia = dt_execucao
	
	tp_ocorrencia = "ERRO"
	
	cod_ocorrencia = cod_programa + cod_erro["LISTA_FATURAS_NAO_ENCONTRARAM_MSISDN_NSU_REQUISICAO"]
	
	mensagem = "Recarga com NSU "+linha[0]+" para o MSISDN "+linha[1]+" nao encontrada para associacao a fatura "+linha[39]
	
	nome_arquivo = arquivo
	
	dt = str(dh_ocorrencia)[:10]
	
	return (dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo,cod_programa,dt)


fieldsLog = [StructField(field_name, StringType(), True) for field_name in schemaLog.split(';')]

fieldsLog[0].dataType = TimestampType()

schemaLog = StructType(fieldsLog)

conf.setAppName(nome)

schema_for01 = "DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,PRODUTO_RECARGA,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,VALOR_ORIGINAL,MEIO_PAGAMENTO,MSISDN_PAGADOR,CD_CELL_ID,CD_AREA_CELL_ID,ID_PLANO_FAT,NUMERO_PEDIDO,DATA_SOLIC_ORDEM_VENDA,NUMERO_FATURA,DATA_HORA_FATURA,CNPJ_EMISSOR,NU_SERIE,NU_VOUCHER,ID_BOLSO,DT_EXECUCAO,dt"
#"DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,PRODUTO_RECARGA,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,NUMERO_FATURA,DATA_HORA_FATURA,CNPJ_EMISSOR,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,VALOR_ORIGINAL,MEIO_PAGAMENTO,MSISDN_PAGADOR,NU_SERIE,NU_VOUCHER,CD_CELL_ID,CD_AREA_CELL_ID,VALOR_FAT,ID_PLANO_FAT,NUMERO_PEDIDO,DATA_SOLIC_ORDEM_VENDA,dt"

schema_arbor = "DH_EXTRACAO02,NUMERO_FAT,MSISDN,DATA_HORA_REC,DATA_HORA_FAT,VALOR_RECARGA,NSU_REQUISICAO,DT_EXECUCAO02,atualizou,dt"

rec_rec_df = sqlContext.sql("select "+schema_for01+" from "+db_stg+".STG_PP_RECARGA_REC")

fat_df = sqlContext.sql("select DH_EXTRACAO,NUMERO_FAT,MSISDN,DATA_HORA_REC,DATA_HORA_FAT,VALOR_RECARGA,NSU_REQUISICAO,DT_EXECUCAO,atualizou,dt from "+db_prepago+".TB_RECARGA_FATURA where atualizou = false").withColumnRenamed("DH_EXTRACAO","DH_EXTRACAO02").withColumnRenamed("DT_EXECUCAO","DT_EXECUCAO02").withColumnRenamed("dt","dt02")#.withColumnRenamed("NSU_REQUISICAO","NSU_REQUISICAO02").withColumnRenamed("MSISDN","MSISDN02") nome dos campos nao alterado por conta do join

# Realiza join entre fatura e recarga por NSU_REQUISICAO e MSISDN

join_df = rec_rec_df.join(fat_df, ["NSU_REQUISICAO", "MSISDN"], "fullouter")

# Registros so de fatura

join_so_fat = join_df.filter("VALOR_FACE is null")

so_fat_final = join_so_fat.select([name for name in schema_arbor.split(",") if name != "dt"]+["dt02"]).withColumnRenamed("dt02","dt")

# Registros apenas de recarga

join_so_rec = join_df.filter("NUMERO_FAT is null") ##obs: numero_fat(arbor) <> numero_fatura(fortuna01)

join_so_rec_final = join_so_rec.select([name for name in schema_for01.split(",")])

# Join realizado

join_feito = join_df.filter("VALOR_FACE is not null and NUMERO_FAT is not null")

join_feito.registerTempTable("join_feito")

rec_rec_atualizado = sqlContext.sql("select DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,PRODUTO_RECARGA,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,VALOR_ORIGINAL,MEIO_PAGAMENTO,MSISDN_PAGADOR,CD_CELL_ID,CD_AREA_CELL_ID,ID_PLANO_FAT,NUMERO_PEDIDO,DATA_SOLIC_ORDEM_VENDA,NUMERO_FAT,DATA_HORA_FAT,CNPJ_EMISSOR,NU_SERIE,NU_VOUCHER,ID_BOLSO,'{dt_exec}' as DT_EXECUCAO,dt from join_feito".format(dt_exec = dt_execucao))#rec_rec_atualizado = sqlContext.sql("select "+schema_for01+" from join_feito")

rec_rec_final = rec_rec_atualizado.unionAll(join_so_rec_final)

rec_rec_final.registerTempTable("rec_rec_final")

# Loga faturas que nao encontraram a recarga para associar

join_so_fat_mapped = join_so_fat.map(to_tb_log)

if join_so_fat_mapped.count() > 0:
	lista_so_faturas = sqlContext.createDataFrame(join_so_fat_mapped).withColumnRenamed("_7", "cod_programa").withColumnRenamed("_8", "dt").coalesce(num_partition) ## lista de faturas que nao encontraram msisdn e nsu requisicao correspondentes
	
	lista_so_faturas.write.mode("append").partitionBy("cod_programa","dt").saveAsTable(db_prepago+".tb_log", format="parquet")


sqlContext.sql(create_TMP_TB_RECARGA_FATURA)

#fat_ja_atualizaram = sqlContext.sql("select DH_EXTRACAO,NUMERO_FAT,MSISDN,DATA_HORA_REC,DATA_HORA_FAT,VALOR_RECARGA,NSU_REQUISICAO,ATUALIZOU,DT_EXECUCAO,dt from " +db_prepago+".TB_RECARGA_FATURA where ATUALIZOU = true")

fat_atualizaram_hoje = sqlContext.sql("select DH_EXTRACAO02,NUMERO_FAT,MSISDN,DATA_HORA_REC,DATA_HORA_FAT,VALOR_RECARGA,NSU_REQUISICAO,DT_EXECUCAO02,1 as atualizou,dt02 from join_feito")

fat_nao_atualizaram = so_fat_final

#fat_nao_atualizaram = fat_nao_atualizaram.cache()
#fat_nao_atualizaram.count() #checkpoint

#fat_atualizaram_hoje = fat_atualizaram_hoje.cache()
#fat_atualizaram_hoje.count() #checkpoint

#fat_ja_atualizaram = fat_ja_atualizaram.cache()
#fat_ja_atualizaram.count() #checkpoint

fat_df_final = fat_nao_atualizaram.unionAll(fat_atualizaram_hoje) #.unionAll(fat_ja_atualizaram)

fat_df_final.write.mode("append").partitionBy("atualizou","dt").saveAsTable(db_stg+".TMP_TB_RECARGA_FATURA", format="parquet")

sqlContext.sql("create table {db_stg}.TMP_STG_PP_RECARGA_REC like {db_stg}.STG_PP_RECARGA_REC".format(db_stg = db_stg))

sqlContext.sql("insert into table "+db_stg+".TMP_STG_PP_RECARGA_REC partition (dt) select * from rec_rec_final")

sqlContext.sql("drop table "+db_stg+".STG_PP_RECARGA_REC");

sqlContext.sql("alter table {db}.TMP_STG_PP_RECARGA_REC rename to {db}.STG_PP_RECARGA_REC".format(db = db_stg))

sqlContext.sql("alter table "+db_prepago+".TB_RECARGA_FATURA drop partition (atualizou = 0)")
#sqlContext.sql("drop table "+db_prepago+".TB_RECARGA_FATURA")

#sqlContext.sql("ALTER TABLE "+db_stg+".TMP_TB_RECARGA_FATURA RENAME TO "+db_prepago+".TB_RECARGA_FATURA")
sqlContext.sql("insert into table "+db_prepago+".TB_RECARGA_FATURA partition(atualizou,dt) select * from "+db_stg+".TMP_TB_RECARGA_FATURA")

sqlContext.sql("drop table "+db_stg+".TMP_TB_RECARGA_FATURA")


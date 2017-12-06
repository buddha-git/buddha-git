###   Nome do programa: cancela_NFV_CF
###
###   Programa responsavel pela atualizacao das NFVs da tabela STG_PP_NFV_CF com os cancelamentos, devolucoes e retornos da tabela STG_PP_CAN_CF
###
###   Os erros geram registros na tabela de log com a data de ocorrencia

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext, Row
from pyspark.sql.types import * 
import sys 

conf = SparkConf()

#conf.setMaster("yarn-client")

sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

data_processa = sys.argv[2]+"000000"
conf_path = sys.argv[1]

sc.addPyFile(conf_path+"/conf_cancela_NFV_CF.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_cancela_NFV_CF import *

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
		grava_log("ERRO",cod_erro,mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
		grava_rej(rej_rdd, dir_rejeicao+arquivo+"."+dt_execucao_formatada+"."+cod_programa + cod_erro[tipo_erro])

def to_tb_log(linha):
	#[dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo,dt]
	dh_ocorrencia = dt_execucao
	
	tp_ocorrencia = "ERRO"
	
	cod_ocorrencia = cod_programa + cod_erro["NFV_NAO_ENCONTRADA_PARA_CANCELAMENTO_DEVOLUCAO_RETORNO"]
	
	nf = linha[7]
	
	mensagem = "NFV "+ nf +" nao encontrada para cancelamento/devolucao/retorno"
	
	nome_arquivo =  arquivo
	
	dt = str(dh_ocorrencia)[:10]
	
	return (dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo, cod_programa, dt)

def cancela(rdd):
	for item in rdd[26]:
		if item not in rdd[27]:
			return False
	return True

fieldsLog = [StructField(field_name, StringType(), True) for field_name in schemaLog.split(';')]

fieldsLog[0].dataType = TimestampType()

schemaLog = StructType(fieldsLog)

conf.setAppName("Cancela NFV_CF")

schemaString = "DH_EXTRACAO_SAP;DH_EXTRACAO_INTEGRACAO;NUMERO_NF;NUM_ORDEM_VENDA;DATA_HORA_NF;ID_ITEM_NF;VALOR_ITEM_NF;VALOR_NF;VALOR_MATERIAL;COD_DISTRIBUIDOR_SAP;ORGANIZACAO_VENDA;SETOR_ATIVIDADE;CANAL_DISTRIBUICAO;UF;TIPO_ORDEM;MATERIAL;DESC_MATERIAL;SERIE;SUBSERIE;CFOP;NUM_PEDIDO;LISTA_SERIES;LISTA_SERIES_VAL;VALOR_FACE;dt"

nfv_df = sqlContext.sql("select * from "+ db_stg+".STG_PP_NFV_CF where STATUS = 'VAL'").select([name for name in schemaString.split(";")]).withColumnRenamed("NUM_ORDEM_VENDA","ORDEM_VENDA_JOIN")

nfv_can = sqlContext.sql("select NUMERO_NF as NUMERO_NF_CAN, NUM_ORDEM_VENDA as NUM_ORDEM_VENDA_CAN, DATA_HORA_NF as DATA_HORA_NF_CAN, TIPO_ORDEM as TIPO_ORDEM_CAN, TIPO_ACAO as STATUS, NUMERO_ORDEM_VENDA_ORIGINAL, LISTA_SERIES as LISTA_SERIAIS_CAN, NUMERO_NF_ORIGINAL, DH_EXTRACAO_SAP as DH_SAP, DH_EXTRACAO_INTEGRACAO as DH_INTEGRACAO, CANCELOU, dt as dt_can from "+ db_stg+".STG_PP_NFV_CF_CAN where CANCELOU = false")

nfv_join = nfv_can.join(nfv_df, nfv_df.ORDEM_VENDA_JOIN == nfv_can.NUMERO_ORDEM_VENDA_ORIGINAL, "fullouter").coalesce(num_partition)

nfv_so_nota = nfv_join.filter("STATUS is null")

nfv_so_nota.registerTempTable("tmp_so_nota")

nfv_so_nota_gravar = sqlContext.sql("select DH_EXTRACAO_SAP,DH_EXTRACAO_INTEGRACAO,NUMERO_NF,ORDEM_VENDA_JOIN,DATA_HORA_NF,ID_ITEM_NF,VALOR_ITEM_NF,VALOR_NF,VALOR_MATERIAL,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,UF,TIPO_ORDEM,MATERIAL,DESC_MATERIAL,SERIE,SUBSERIE,CFOP,NUM_PEDIDO, '' as NUMERO_NF_CAN,'' as NUM_ORDEM_VENDA_CAN,'' as DATA_HORA_NF_CAN,'' as TIPO_ORDEM_CAN,'VAL' as STATUS,array(-1) as LISTA_SERIES_CAN,LISTA_SERIES,LISTA_SERIES_VAL,VALOR_FACE,dt from tmp_so_nota")

nfv_so_cancelamento = nfv_join.filter("NUMERO_NF is null")
if nfv_so_cancelamento.count() > 0:
	nfvs_nao_encontradas = sqlContext.createDataFrame(nfv_so_cancelamento.rdd.map(to_tb_log)).withColumnRenamed("_8", "dt").withColumnRenamed("_7", "cod_programa").coalesce(num_partition) 
	
	nfvs_nao_encontradas.write.mode("append").partitionBy("cod_programa","dt").saveAsTable( db_prepago+".tb_log", format="parquet")

nfv_pode_cancelar = nfv_join.filter("NUMERO_NF is not null and STATUS is not null")

nfv_rdd = nfv_pode_cancelar.map(lambda x: (x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19],x[20],x[21],x[22],x[23],x[24],x[25],x[26],x[27],x[28],x[29],x[30],x[31],x[32],x[0],x[1],str(x[2]),x[3],x[4],x[6],x[33],x[34],x[35],x[36],x[2],x[5],x[7],x[8],x[9],True,x[11])) ##x[36] acrescentado por conta da lista_series_val

nfv_cancela = nfv_rdd.filter(cancela)

def add_lista_series_val(x):
	lista_series_val = [item for item in x[27] if item not in x[26]]
	if lista_series_val == []:
		lista_series_val = [-1]
	return (x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19],x[20],x[21],x[22],x[23],x[24],x[25],x[26],x[27],lista_series_val,x[29],x[30],x[31],x[32],x[33],x[34],x[35],x[36],x[37])

nfv_cancela = nfv_cancela.map(add_lista_series_val)

nfv_nao_contido = nfv_rdd.filter(lambda x: cancela(x) == False) # array de canceladas nao esta contido em array de seriais. TO DO: rejeitar

nfvc_cancelou = nfv_cancela.map(lambda x: (x[34],x[35],x[31],x[21],x[22],x[25],x[24],x[33],x[32],x[26],True,x[37]))

#nfv_rdd = nfv_rdd.map(lambda x: x[:31])
nfv_cancela = nfv_cancela.map(lambda x: x[:31])

schema_final = "DH_EXTRACAO_SAP;DH_EXTRACAO_INTEGRACAO;NUMERO_NF;NUM_ORDEM_VENDA;DATA_HORA_NF;ID_ITEM_NF;VALOR_ITEM_NF;VALOR_NF;VALOR_MATERIAL;COD_DISTRIBUIDOR_SAP;ORGANIZACAO_VENDA;SETOR_ATIVIDADE;CANAL_DISTRIBUICAO;UF;TIPO_ORDEM;MATERIAL;DESC_MATERIAL;SERIE;SUBSERIE;CFOP;NUM_PEDIDO;NUMERO_NF_CAN;NUM_ORDEM_VENDA_CAN;DATA_HORA_NF_CAN;TIPO_ORDEM_CAN;STATUS;LISTA_SERIES_CAN;LISTA_SERIES;LISTA_SERIES_VAL;VALOR_FACE;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schema_final.split(';')]

fields[0].dataType = TimestampType()
fields[1].dataType = TimestampType()
fields[6].dataType = DecimalType(10,2)
fields[7].dataType = DecimalType(10,2)
fields[8].dataType = DecimalType(10,2)
fields[4].dataType = TimestampType()
#fields[23].dataType = TimestampType()
fields[26].dataType = ArrayType(StringType(), True)
fields[27].dataType = ArrayType(StringType(), True)
fields[28].dataType = ArrayType(StringType(), True)
fields[29].dataType = DecimalType(10,2)

schema = StructType(fields)

nfv_ja_canceladas = sqlContext.sql("select * from "+ db_stg+".STG_PP_NFV_CF where STATUS <> 'VAL'").drop("dt_execucao").rdd.cache()
nfv_ja_canceladas.count() #checkpoint

nfv_union = nfv_cancela.union(nfv_so_nota_gravar.rdd).union(nfv_ja_canceladas).cache()
nfv_union.count()

nfv_df = sqlContext.createDataFrame(nfv_union, schema)

nfv_df.registerTempTable("temp_notas")

nfv_so_cancelamento.registerTempTable("temp_so_canc")

nfvc_so_cancelamento = sqlContext.sql("select DH_SAP, DH_INTEGRACAO, DATA_HORA_NF_CAN, NUMERO_NF_CAN, NUM_ORDEM_VENDA_CAN, STATUS, TIPO_ORDEM_CAN, NUMERO_NF_ORIGINAL, NUMERO_ORDEM_VENDA_ORIGINAL, LISTA_SERIAIS_CAN,false,cast(to_date(DATA_HORA_NF_CAN) as string) as dt from temp_so_canc")

nfvc_ja_can = sqlContext.sql("select DH_EXTRACAO_SAP, DH_EXTRACAO_INTEGRACAO,DATA_HORA_NF,NUMERO_NF,NUM_ORDEM_VENDA,TIPO_ACAO,TIPO_ORDEM,NUMERO_NF_ORIGINAL,NUMERO_ORDEM_VENDA_ORIGINAL,LISTA_SERIES,CANCELOU,dt from "+ db_stg+".STG_PP_NFV_CF_CAN where CANCELOU = true").cache()
nfvc_ja_can.count() #checkpoint

nfvc_ja_can = nfvc_ja_can.cache()
nfvc_ja_can.count() #checkpoint

nfvc_so_cancelamento = nfvc_so_cancelamento.cache()
nfvc_so_cancelamento.count() #checkpoint

nfvc_cancelou = sqlContext.createDataFrame(nfvc_cancelou, nfvc_ja_can.schema).cache()
nfvc_cancelou.count() #checkpoint

nfv_cancelamentos_final_df = nfvc_ja_can.unionAll(nfvc_so_cancelamento).unionAll(nfvc_cancelou).coalesce(num_partition)

#nfv_cancelamentos_final_df = sqlContext.createDataFrame(nfv_cancelamentos_final)

nfv_cancelamentos_final_df.registerTempTable("temp_cancelamentos")

#cria tabelas temporarias pra fazer o overwrite

sqlContext.sql(create_TMP_STG_PP_NFV_CF)

notas_gravar = sqlContext.sql("select DH_EXTRACAO_SAP,DH_EXTRACAO_INTEGRACAO,NUMERO_NF,NUM_ORDEM_VENDA,DATA_HORA_NF,ID_ITEM_NF,VALOR_ITEM_NF,VALOR_NF,VALOR_MATERIAL,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,UF,TIPO_ORDEM,MATERIAL,DESC_MATERIAL,SERIE,SUBSERIE,CFOP,NUM_PEDIDO,NUMERO_NF_CAN,NUM_ORDEM_VENDA_CAN,DATA_HORA_NF_CAN,TIPO_ORDEM_CAN,STATUS,LISTA_SERIES_CAN,LISTA_SERIES,LISTA_SERIES_VAL,VALOR_FACE,'{dt_exec}' as dt_execucao,dt  from temp_notas".format(dt_exec = dt_execucao))

notas_gravar.write.mode("append").partitionBy("dt").saveAsTable(db_stg+".TMP_STG_PP_NFV_CF", format="parquet")

#sqlContext.sql("insert overwrite table "+ db_stg+".STG_PP_NFV_CF partition (dt) select * from temp_notas")

sqlContext.sql(create_TMP_STG_PP_NFV_CF_CAN)

cancelamentos_gravar = sqlContext.sql("select DH_EXTRACAO_SAP,DH_EXTRACAO_INTEGRACAO,DATA_HORA_NF,NUMERO_NF,NUM_ORDEM_VENDA,TIPO_ACAO,TIPO_ORDEM,NUMERO_NF_ORIGINAL,NUMERO_ORDEM_VENDA_ORIGINAL,LISTA_SERIES,CANCELOU,'{dt_exec}' as DT_EXECUCAO,dt from temp_cancelamentos".format(dt_exec = dt_execucao))

# So grava cancelamentos nao realizados
cancelamentos_gravar.filter("cancelou = false").write.mode("append").partitionBy("dt").saveAsTable(db_stg+".TMP_STG_PP_NFV_CF_CAN", format="parquet")

sqlContext.sql("drop table {db}.STG_PP_NFV_CF_CAN".format(db = db_stg))

sqlContext.sql("alter table "+db_stg+".TMP_STG_PP_NFV_CF_CAN rename to {db}.STG_PP_NFV_CF_CAN".format(db = db_stg))

#sqlContext.sql("insert overwrite table "+ db_stg+".STG_PP_NFV_CF_CAN partition (dt) select * from temp_cancelamentos")

sqlContext.sql("drop table {db}.STG_PP_NFV_CF".format(db = db_stg))

sqlContext.sql("alter table "+db_stg+".TMP_STG_PP_NFV_CF rename to {db}.STG_PP_NFV_CF".format(db = db_stg))


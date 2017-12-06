###   Nome do programa: cancela_NFV
###
###   Programa responsavel pela atualizacao das NFVs da tabela STG_PP_NFV com os cancelamentos, devolucoes e retornos da tabela STG_PP_CAN
###
###   Os erros geram registros na tabela de log com a data de ocorrencia

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext, Row
from pyspark.sql.types import *
import sys 

conf = SparkConf()

sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

data_processa = sys.argv[2]+"000000"
conf_path = sys.argv[1]

sc.addPyFile(conf_path+"/conf_cancela_NFV_NCF.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_cancela_NFV_NCF import *

dt_execucao = str(datetime.now()).split(".")[0] 

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
		grava_rej(rej_rdd, dir_rejeicao+arquivo+"."+cod_programa + cod_erro[tipo_erro])

def to_tb_log(linha):
	#[dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo,dt]
	dh_ocorrencia = dt_execucao
	tp_ocorrencia = "ERRO"
	cod_ocorrencia = cod_programa + cod_erro["NFV_NAO_ENCONTRADA_PARA_CANCELAMENTO_DEVOLUCAO_RETORNO"]
	nf = linha[7]
	mensagem = "NFV "+ nf +" nao encontrada para cancelamento/devolucao/retorno"
	nome_arquivo = arquivo
	dt = str(dh_ocorrencia)[:10]
	return (dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo,cod_programa,dt)

fieldsLog = [StructField(field_name, StringType(), True) for field_name in schemaLog.split(';')]

fieldsLog[0].dataType = TimestampType()

schemaLog = StructType(fieldsLog)

conf.setAppName(nome)

schemaString = "DH_EXTRACAO_SAP;DH_EXTRACAO_INTEGRACAO;NUMERO_NF;NUM_ORDEM_VENDA;DATA_HORA_NF;ID_ITEM_NF;VALOR_ITEM_NF;VALOR_NF;VALOR_MATERIAL;COD_DISTRIBUIDOR_SAP;ORGANIZACAO_VENDA;SETOR_ATIVIDADE;CANAL_DISTRIBUICAO;UF;TIPO_ORDEM;MATERIAL;DESC_MATERIAL;SERIE;SUBSERIE;CFOP;NUM_PEDIDO;NUMERO_NF_CAN;NUM_ORDEM_VENDA_CAN;DATA_HORA_NF_CAN;TIPO_ORDEM_CAN;STATUS"

nfv_df = sqlContext.sql("select * from "+db_stg+".STG_PP_NFV_NCF where STATUS = 'VAL'").withColumnRenamed("NUM_ORDEM_VENDA","ORDEM_VENDA_JOIN").coalesce(num_partition)#.select([name for name in schemaString.split(";")])

nfv_can = sqlContext.sql("select DH_EXTRACAO_SAP as DH_EXTRACAO_SAP_CAN, DH_EXTRACAO_INTEGRACAO as DH_EXTRACAO_INTEGRACAO_CAN, DATA_HORA_NF as DATA_HORA_CAN, NUMERO_NF as NF_CAN, NUM_ORDEM_VENDA as ORDEM_VENDA_CAN, TIPO_ACAO as TIPO_ACAO_CAN, TIPO_ORDEM as ORDEM_CAN, NUMERO_NF_ORIGINAL as NUMERO_NF_ORIGINAL_CAN, NUMERO_ORDEM_VENDA_ORIGINAL as NUMERO_ORDEM_VENDA_ORIGINAL_CAN, CANCELOU, dt as dt_can from "+db_stg+".STG_PP_NFV_NCF_CAN where CANCELOU = false").coalesce(num_partition)

nfv_join = nfv_can.join(nfv_df, nfv_df.ORDEM_VENDA_JOIN == nfv_can.NUMERO_ORDEM_VENDA_ORIGINAL_CAN, "fullouter")

# dataframe de nfv sem nota de cancelamento associada
nfv_so_nota = nfv_join.filter("TIPO_ACAO_CAN is null")

nfv_so_nota.registerTempTable("tmp_so_nota")

nfv_so_nota_gravar = sqlContext.sql("select DH_EXTRACAO_SAP,DH_EXTRACAO_INTEGRACAO,NUMERO_NF,ORDEM_VENDA_JOIN,DATA_HORA_NF,ID_ITEM_NF,VALOR_ITEM_NF,VALOR_NF,VALOR_MATERIAL,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,UF,TIPO_ORDEM,MATERIAL,DESC_MATERIAL,SERIE,SUBSERIE,CFOP,NUM_PEDIDO, '' as NUMERO_NF_CAN,'' as NUM_ORDEM_VENDA_CAN,'' as DATA_HORA_NF_CAN,'' as TIPO_ORDEM_CAN,'VAL' as STATUS from tmp_so_nota")

# dataframe de nota de cancelamento sem nfv associada
nfvc_so_cancelamento = nfv_join.filter("NUMERO_NF is null")

# mapeia cancelamentos sem nota para gravacao do erro na tb_log
notas_n_encontradas = nfvc_so_cancelamento.rdd.map(to_tb_log)

if notas_n_encontradas.count() > 0:
	notas_n_encontradas = sqlContext.createDataFrame(notas_n_encontradas).withColumnRenamed("_7", "cod_programa").withColumnRenamed("_8", "dt").coalesce(num_partition)	 # lista de faturas que nao encontraram msisdn e nsu requisicao correspondentes
	notas_n_encontradas.write.mode("append").partitionBy("cod_programa","dt").saveAsTable(db_prepago+".tb_log", format="parquet")

# notas de cancelamentos com nfvs associadas
nfv_pode_cancelar = nfv_join.filter("NUMERO_NF is not null and TIPO_ACAO_CAN is not null")

nfv_rdd = nfv_pode_cancelar.map(lambda x: (x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19],x[20],x[21],x[22],x[23],x[24],x[25],x[26],x[27],x[28],x[29],x[30],x[31],x[3],x[4],str(x[2]),x[6],x[5],x[37])) #NFVs canceladas

# mantem apenas as informacoes da nota de cancelamento e atualiza CANCELOU = True
nfvc_cancelou = nfv_pode_cancelar.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],True,x[10])) #Cancelamentos efetuados

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[1].dataType = TimestampType()
fields[6].dataType = DecimalType(10,2)
fields[7].dataType = DecimalType(10,2)
fields[8].dataType = DecimalType(10,2)
fields[4].dataType = TimestampType()
#fields[23].dataType = TimestampType()

schema = StructType(fields)

nfv_ja_canceladas = sqlContext.sql("select * from "+db_stg+".STG_PP_NFV_NCF where STATUS <> 'VAL'").drop("dt").rdd

#Une notas que estao sendo canceladas nessa execucao, notas que ja foram canceladas anteriormente e notas sem cancelamento
nfv_union = nfv_rdd.union(nfv_so_nota_gravar.rdd).union(nfv_ja_canceladas).coalesce(int(num_partition)).cache()
nfv_union.count()

nfv_df = sqlContext.createDataFrame(nfv_union, schema)

nfv_df.registerTempTable("temp_notas")

nfvc_so_cancelamento.registerTempTable("temp_so_canc")

nfvc_so_cancelamento = sqlContext.sql("select DH_EXTRACAO_SAP_CAN, DH_EXTRACAO_INTEGRACAO_CAN, DATA_HORA_CAN, NF_CAN, ORDEM_VENDA_CAN, TIPO_ACAO_CAN, ORDEM_CAN, NUMERO_NF_ORIGINAL_CAN, NUMERO_ORDEM_VENDA_ORIGINAL_CAN, false,dt_can from temp_so_canc")

nfvc_ja_can = sqlContext.sql("select DH_EXTRACAO_SAP, DH_EXTRACAO_INTEGRACAO,DATA_HORA_NF,NUMERO_NF,NUM_ORDEM_VENDA,TIPO_ACAO,TIPO_ORDEM,NUMERO_NF_ORIGINAL,NUMERO_ORDEM_VENDA_ORIGINAL,CANCELOU,dt from "+db_stg+".stg_pp_nfv_ncf_can where CANCELOU = true")

nfvc_ja_can = nfvc_ja_can.rdd.cache()
nfvc_ja_can.count()

nfvc_so_cancelamento = nfvc_so_cancelamento.rdd.cache()
nfvc_so_cancelamento.count()

nfvc_cancelou = nfvc_cancelou.cache()
nfvc_cancelou.count()

# une cancelamentos de execucoes anteriores, cancelamentos da execucao atual e cancelamentos que nao encontraram a nfv associada
nfvc_cancelamentos_final = nfvc_ja_can.union(nfvc_so_cancelamento).union(nfvc_cancelou).cache()
nfvc_cancelamentos_final.count()

schema_cancelamentos = 'DH_EXTRACAO_SAP;DH_EXTRACAO_INTEGRACAO;DATA_HORA_NF;NUMERO_NF;NUM_ORDEM_VENDA;TIPO_ACAO;TIPO_ORDEM;\
NUMERO_NF_ORIGINAL;NUMERO_ORDEM_VENDA_ORIGINAL;CANCELOU;dt'

fields_cancelamentos = [StructField(field_name, StringType(), True) for field_name in schema_cancelamentos.split(';')]

fields_cancelamentos[0].dataType = TimestampType()
fields_cancelamentos[1].dataType = TimestampType()
fields_cancelamentos[2].dataType = TimestampType()
fields_cancelamentos[9].dataType = BooleanType()
#fields_cancelamentos[23].dataType = TimestampType()

schema_cancelamentos = StructType(fields_cancelamentos)

nfvc_cancelamentos_final_df = sqlContext.createDataFrame(nfvc_cancelamentos_final,schema_cancelamentos).cache()
nfvc_cancelamentos_final_df.count()

sqlContext.sql("insert overwrite table "+db_stg+".STG_PP_NFV_NCF partition (dt) select *,'{dt_exec}' as DT_EXECUCAO, to_date(DATA_HORA_NF) as dt from temp_notas".format(dt_exec = dt_execucao))

nfvc_cancelamentos_final_df.registerTempTable("temp_cancelamentos")

nfvc_cancelamentos_final_gravar = sqlContext.sql("select DH_EXTRACAO_SAP,DH_EXTRACAO_INTEGRACAO,DATA_HORA_NF,NUMERO_NF,NUM_ORDEM_VENDA,TIPO_ACAO,TIPO_ORDEM,NUMERO_NF_ORIGINAL,NUMERO_ORDEM_VENDA_ORIGINAL,CANCELOU,'{dt_exec}' as dt_execucao, dt from temp_cancelamentos".format(dt_exec = dt_execucao))

##Cria tabela temporaria para realizar o overwrite da STG_PP_NFV_NCF_CAN 

sqlContext.sql(create_tmp_stg_pp_nfv_ncf_can)

# So grava cancelamentos nao realizados
nfvc_cancelamentos_final_gravar.filter("CANCELOU = false").write.mode("append").partitionBy("dt").saveAsTable(db_stg+".tmp_stg_pp_nfv_ncf_can", format="parquet")

sqlContext.sql("drop table "+db_stg+".STG_PP_NFV_NCF_CAN")

sqlContext.sql("alter table "+db_stg+".tmp_stg_pp_nfv_ncf_can rename to "+db_stg+".STG_PP_NFV_NCF_CAN")

#nfvc_cancelamentos_final_gravar.registerTempTable("nfvc_cancelamentos_final_gravar")
#sqlContext.sql("insert overwrite table "+db_stg+".STG_PP_NFV_NCF_CAN partition (dt) select * from nfvc_cancelamentos_final_gravar")

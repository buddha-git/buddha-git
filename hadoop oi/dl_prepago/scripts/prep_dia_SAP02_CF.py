###   Nome do programa: prep_dia_SAP02_CF
###
###   O seguinte programa le os arquivos da interface SAP02_CF, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela 
###
###   Tambem verifica a duplicidade dos registros, tanto dentro do proprio arquivo quanto em relacao a tabela final
###
###   Os registros rejeitados na validacao serao gravados no diretorio de rejeicoes no HDFS
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

data_processa = sys.argv[2]+"000000"
conf_path = sys.argv[1] 
data_arquivo = sys.argv[4]

sc.addPyFile(conf_path+"/conf_prep_dia_SAP02_CF.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_SAP02_CF import *

for chave in sqlContext_conf_list:
	sqlContext.setConf(chave, sqlContext_conf_list[chave])

arquivo = arquivo.replace("dataArquivo",data_arquivo)

dt_execucao = str(datetime.now()).split(".")[0] 
dt_execucao_formatada = dt_execucao.replace("-","").replace(":","").replace(" ","").split(".")[0] 

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
		grava_rej(rej_rdd, dir_rejeitado+arquivo+"."+dt_execucao_formatada+"."+cod_programa + cod_erro[tipo_erro])

		
##Realiza a gravacao de um registro na tabela de log

def grava_log(tp_ocorrencia,cod_ocorrencia,mensagem):
	dh_ocorrencia = dt_execucao
	nome_arquivo = arquivo
	dt = str(dh_ocorrencia)[:10]
	dup_rdd = sc.parallelize([[dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo, cod_programa, dt]])
	dup_df = sqlContext.createDataFrame(dup_rdd).withColumnRenamed("_7","cod_programa").withColumnRenamed("_8","dt")
	dup_df.write.mode("append").partitionBy("cod_programa","dt").saveAsTable(db_prepago+".TB_LOG", format="parquet")

def get_array(ini, fim):
	if ini == "" and fim == "":
		return []
	elif ini == "" and fim != "":
		return None
	elif ini != "" and fim == "":
		return None
	
	try:
		ini = long(ini)
		fim = long(fim)
		lista = []
		for num in range(ini,(fim+1)):
			lista.append(num)
		return lista
	except ValueError:
		return None

fieldsLog = [StructField(field_name, StringType(), True) for field_name in schemaLog.split(';')]

fieldsLog[0].dataType = TimestampType()

schemaLog = StructType(fieldsLog)

conf.setAppName("Valida SAP02_CF")

dir_arquivo = dir_processamento+arquivo

sap02 = sc.textFile(dir_arquivo) 

##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "DH_EXTRACAO_SAP;DH_EXTRACAO_INTEGRACAO;DATA_HORA_NF;NUMERO_NF;NUM_ORDEM_VENDA;TIPO_ACAO;TIPO_ORDEM;NUMERO_NF_ORIGINAL;NUMERO_ORDEM_VENDA_ORIGINAL;LISTA_SERIES;MANDT;CANCELOU;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[1].dataType = TimestampType()
fields[2].dataType = TimestampType()
fields[9].dataType = ArrayType(StringType(), True)

schema = StructType(fields) 

##Mapeia os campos para o formato da tabela e inicia as validacoes

def sap_map(x):
	try:
		return (valida_data(x[0]),valida_data_nula(x[1]),valida_data(x[2]),x[3],x[4],x[5],x[6],x[7],x[8],get_array(x[9],x[10]),x[11],False,dt_execucao,str(valida_data(x[2]))[:10])
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

sap02_temp = sap02.map(lambda k: k.split(";")).map(sap_map).cache()

sap_rej_estrutura = sap02_temp.filter(lambda x: len(x) <= 1)

registra_erros(sap_rej_estrutura," ","ESTRUTURA_INVALIDA")

sap02_ace = sap02_temp.filter(lambda x : len(x) > 1)

sap02_rej_data_extracao_sap = sap02_ace.filter(lambda x: type(x[0]) is unicode)

registra_erros(sap02_rej_data_extracao_sap,"DH_EXTRACAO_SAP","DH_EXTRACAO_SAP_INVALIDA") 

sap02_ace = sap02_ace.filter(lambda x : type(x[0]) is not unicode)

sap02_rej_data_extracao_int = sap02_ace.filter(lambda x: type(x[1]) is unicode)

registra_erros(sap02_rej_data_extracao_int,"DH_EXTRACAO_INTEGRACAO","DH_EXTRACAO_INTEGRACAO_INVALIDA")

sap02_ace = sap02_ace.filter(lambda x : type(x[1]) is not unicode)

sap02_rej_dh_nf = sap02_ace.filter(lambda x: type(x[2]) is unicode)

registra_erros(sap02_rej_dh_nf,"DATA_HORA_NF","DATA_HORA_NF_INVALIDA")

sap02_ace = sap02_ace.filter(lambda x : type(x[2]) is not unicode)

sap02_rej_tempo_guarda = sap02_ace.filter(lambda x: x[2] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(sap02_rej_tempo_guarda,"DATA_HORA_NF","REGISTRO_FORA_TEMPO_GUARDA")

sap02_ace = sap02_ace.filter(lambda x : x[2] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

sap02_rej_antigos = sap02_ace.filter(lambda x: x[2] < valida_data(data_producao))

registra_erros(sap02_rej_antigos,"DATA_HORA_NF","REGISTRO_ANTIGO")

sap02_ace = sap02_ace.filter(lambda x : x[2] >= valida_data(data_producao))

sap02_rej_tipo_acao = sap02_ace.filter(lambda x: x[5] not in ("CAN","DCE","DSE","RET"))

registra_erros(sap02_rej_tipo_acao,"TIPO_ACAO","TIPO_ACAO_INVALIDA")

sap02_ace = sap02_ace.filter(lambda x : x[5]  in ("CAN","DCE","DSE","RET"))

sap02_rej_num_nf_original = sap02_ace.filter(lambda x: x[5] in ("CAN","DCE","DSE") and x[7] == "")

registra_erros(sap02_rej_num_nf_original,"NUMERO_NF_ORIGINAL","NUMERO_NF_ORIGINAL_INVALIDO")

sap02_ace = sap02_ace.filter(lambda x : x[7] != "" or x[5] == "RET")

sap02_rej_num_ov_original = sap02_ace.filter(lambda x: x[5] in ("CAN","DCE","DSE") and x[8] == "")

registra_erros(sap02_rej_num_ov_original,"NUMERO_ORDEM_VENDA_ORIGINAL","NUMERO_ORDEM_VENDA_ORIGINAL_INVALIDO")

sap02_ace = sap02_ace.filter(lambda x : x[8] != "" or x[5] == "RET")

sap02_rej_lista_series = sap02_ace.filter(lambda x: x[9] is None)

registra_erros(sap02_rej_lista_series,"LISTA_SERIES","LISTA_SERIES_INVALIDA")

sap02_ace = sap02_ace.filter(lambda x : x[9] is not None)

sap02_df = sqlContext.createDataFrame(sap02_ace, schema).cache()

##Agrupa registros por numero_ordem_venda_original e lista_series para verificar duplicidade

windowSpec = Window.partitionBy(sap02_df['NUMERO_ORDEM_VENDA_ORIGINAL'],sap02_df['LISTA_SERIES'])

sap02_window = sap02_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

sap02_dup = sap02_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

sap02_df_uniq = sap02_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

sap02_old = sqlContext.sql("select NUMERO_ORDEM_VENDA_ORIGINAL, LISTA_SERIES, NUMERO_NF as testeJoin from "+db_stg+".STG_PP_NFV_CF_CAN")

sap02_join = sap02_df_uniq.join(sap02_old, ["NUMERO_ORDEM_VENDA_ORIGINAL","LISTA_SERIES"], "leftouter")

sap02_dup2 = sap02_join.filter("testejoin is not null")

sap02_df_final = sap02_join.filter("testeJoin is null").select([name for name in schemaString.split(";")]) #testeJoin sera nulo se o join nao tiver sido feito

sap02_duplicados = sap02_dup.unionAll(sap02_dup2.select([name for name in schemaString.split(";")]))

n_dup = sap02_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(sap02_duplicados, dir_rejeitado+arquivo+"."+dt_execucao_formatada+"."+cod_programa + cod_erro["REGISTRO_DUPLICADO"])

sap02_df_final.registerTempTable("sap02_df_final")

nfv_can_df = sqlContext.sql("select DH_EXTRACAO_SAP,DH_EXTRACAO_INTEGRACAO,DATA_HORA_NF,NUMERO_NF,NUM_ORDEM_VENDA,TIPO_ACAO,TIPO_ORDEM,NUMERO_NF_ORIGINAL,NUMERO_ORDEM_VENDA_ORIGINAL,LISTA_SERIES[0] as NUM_SERIE_INICIAL,LISTA_SERIES[size(LISTA_SERIES)-1] as NUM_SERIE_FINAL,MANDT,dt_execucao,dt from  sap02_df_final")

##Os dados resultantes sao armazenados na tabela de stage

sap02_df_final.drop("MANDT").write.mode("append").partitionBy("dt").saveAsTable(db_stg+".STG_PP_NFV_CF_CAN", format="parquet")

nfv_can_df.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_NFV_CAN", format="parquet")

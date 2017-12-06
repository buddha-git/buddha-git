###   Nome do programa: prep_dia_ARBOR01
###
###   O seguinte programa le os arquivos da interface ARBOR01, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

data_processa = sys.argv[2]+"000000"
conf_path = sys.argv[1]
data_arquivo = sys.argv[4] 

sc.addPyFile(conf_path+"/conf_prep_dia_ARBOR01.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")


from conf_prep_dia_ARBOR01 import *

arquivo = arquivo.replace("dataArquivo",data_arquivo)

for chave in sqlContext_conf_list:
	sqlContext.setConf(chave, sqlContext_conf_list[chave])

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

fieldsLog = [StructField(field_name, StringType(), True) for field_name in schemaLog.split(';')]

fieldsLog[0].dataType = TimestampType()

schemaLog = StructType(fieldsLog)

conf.setAppName(nome) 

dir_arquivo = dir_processamento+arquivo

arbor01 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "DH_EXTRACAO;NUMERO_FAT;MSISDN;DATA_HORA_REC;DATA_HORA_FAT;VALOR_RECARGA;NSU_REQUISICAO;DT_EXECUCAO;atualizou;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[3].dataType = TimestampType()
fields[4].dataType = TimestampType()
fields[5].dataType = DecimalType(10,2)
#fields[7].dataType = BooleanType()
#fields[10].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes

def arbor_map(p):
	try:
		return (valida_data(p[0]), p[1], "55"+p[2], valida_data(p[3]), valida_data(p[4]), valida_valor_divide(p[5]), p[6], dt_execucao,"0",str(valida_data(p[4]))[:10])
	except IndexError:
		linha = ""
		for campo in p:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

arbor01_temp = arbor01.map(lambda k: k.split(";")).map(arbor_map).cache()

arbor_rej_estrutura = arbor01_temp.filter(lambda x: len(x) <= 1)

registra_erros(arbor_rej_estrutura," ","ESTRUTURA_INVALIDA")

arbor01_ace = arbor01_temp.filter(lambda x : len(x) > 1)

arbor01_rej_dh_extracao = arbor01_ace.filter(lambda x: type(x[0]) is unicode)

registra_erros(arbor01_rej_dh_extracao,"DH_EXTRACAO","DT_EXTRACAO_INVALIDA")

arbor01_ace = arbor01_ace.filter(lambda x : type(x[0]) is not unicode)

arbor01_rej_numero_fat = arbor01_ace.filter(lambda x : x[1] == "")

registra_erros(arbor01_rej_numero_fat,"NUMERO_FAT","NUMERO_FAT_INVALIDO")

arbor01_ace = arbor01_ace.filter(lambda x : x[1] != "")

arbor01_rej_msisdn = arbor01_ace.filter(lambda x : x[2] == "")

registra_erros(arbor01_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

arbor01_ace = arbor01_ace.filter(lambda x : x[2] != "")	

arbor01_rej_data_hora_rec = arbor01_ace.filter(lambda x: type(x[3]) is unicode)

registra_erros(arbor01_rej_data_hora_rec,"DATA_HORA_REC","DATA_HORA_REC_INVALIDA")

arbor01_ace = arbor01_ace.filter(lambda x : type(x[3]) is not unicode)

arbor01_rej_tempo_guarda = arbor01_ace.filter(lambda x: x[3] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(arbor01_rej_tempo_guarda,"DATA_HORA_REC","REGISTRO_FORA_TEMPO_GUARDA")

arbor01_ace = arbor01_ace.filter(lambda x : x[3] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

arbor01_rej_antigos = arbor01_ace.filter(lambda x: x[3] < valida_data(data_producao))

registra_erros(arbor01_rej_antigos,"DATA_HORA_REC","REGISTRO_ANTIGO")

arbor01_ace = arbor01_ace.filter(lambda x : x[3] >= valida_data(data_producao))

arbor01_rej_data_hora_fat = arbor01_ace.filter(lambda x: type(x[4]) is unicode)

registra_erros(arbor01_rej_data_hora_fat,"DATA_HORA_FAT","DATA_HORA_FAT_INVALIDA")

arbor01_ace = arbor01_ace.filter(lambda x : type(x[4]) is not unicode)

arbor01_rej_vl_recarga = arbor01_ace.filter(lambda x: type(x[5]) is unicode)

registra_erros(arbor01_rej_vl_recarga,"VALOR_RECARGA","VALOR_RECARGA_INVALIDO")

arbor01_ace = arbor01_ace.filter(lambda x : type(x[5]) is not unicode)

arbor01_rej_nsu_requisicao = arbor01_ace.filter(lambda x : x[6] == "")

registra_erros(arbor01_rej_nsu_requisicao,"NSU_REQUISICAO","NSU_REQUISICAO_INVALIDO")

arbor01_ace = arbor01_ace.filter(lambda x : x[6] != "")	

arbor01_df = sqlContext.createDataFrame(arbor01_ace, schema).cache() 

##Agrupa registros por numero_fat, msisdn e nsu_requisicao para verificar duplicidade

windowSpec = Window.partitionBy(arbor01_df['NUMERO_FAT'], arbor01_df['MSISDN'], arbor01_df['NSU_REQUISICAO'])

arbor01_window = arbor01_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

arbor01_dup = arbor01_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

arbor01_df_uniq = arbor01_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

arbor01_old = sqlContext.sql("select NUMERO_FAT, MSISDN, NSU_REQUISICAO, VALOR_RECARGA as testeJoin from "+db_prepago+".TB_RECARGA_FATURA")

arbor01_join = arbor01_df_uniq.join(arbor01_old, ["NUMERO_FAT", "MSISDN", "NSU_REQUISICAO"], "leftouter")

arbor01_dup2 = arbor01_join.filter("testejoin is not null")

arbor01_df_final = arbor01_join.filter("testeJoin is null").select([name for name in schemaString.split(";")])

arbor01_duplicados = arbor01_dup.unionAll(arbor01_dup2.select([name for name in schemaString.split(";")]))

n_dup = arbor01_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(arbor01_duplicados, dir_rejeitado+arquivo+"."+dt_execucao_formatada+"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])

##Os dados resultantes sao armazenados na tabela final

arbor01_df_final.write.mode("append").partitionBy("atualizou","dt").saveAsTable(db_prepago+".TB_RECARGA_FATURA", format="parquet")

sc.stop()

###   Nome do programa: prep_dia_BLL09
###
###   O seguinte programa le os arquivos da BLL09, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc.addPyFile(conf_path+"/conf_prep_dia_BLL09.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_BLL09 import *

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

conf.setAppName("Valida BLL09") 

dir_arquivo = dir_processamento+arquivo

bll09 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final

schemaString = "MSISDN;DT_REQUISICAO;VL_SALDO;TP_REQUISICAO;PLANO_PRECO;VL_EMPRESTIMO;DH_EXTRACAO;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[1].dataType = TimestampType()
fields[2].dataType = DecimalType(10,2)
#fields[5].dataType = DecimalType(10,2)
fields[6].dataType = TimestampType()
#fields[7].dataType = TimestampType()


schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes
def bll09_map(x):
	try:
		return (x[0], valida_data_traco(x[1]), valida_valor(x[2]), x[3], x[4], x[5], valida_data_traco(x[6]), dt_execucao, str(valida_data_traco(x[1]))[:10])
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

bll09_temp = bll09.map(lambda k: k.split(";")).map(bll09_map).cache()

bll09_rej_estrutura = bll09_temp.filter(lambda x: len(x) <= 1)

registra_erros(bll09_rej_estrutura," ","ESTRUTURA_INVALIDA")

bll09_ace = bll09_temp.filter(lambda x : len(x) > 1)

bll09_rej_msisdn = bll09_ace.filter(lambda x : x[0] == "")

registra_erros(bll09_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

bll09_ace = bll09_ace.filter(lambda x : x[0] != "")	

bll09_rej_dt_requisicao = bll09_ace.filter(lambda x: type(x[1]) is unicode)

registra_erros(bll09_rej_dt_requisicao,"DT_REQUISICAO","DT_REQUISICAO_INVALIDA")

bll09_ace = bll09_ace.filter(lambda x : type(x[1]) is not unicode)

bll09_rej_tempo_guarda = bll09_ace.filter(lambda x: x[1] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(bll09_rej_tempo_guarda,"DT_REQUISICAO","REGISTRO_FORA_TEMPO_GUARDA")

bll09_ace = bll09_ace.filter(lambda x : x[1] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

bll09_rej_antigos = bll09_ace.filter(lambda x: x[1] < valida_data(data_producao))

registra_erros(bll09_rej_antigos,"DT_REQUISICAO","REGISTRO_ANTIGO")

bll09_ace = bll09_ace.filter(lambda x : x[1] >= valida_data(data_producao))

bll09_rej_vl_saldo = bll09_ace.filter(lambda x: type(x[2]) is unicode)

registra_erros(bll09_rej_vl_saldo,"VL_SALDO","VL_SALDO_INVALIDO")

bll09_ace = bll09_ace.filter(lambda x : type(x[2]) is not unicode)

bll09_rej_tp_requisicao = bll09_ace.filter(lambda x : x[3] not in ("DES", "ATI", "APR", "NSE", "SE", "PRE", "TPL"))

registra_erros(bll09_rej_tp_requisicao,"TP_REQUISICAO","TP_REQUISICAO_INVALIDA")

bll09_ace = bll09_ace.filter(lambda x : x[3] in ("DES", "ATI", "APR", "NSE", "SE", "PRE", "TPL"))	

bll09_rej_dh_extracao = bll09_ace.filter(lambda x: type(x[6]) is unicode)

registra_erros(bll09_rej_dh_extracao,"DH_EXTRACAO","DH_EXTRACAO_INVALIDA")

bll09_ace = bll09_ace.filter(lambda x : type(x[6]) is not unicode)

bll09_df = sqlContext.createDataFrame(bll09_ace, schema).cache() 

##Agrupa registros por msisdn, dt_requisicao e vl_saldo para verificar duplicidade

windowSpec = Window.partitionBy(bll09_df['MSISDN'], bll09_df['DT_REQUISICAO'], bll09_df['VL_SALDO'])

bll09_window = bll09_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

bll09_dup = bll09_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

bll09_df_uniq = bll09_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

bll09_old = sqlContext.sql("select MSISDN, DT_REQUISICAO, VL_SALDO, TP_REQUISICAO as testeJoin from "+db_prepago+".TB_APROPRIACAO")

bll09_join = bll09_df_uniq.join(bll09_old, ["MSISDN", "DT_REQUISICAO", "VL_SALDO"], "leftouter")

bll09_dup2 = bll09_join.filter("testejoin is not null")

bll09_df_final = bll09_join.filter("testeJoin is null").select([name for name in schemaString.split(";")])

bll09_duplicados = bll09_dup.unionAll(bll09_dup2.select([name for name in schemaString.split(";")]))

n_dup = bll09_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(bll09_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])
 
##Os dados resultantes sao armazenados na tabela final

bll09_df_final.registerTempTable("bll09_df_final")

bll09_gravar = sqlContext.sql("select MSISDN,DT_REQUISICAO,VL_SALDO,TP_REQUISICAO,PLANO_PRECO,VL_EMPRESTIMO, DH_EXTRACAO, '1' as NO_BOLSO ,'{dt_exec}' as dt_execucao,dt from bll09_df_final".format(dt_exec = str(dt_execucao)))

bll09_gravar.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_APROPRIACAO", format="parquet")


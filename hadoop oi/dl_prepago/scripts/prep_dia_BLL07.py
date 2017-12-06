###   Nome do programa: prep_dia_BLL07
###
###   O seguinte programa le os arquivos da BLL07, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc.addPyFile(conf_path+"/conf_prep_dia_BLL07.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_BLL07 import *

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

conf.setAppName("Valida BLL07") 

dir_arquivo = dir_processamento+arquivo

bll07 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final 
 
schemaString = "DT_AJUSTE;LOGIN_OPERADOR;MSISDN;VALOR;NO_BOLSO;NR_SERIAL;DH_EXTRACAO;CD_PRODUTO;CD_TIPO_PRODUTO;STATUS;TP_RECARGA;NUM_DIAS_BONUS;NU_REG_SIEBEL;SALDO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[3].dataType = DecimalType(15,2)
fields[6].dataType = TimestampType()
fields[13].dataType = DecimalType(15,2)

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes
def bll07_map(x):
	try:
		return (valida_data(x[0]), x[1], x[2], valida_valor(x[3]), x[4], x[5], valida_data_traco(x[6]), x[7], x[8], x[9],"", x[11], x[12], valida_valor_nulo(x[13]), str(valida_data(x[0]))[:10])
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

bll07_temp = bll07.map(lambda k: k.split(";")).map(bll07_map).cache()

bll07_rej_estrutura = bll07_temp.filter(lambda x: len(x) <= 1)

registra_erros(bll07_rej_estrutura," ","ESTRUTURA_INVALIDA")

bll07_ace = bll07_temp.filter(lambda x : len(x) > 1)

bll07_rej_dt_ajuste = bll07_ace.filter(lambda x: type(x[0]) is unicode)

registra_erros(bll07_rej_dt_ajuste,"DT_AJUSTE","DT_AJUSTE_INVALIDA") 

bll07_ace = bll07_ace.filter(lambda x : type(x[0]) is not unicode)

bll07_rej_tempo_guarda = bll07_ace.filter(lambda x: x[0] < valida_data( data_processa) - timedelta(days=int( tempo_guarda)))

registra_erros(bll07_rej_tempo_guarda,"DT_AJUSTE","REGISTRO_FORA_TEMPO_GUARDA")

bll07_ace = bll07_ace.filter(lambda x : x[0] >= valida_data( data_processa) - timedelta(days=int( tempo_guarda)))

bll07_rej_antigos = bll07_ace.filter(lambda x: x[0] < valida_data( data_producao))

registra_erros(bll07_rej_antigos,"DT_AJUSTE","REGISTRO_ANTIGO")

bll07_ace = bll07_ace.filter(lambda x : x[0] >= valida_data( data_producao))

bll07_rej_msisdn = bll07_ace.filter(lambda x : x[2] == "")

registra_erros(bll07_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

bll07_ace = bll07_ace.filter(lambda x : x[2] != "")	

bll07_rej_valor = bll07_ace.filter(lambda x: type(x[3]) is unicode)

registra_erros(bll07_rej_valor,"VALOR","VALOR_INVALIDO")

bll07_ace = bll07_ace.filter(lambda x : type(x[3]) is not unicode)

bll07_rej_id_bolso = bll07_ace.filter(lambda x : x[4] == "")

registra_erros(bll07_rej_id_bolso,"NO_BOLSO","NO_BOLSO_INVALIDO")

bll07_ace = bll07_ace.filter(lambda x : x[4] != "")	

bll07_rej_dt_extracao = bll07_ace.filter(lambda x: type(x[6]) is unicode)

registra_erros(bll07_rej_dt_extracao,"DH_EXTRACAO","DH_EXTRACAO_INVALIDA")

bll07_ace = bll07_ace.filter(lambda x : type(x[6]) is not unicode)

bll07_rej_saldo = bll07_ace.filter(lambda x : type(x[13]) is unicode)

registra_erros(bll07_rej_saldo,"SALDO","SALDO_INVALIDO")

bll07_ace = bll07_ace.filter(lambda x : type(x[13]) is not unicode)

bll07_df = sqlContext.createDataFrame(bll07_ace, schema).cache()

##Agrupa registros por msisdn, valor, dt_ajuste para verificar duplicidade

windowSpec = Window.partitionBy(bll07_df['DT_AJUSTE'], bll07_df['MSISDN'], bll07_df['VALOR'])

bll07_window = bll07_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

bll07_dup = bll07_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

bll07_df_uniq = bll07_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

bll07_old = sqlContext.sql("select DT_AJUSTE, MSISDN, VALOR, NO_BOLSO,NO_BOLSO as testeJoin from "+ db_prepago+".TB_AJUSTE")

bll07_join = bll07_df_uniq.join(bll07_old, ["DT_AJUSTE","MSISDN","VALOR","NO_BOLSO"], "leftouter")

bll07_dup2 = bll07_join.filter("testejoin is not null")

bll07_df_final = bll07_join.filter("testeJoin is null").select([name for name in schemaString.split(";")]) #testeJoin sera nulo se o join nao tiver sido feito

bll07_duplicados = bll07_dup.unionAll(bll07_dup2.select([name for name in schemaString.split(";")]))

n_dup = bll07_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(bll07_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])
	
##Mapeia os campos para o formato da tabela final 

bll07_map = bll07_df_final.map(lambda x: (x[6],x[0],x[1],x[2],x[3],x[4],x[5],x[11],"","",x[8],x[7],x[5],x[9],x[10],x[12],x[13],dt_execucao,x[14]))

schema_df = "DH_EXTRACAO;DT_AJUSTE;LOGIN_OPERADOR;MSISDN;VALOR;NO_BOLSO;ID_RECARGA;NUM_DIAS_BONUS;MOTIVO_AJUSTE;PLANO_PRECO;CD_TIPO_PRODUTO;CD_PRODUTO;NR_SERIAL;STATUS;TP_RECARGA;NU_REG_SIEBEL;SALDO;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schema_df.split(';')]

fields[0].dataType = TimestampType()
fields[1].dataType = TimestampType()
fields[4].dataType = DecimalType(15,2)
fields[16].dataType = DecimalType(15,2)

schema = StructType(fields)

bll07_df = sqlContext.createDataFrame(bll07_map, schema).cache()
    
##Os dados resultantes sao armazenados na tabela final

bll07_df.write.mode("append").partitionBy("dt").saveAsTable( db_prepago+".TB_AJUSTE", format="parquet")




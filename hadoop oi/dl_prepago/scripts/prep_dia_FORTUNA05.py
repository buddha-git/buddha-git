###   Nome do programa: prep_dia_FORTUNA05
###
###   O seguinte programa le os arquivos da interface FORTUNA05, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc.addPyFile(conf_path+"/conf_prep_dia_FORTUNA05.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_FORTUNA05 import *

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
		grava_log("ERRO",cod_erro[tipo_erro],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
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

conf.setAppName("Valida FORTUNA05") 

dir_arquivo = dir_processamento+arquivo

fortuna05 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final

schemaString = "DT_AJUSTE;LOGIN_OPERADOR;MSISDN;VALOR;NO_BOLSO;ID_RECARGA;DT_EXTRACAO;TP_RECARGA_CONCEDIDA;NUM_DIAS_BONUS;MOTIVO_AJUSTE;STATUS_RECARGA;PLANO_PRECO;CD_TIPO_PRODUTO;REGISTRO_SIEBEL;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[3].dataType = DecimalType(10,2)
fields[6].dataType = TimestampType()
#fields[14].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes
def fortuna05_map(x):
	try:
		return (valida_data(x[0]), x[1], x[2], valida_valor_divide(x[3]), x[4], x[5], valida_data(x[6]), x[7], x[8], x[9], x[10], x[11], x[12], x[13], dt_execucao, str(valida_data(x[0]))[:10])
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

fortuna05_temp = fortuna05.map(lambda k: k.split(";")).map(fortuna05_map).cache()

fortuna05_rej_estrutura = fortuna05_temp.filter(lambda x: len(x) <= 1)

registra_erros(fortuna05_rej_estrutura," ","ESTRUTURA_INVALIDA")

fortuna05_ace = fortuna05_temp.filter(lambda x : len(x) > 1)

fortuna05_rej_dt_ajuste = fortuna05_ace.filter(lambda x: type(x[0]) is unicode)

registra_erros(fortuna05_rej_dt_ajuste,"DT_AJUSTE","DT_AJUSTE_INVALIDA")

fortuna05_ace = fortuna05_ace.filter(lambda x : type(x[0]) is not unicode)

fortuna05_rej_tempo_guarda = fortuna05_ace.filter(lambda x: x[0] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(fortuna05_rej_tempo_guarda,"DT_AJUSTE","REGISTRO_FORA_TEMPO_GUARDA")

fortuna05_ace = fortuna05_ace.filter(lambda x : x[0] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

fortuna05_rej_antigos = fortuna05_ace.filter(lambda x: x[0] < valida_data(data_producao))

registra_erros(fortuna05_rej_antigos,"DT_AJUSTE","REGISTRO_ANTIGO")

fortuna05_ace = fortuna05_ace.filter(lambda x : x[0] >= valida_data(data_producao))

fortuna05_rej_msisdn = fortuna05_ace.filter(lambda x : x[2] == "")

registra_erros(fortuna05_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

fortuna05_ace = fortuna05_ace.filter(lambda x : x[2] != "")	

fortuna05_rej_valor = fortuna05_ace.filter(lambda x: type(x[3]) is unicode)

registra_erros(fortuna05_rej_valor,"VALOR","VALOR_INVALIDO")

fortuna05_ace = fortuna05_ace.filter(lambda x : type(x[3]) is not unicode)

fortuna05_rej_id_bolso = fortuna05_ace.filter(lambda x : x[4] == "")

registra_erros(fortuna05_rej_id_bolso,"NO_BOLSO","NO_BOLSO_INVALIDO")

fortuna05_ace = fortuna05_ace.filter(lambda x : x[4] != "")	

fortuna05_rej_dt_extracao = fortuna05_ace.filter(lambda x: type(x[6]) is unicode)

registra_erros(fortuna05_rej_dt_extracao,"DT_EXTRACAO","DT_EXTRACAO_INVALIDA")

fortuna05_ace = fortuna05_ace.filter(lambda x : type(x[6]) is not unicode)

fortuna05_df = sqlContext.createDataFrame(fortuna05_ace, schema).cache()

##Agrupa registros por msisdn, dt_ajuste e valor para verificar duplicidade

windowSpec = Window.partitionBy(fortuna05_df['MSISDN'], fortuna05_df['DT_AJUSTE'], fortuna05_df['VALOR'], fortuna05_df['NO_BOLSO'])

fortuna05_window = fortuna05_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

fortuna05_dup = fortuna05_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

fortuna05_df_uniq = fortuna05_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

fortuna05_old = sqlContext.sql("select MSISDN, DT_AJUSTE, VALOR, NO_BOLSO, ID_RECARGA as testeJoin from "+db_prepago+".TB_AJUSTE")

fortuna05_join = fortuna05_df_uniq.join(fortuna05_old, ["MSISDN","DT_AJUSTE", "VALOR", "NO_BOLSO"], "leftouter")

fortuna05_dup2 = fortuna05_join.filter("testejoin is not null")

fortuna05_df_final = fortuna05_join.filter("testeJoin is null").select([name for name in schemaString.split(";")])

fortuna05_duplicados = fortuna05_dup.unionAll(fortuna05_dup2.select([name for name in schemaString.split(";")]))

n_dup = fortuna05_duplicados.count()

if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(fortuna05_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])
 
##Mapeia os campos para o formato da tabela final 

fortuna05_map = fortuna05_df_final.map(lambda x: (x[6],x[0],x[1],x[2],x[3],x[4],x[5],x[8],x[9],x[11],x[12],"","",x[10],x[7],x[13],Decimal('0'),dt_execucao,str(x[0])[:10]))

schema_df = "DH_EXTRACAO;DT_AJUSTE;LOGIN_OPERADOR;MSISDN;VALOR;NO_BOLSO;ID_RECARGA;NUM_DIAS_BONUS;MOTIVO_AJUSTE;PLANO_PRECO;CD_TIPO_PRODUTO;CD_PRODUTO;NR_SERIAL;STATUS;TP_RECARGA_CONCEDIDA;NU_REG_SIEBEL;SALDO;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schema_df.split(';')]

fields[0].dataType = TimestampType()
fields[1].dataType = TimestampType()
fields[4].dataType = DecimalType(10,2)
fields[16].dataType = DecimalType(10,2)
#fields[17].dataType = TimestampType()

schema = StructType(fields)

fortuna05_df = sqlContext.createDataFrame(fortuna05_map, schema)

##Os dados resultantes sao armazenados na tabela final

fortuna05_df.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_AJUSTE", format="parquet")



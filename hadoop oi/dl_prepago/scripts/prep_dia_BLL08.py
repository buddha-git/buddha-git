###   Nome do programa: prep_dia_BLL08
###
###   O seguinte programa le os arquivos da BLL08, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc.addPyFile(conf_path+"/conf_prep_dia_BLL08.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_BLL08 import *

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

conf.setAppName("Valida BLL08") 

dir_arquivo = dir_processamento+arquivo

bll08 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final

schemaString = "DT_AJUSTE;LOGIN_OPERADOR;MSISDN;VALOR;NO_BOLSO;NR_SERIAL;DT_EXTRACAO;CD_PRODUTO;CD_TIPO_PRODUTO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[3].dataType = DecimalType(10,2)
fields[6].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes
def bll08_map(p):
	try:
		return (valida_data(p[0]), p[1], p[2], valida_valor(p[3]), p[4], p[5], valida_data_traco(p[6]), p[7], p[8], str(valida_data(p[0]))[:10])
	except IndexError:
		linha = ""
		for campo in p:
			linha += campo+";"
		linha = linha[:-1] # tira ultimo ponto e virgula
		return [linha] #precisa ser array por conta da funcao str_rej

bll08_temp = bll08.map(lambda k: k.split(";")).map(bll08_map).cache()

bll08_rej_estrutura = bll08_temp.filter(lambda x: len(x) <= 1)

registra_erros(bll08_rej_estrutura," ","ESTRUTURA_INVALIDA")

bll08_ace = bll08_temp.filter(lambda x : len(x) > 1)

bll08_rej_dh_ajuste = bll08_ace.filter(lambda x: type(x[0]) is unicode)

registra_erros(bll08_rej_dh_ajuste,"DT_AJUSTE","DT_AJUSTE_INVALIDA") 

bll08_ace = bll08_ace.filter(lambda x : type(x[0]) is not unicode)

bll08_rej_tempo_guarda = bll08_ace.filter(lambda x: x[0] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(bll08_rej_tempo_guarda,"DT_AJUSTE","REGISTRO_FORA_TEMPO_GUARDA")

bll08_ace = bll08_ace.filter(lambda x : x[0] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

bll08_rej_antigos = bll08_ace.filter(lambda x: x[0] < valida_data(data_producao))

registra_erros(bll08_rej_antigos,"DT_AJUSTE","REGISTRO_ANTIGO")

bll08_ace = bll08_ace.filter(lambda x : x[0] >= valida_data(data_producao))

bll08_rej_msisdn = bll08_ace.filter(lambda x : x[2] == "")

registra_erros(bll08_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

bll08_ace = bll08_ace.filter(lambda x : x[2] != "")	

bll08_rej_valor = bll08_ace.filter(lambda x: type(x[3]) is unicode)

registra_erros(bll08_rej_valor,"VALOR","VALOR_INVALIDO")

bll08_ace = bll08_ace.filter(lambda x : type(x[3]) is not unicode)

bll08_rej_no_bolso = bll08_ace.filter(lambda x : x[4] == "")

registra_erros(bll08_rej_no_bolso,"NO_BOLSO","NO_BOLSO_INVALIDO")

bll08_ace = bll08_ace.filter(lambda x : x[4] != "")	

bll08_rej_dt_extracao = bll08_ace.filter(lambda x: type(x[6]) is unicode)

registra_erros(bll08_rej_dt_extracao,"DH_EXTRACAO","DH_EXTRACAO_INVALIDA")

bll08_ace = bll08_ace.filter(lambda x : type(x[6]) is not unicode)

bll08_df = sqlContext.createDataFrame(bll08_ace, schema).cache()

##Agrupa registros por msisdn, valor, dt_ajuste para verificar duplicidade

windowSpec = Window.partitionBy(bll08_df['DT_AJUSTE'], bll08_df['MSISDN'], bll08_df['VALOR'])

bll08_window = bll08_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

bll08_dup = bll08_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

bll08_df_uniq = bll08_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

bll08_old = sqlContext.sql("select DT_AJUSTE, MSISDN, VALOR, NO_BOLSO,NO_BOLSO as testeJoin from "+db_prepago+".TB_AJUSTE")

bll08_join = bll08_df_uniq.join(bll08_old, ["DT_AJUSTE", "MSISDN", "VALOR","NO_BOLSO"], "leftouter")

bll08_dup2 = bll08_join.filter("testejoin is not null")

bll08_df_final = bll08_join.filter("testeJoin is null").select([name for name in schemaString.split(";")]) 

bll08_duplicados = bll08_dup.unionAll(bll08_dup2.select([name for name in schemaString.split(";")]))

n_dup = bll08_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(bll08_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])
 
##Mapeia os campos para o formato da tabela final 

bll08_map = bll08_df_final.map(lambda x: (x[6],x[0],x[1],x[2],x[3],x[4],x[5],"","","",x[8],x[7],x[5],"","","",Decimal("0"),dt_execucao,str(x[0])[:10]))

schema_df = "DH_EXTRACAO;DT_AJUSTE;LOGIN_OPERADOR;MSISDN;VALOR;NO_BOLSO;ID_RECARGA;NUM_DIAS_BONUS;MOTIVO_AJUSTE;PLANO_PRECO;CD_TIPO_PRODUTO;CD_PRODUTO;NR_SERIAL;STATUS;TP_RECARGA;NU_REG_SIEBEL;SALDO;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schema_df.split(';')]

fields[0].dataType = TimestampType()
fields[1].dataType = TimestampType()
fields[4].dataType = DecimalType(10,2)
fields[16].dataType = DecimalType(10,2)
#fields[17].dataType = TimestampType()

schema = StructType(fields)

bll08_df = sqlContext.createDataFrame(bll08_map, schema).cache()

##Os dados resultantes sao armazenados na tabela final

bll08_df.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_AJUSTE", format="parquet")





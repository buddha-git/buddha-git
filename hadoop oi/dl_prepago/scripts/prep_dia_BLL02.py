###   Nome do programa: prep_dia_BLL02
###
###   O seguinte programa le os arquivos da interface BLL02, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

conf_path = sys.argv[1]
data_processa = sys.argv[2]+"000000"
data_arquivo = sys.argv[4] 

sc.addPyFile(conf_path+"/conf_prep_dia_BLL02.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_BLL02 import *

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

fieldsLog = [StructField(field_name, StringType(), True) for field_name in schemaLog.split(';')]

fieldsLog[0].dataType = TimestampType()

schemaLog = StructType(fieldsLog)

conf.setAppName("Valida BLL02") 

dir_arquivo = dir_processamento+arquivo

bll02 = sc.textFile(dir_arquivo)


##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "MSISDN;MSISDN_B;DT_INICIO;DT_FIM;DURACAO;CD_PLANO_PRECO;TP_TARIFACAO;VL_CHAMADA;TP_PERIODO;TP_DIA_SEMANA;CD_ZONA_ORIGEM;DS_DESTINO;DS_ROAMING;CD_OPLD;TP_CHAMADA;VL_SALDO;AREA_TARIFACAO;CD_CATEGORIA;NO_BOLSO;AOS;DT_EXTRACAO;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[2].dataType = TimestampType()
fields[7].dataType = DecimalType(10,2)
fields[15].dataType = DecimalType(10,2)
#fields[20].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes
def bll02_map(x):
	try:
		return (x[0], x[1], valida_data_barra(x[2]), str(valida_data_barra(x[3])), x[4], x[5], x[6], valida_valor_divide(x[7]), x[8], x[9], x[10], x[11], x[12], x[13], x[14], valida_valor(x[15]), x[16], x[17], x[18], x[19], x[20], dt_execucao, str(valida_data_barra(x[2]))[:10])
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		linha = linha[:-1] # tira ultimo ponto e virgula (desnecessario)
		return [linha] #precisa ser array por conta da funcao str_rej

bll02_temp = bll02.map(lambda k: k.split(";")).map(bll02_map).cache()

bll02_rej_estrutura = bll02_temp.filter(lambda x: len(x) <= 1)

registra_erros(bll02_rej_estrutura," ","ESTRUTURA_INVALIDA")

bll02_ace = bll02_temp.filter(lambda x : len(x) > 1)

bll02_rej_msisdn = bll02_ace.filter(lambda x : x[0] == "")

registra_erros(bll02_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

bll02_ace = bll02_ace.filter(lambda x : x[0] != "")	

bll02_rej_msisdn_b = bll02_ace.filter(lambda x : x[1] == "")

registra_erros(bll02_rej_msisdn_b,"MSISDN_B","MSISDN_B_INVALIDO")

bll02_ace = bll02_ace.filter(lambda x : x[1] != "")	

bll02_rej_dt_inicio = bll02_ace.filter(lambda x: type(x[2]) is unicode)

registra_erros(bll02_rej_dt_inicio,"DT_INICIO","DT_INICIO_INVALIDA")

bll02_ace = bll02_ace.filter(lambda x : type(x[2]) is not unicode)

bll02_rej_tempo_guarda = bll02_ace.filter(lambda x: x[2] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(bll02_rej_tempo_guarda,"DT_INICIO","REGISTRO_FORA_TEMPO_GUARDA")

bll02_ace = bll02_ace.filter(lambda x : x[2] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

bll02_rej_antigos = bll02_ace.filter(lambda x: x[2] < valida_data(data_producao))

registra_erros(bll02_rej_antigos,"DT_INICIO","REGISTRO_ANTIGO")

bll02_ace = bll02_ace.filter(lambda x : x[2] >= valida_data(data_producao))

bll02_rej_tp_tarifacao = bll02_ace.filter(lambda x : x[6] == "")

registra_erros(bll02_rej_tp_tarifacao,"TP_TARIFACAO","TP_TARIFACAO_INVALIDO")

bll02_ace = bll02_ace.filter(lambda x : x[6] != "")	

bll02_rej_vl_chamada = bll02_ace.filter(lambda x: type(x[7]) is unicode)

registra_erros(bll02_rej_vl_chamada,"VL_CHAMADA","VL_CHAMADA_INVALIDO")

bll02_ace = bll02_ace.filter(lambda x : type(x[7]) is not unicode)

bll02_rej_vl_saldo = bll02_ace.filter(lambda x: type(x[15]) is unicode)

registra_erros(bll02_rej_vl_chamada,"VL_SALDO","VL_SALDO_INVALIDO")

bll02_ace = bll02_ace.filter(lambda x : type(x[15]) is not unicode)

bll02_rej_no_bolso = bll02_ace.filter(lambda x : x[18] == "")

registra_erros(bll02_rej_no_bolso,"NO_BOLSO","NO_BOLSO_INVALIDO")

bll02_ace = bll02_ace.filter(lambda x : x[18] != "")	

bll02_df = sqlContext.createDataFrame(bll02_ace, schema).cache() 

##Agrupa registros por msisdn, msisdn_b, dt_inicio e vl_chamada para verificar duplicidade

windowSpec = Window.partitionBy(bll02_df['MSISDN'], bll02_df['MSISDN_B'], bll02_df['DT_INICIO'], bll02_df['VL_CHAMADA'], bll02_df['NO_BOLSO'])#!!!

bll02_window = bll02_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

bll02_dup = bll02_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

bll02_df_uniq = bll02_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

bll02_old = sqlContext.sql("select MSISDN, MSISDN_B, DT_INICIO, VL_CHAMADA, NO_BOLSO, TP_TARIFACAO as testeJoin from " +db_prepago+".TB_SMS")

bll02_join = bll02_df_uniq.join(bll02_old, ["MSISDN", "MSISDN_B", "DT_INICIO", "VL_CHAMADA", "NO_BOLSO" ], "leftouter")

bll02_dup2 = bll02_join.filter("testejoin is not null")

bll02_df_final = bll02_join.filter("testeJoin is null").select([name for name in schemaString.split(";")])

bll02_duplicados = bll02_dup.unionAll(bll02_dup2.select([name for name in schemaString.split(";")]))

n_dup = bll02_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(bll02_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])

##Os dados resultantes sao armazenados na tabela final

bll02_df_final.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_SMS", format="parquet")

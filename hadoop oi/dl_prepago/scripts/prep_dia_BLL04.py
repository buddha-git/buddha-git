###   Nome do programa: prep_dia_BLL04
###
###   O seguinte programa le os arquivos da BLL04, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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
sqlContext.setConf("hive.enforce.bucketing", "true")

data_processa = sys.argv[2]+"000000"
conf_path = sys.argv[1]
data_arquivo = sys.argv[4] 

sc.addPyFile(conf_path+"/conf_prep_dia_BLL04.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_BLL04 import *

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

conf.setAppName("Valida BLL04") 

dir_arquivo = dir_processamento+arquivo

bll04 = sc.textFile(dir_arquivo)

n_regs = bll04.count()

if n_regs == 0:
    mensagem = "Arquivo vazio"
    grava_log("WARN",cod_programa + cod_erro["FORMATO_ARQUIVO_INVALIDO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    exit(0)

##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "MSISDN;MSISDN_B;DT_INICIO;DT_FIM;DURACAO;CD_PLANO_PRECO;TP_TARIFACAO;VL_CHAMADA;TP_PERIODO;TP_DIA_SEMANA;CD_ZONA_ORIGEM;DS_DESTINO;DS_ROAMING;CD_OPLD;TP_CHAMADA;VL_SALDO;AREA_TARIFACAO;CD_CATEGORIA;NO_BOLSO;AOS;VL_VOLUME_TRAFEGADO;CD_VELOCIDADE_ACESSO;IN_TIPO_VELOCIDADE;DDD_CELL_ID;CELL_ID;DT_EXTRACAO;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[2].dataType = TimestampType()
fields[7].dataType = DecimalType(10,2)
fields[15].dataType = DecimalType(10,2)
#fields[23].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes
def bll04_map(x):
	try:
		return (x[0], x[1], valida_data_barra(x[2]), str(valida_data_barra(x[3])), x[4], x[5], x[6], valida_valor_divide(x[7]), x[8], x[9], x[10], x[11], x[12], x[13], x[14], valida_valor_divide(x[15]), x[16], x[17], x[18], x[19], x[20], x[21], x[22], x[23], x[24], x[25],dt_execucao, str(valida_data_barra(x[2]))[:10])
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

bll04_temp = bll04.map(lambda k: k.split(";")).map(bll04_map).cache()

bll04_rej_estrutura = bll04_temp.filter(lambda x: len(x) <= 1)

registra_erros(bll04_rej_estrutura," ","ESTRUTURA_INVALIDA")

bll04_ace = bll04_temp.filter(lambda x : len(x) > 1)

bll04_rej_msisdn = bll04_ace.filter(lambda x : x[0] == "")

registra_erros(bll04_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

bll04_ace = bll04_ace.filter(lambda x : x[0] != "")	

bll04_rej_dt_inicio = bll04_ace.filter(lambda x: type(x[2]) is unicode)

registra_erros(bll04_rej_dt_inicio,"DT_INICIO","DT_INICIO_INVALIDA")

bll04_ace = bll04_ace.filter(lambda x : type(x[2]) is not unicode)

bll04_rej_tempo_guarda = bll04_ace.filter(lambda x: x[2] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(bll04_rej_tempo_guarda,"DT_INICIO","REGISTRO_FORA_TEMPO_GUARDA")

bll04_ace = bll04_ace.filter(lambda x : x[2] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

bll04_rej_antigos = bll04_ace.filter(lambda x: x[2] < valida_data(data_producao))

registra_erros(bll04_rej_antigos,"DT_INICIO","REGISTRO_ANTIGO")

bll04_ace = bll04_ace.filter(lambda x : x[2] >= valida_data(data_producao))

bll04_rej_vl_chamada = bll04_ace.filter(lambda x: type(x[7]) is unicode)

registra_erros(bll04_rej_vl_chamada,"VL_CHAMADA","VL_CHAMADA_INVALIDO")

bll04_ace = bll04_ace.filter(lambda x : type(x[7]) is not unicode)

bll04_rej_vl_saldo = bll04_ace.filter(lambda x: type(x[15]) is unicode)

registra_erros(bll04_rej_vl_chamada,"VL_SALDO","VL_SALDO_INVALIDO")

bll04_ace = bll04_ace.filter(lambda x : type(x[15]) is not unicode)

bll04_rej_no_bolso = bll04_ace.filter(lambda x : x[18] == "")

registra_erros(bll04_rej_no_bolso,"NO_BOLSO","NO_BOLSO_INVALIDO")

bll04_ace = bll04_ace.filter(lambda x : x[18] != "")	

bll04_df = sqlContext.createDataFrame(bll04_ace, schema).cache() 

##Agrupa registros por msisdn, msisdn_b, dt_inicio e vl_chamada para verificar duplicidade

windowSpec = Window.partitionBy(bll04_df['MSISDN'], bll04_df['MSISDN_B'], bll04_df['DT_INICIO'], bll04_df['VL_CHAMADA'], bll04_df['NO_BOLSO']) #!!!!

bll04_window = bll04_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")]) 

bll04_dup = bll04_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

bll04_df_uniq = bll04_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

bll04_df_uniq.registerTempTable("bll04_df_uniq")

#pega lista de particoes que aparecem no arquivo p/filtrar tabela

dt_list = sqlContext.sql("select distinct dt from bll04_df_uniq").collect()

dt_str = "("

for dt in dt_list:
	dt_str += "'{dt}',".format(dt = dt[0])

dt_str = dt_str[:-1] + ")"

bll04_old = sqlContext.sql("select MSISDN, MSISDN_B, DT_INICIO, VL_CHAMADA, NO_BOLSO, TP_TARIFACAO as testeJoin from "+db_prepago+".TB_GPRS where dt in "+dt_str) #!!!

bll04_join = bll04_df_uniq.join(bll04_old, ["MSISDN", "MSISDN_B", "DT_INICIO", "VL_CHAMADA", "NO_BOLSO" ], "leftouter") #!!!

bll04_dup2 = bll04_join.filter("testejoin is not null")

bll04_df_final = bll04_join.filter("testeJoin is null").select([name for name in schemaString.split(";")])

bll04_duplicados = bll04_dup.unionAll(bll04_dup2.select([name for name in schemaString.split(";")]))

n_dup = bll04_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(bll04_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])
    
##Os dados resultantes sao armazenados na tabela final

bll04_df_final.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_GPRS", format="parquet")


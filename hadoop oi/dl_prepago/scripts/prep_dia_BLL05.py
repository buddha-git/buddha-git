###   Nome do programa: prep_dia_BLL05
###
###   O seguinte programa le os arquivos da BLL05, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc.addPyFile(conf_path+"/conf_prep_dia_BLL05.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_BLL05 import *

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

conf.setAppName("Valida BLL05") 

dir_arquivo = dir_processamento+arquivo

bll05 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "MSISDN;DT_INICIO;TP_CHAMADA;DURACAO_REAL;DS_PLANO_PRECO;CD_OPLD;CD_PLANO_PRECO;CD_ZONA_ORIGEM_A;CD_ZONA_ORIGEM_B;TP_TARIFACAO;VL_CHAMADA;VL_SALDO;NO_BOLSO;DURACAO;TP_PERIODO;AREA_TARIFACAO;DS_DESTINO;DT_MSC;CD_CONCAT;MSISDN_B;ICMS;AOS;DT_EXTRACAO;DT_FIM;CD_CATEGORIA;CD_GRUPO;CD_TIPO_PRODUTO;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[1].dataType = TimestampType()
fields[10].dataType = DecimalType(10,2)
fields[11].dataType = DecimalType(10,2)
fields[22].dataType = TimestampType()
fields[23].dataType = TimestampType()
#fields[27].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes
def bll05_map(x):
	try:
		return (x[0], valida_data_barra(x[1]), x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], valida_valor_divide(x[10]), valida_valor_divide(x[11]), x[12], x[13], x[14], x[15], x[16], x[17], x[18], x[19], x[20], x[21], valida_data_barra(x[22]), valida_data_barra(x[23]), x[24], x[25], x[26], dt_execucao, str(valida_data_barra(x[1]))[:10])
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

bll05_temp = bll05.map(lambda k: k.split(";")).map(bll05_map).cache()

bll05_rej_estrutura = bll05_temp.filter(lambda x: len(x) <= 1)

registra_erros(bll05_rej_estrutura," ","ESTRUTURA_INVALIDA")

bll05_ace = bll05_temp.filter(lambda x : len(x) > 1)

bll05_rej_msisdn = bll05_ace.filter(lambda x : x[0] == "")

registra_erros(bll05_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

bll05_ace = bll05_ace.filter(lambda x : x[0] != "")	

bll05_rej_dt_inicio = bll05_ace.filter(lambda x: type(x[1]) is unicode)

registra_erros(bll05_rej_dt_inicio,"DT_INICIO","DT_INICIO_INVALIDA")

bll05_ace = bll05_ace.filter(lambda x : type(x[1]) is not unicode)

bll05_rej_tempo_guarda = bll05_ace.filter(lambda x: x[1] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(bll05_rej_tempo_guarda,"DT_INICIO","REGISTRO_FORA_TEMPO_GUARDA")

bll05_ace = bll05_ace.filter(lambda x : x[1] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

bll05_rej_antigos = bll05_ace.filter(lambda x: x[1] < valida_data(data_producao))

registra_erros(bll05_rej_antigos,"DT_INICIO","REGISTRO_ANTIGO")

bll05_ace = bll05_ace.filter(lambda x : x[1] >= valida_data(data_producao))

bll05_rej_tp_tarifacao = bll05_ace.filter(lambda x : x[9] == "")

registra_erros(bll05_rej_tp_tarifacao,"TP_TARIFACAO","TP_TARIFACAO_INVALIDO")

bll05_ace = bll05_ace.filter(lambda x : x[9] != "")	

bll05_rej_vl_chamada = bll05_ace.filter(lambda x: type(x[10]) is unicode)

registra_erros(bll05_rej_vl_chamada,"VL_CHAMADA","118010")

bll05_ace = bll05_ace.filter(lambda x : type(x[10]) is not unicode)

bll05_rej_vl_saldo = bll05_ace.filter(lambda x : type(x[11]) is unicode)

registra_erros(bll05_rej_vl_saldo,"VL_SALDO","VL_CHAMADA_INVALIDO")

bll05_ace = bll05_ace.filter(lambda x : type(x[11]) is not unicode)

bll05_rej_no_bolso = bll05_ace.filter(lambda x : x[12] == "")

registra_erros(bll05_rej_no_bolso,"NO_BOLSO","NO_BOLSO_INVALIDO")

bll05_ace = bll05_ace.filter(lambda x : x[12] != "")	

bll05_rej_msisdn_b = bll05_ace.filter(lambda x : x[19] == "")

registra_erros(bll05_rej_msisdn_b,"MSISDB_B","MSISDN_B_INVALIDO")

bll05_ace = bll05_ace.filter(lambda x : x[19] != "")	

bll05_rej_dt_inicio = bll05_ace.filter(lambda x: type(x[22]) is unicode)

registra_erros(bll05_rej_dt_inicio,"DT_EXTRACAO","DT_EXTRACAO_INVALIDA")

bll05_ace = bll05_ace.filter(lambda x : type(x[22]) is not unicode)

bll05_rej_dt_inicio = bll05_ace.filter(lambda x: type(x[23]) is unicode)

registra_erros(bll05_rej_dt_inicio,"DT_FIM","DT_FIM_INVALIDA")

bll05_ace = bll05_ace.filter(lambda x : type(x[23]) is not unicode)

bll05_df = sqlContext.createDataFrame(bll05_ace, schema).cache() 

##Agrupa registros por msisdn, msisdn_b, dt_inicio e vl_chamada para verificar duplicidade

windowSpec = Window.partitionBy(bll05_df['MSISDN'], bll05_df['DT_INICIO'], bll05_df['VL_CHAMADA'], bll05_df['MSISDN_B'], bll05_df['NO_BOLSO'])

bll05_window = bll05_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

bll05_dup = bll05_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

bll05_df_uniq = bll05_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

bll05_old = sqlContext.sql("select MSISDN, DT_INICIO, VL_CHAMADA, MSISDN_B, NO_BOLSO, TP_TARIFACAO as testeJoin from "+db_prepago+".TB_VOZ_FIXA")

bll05_join = bll05_df_uniq.join(bll05_old, ["MSISDN", "DT_INICIO", "MSISDN_B", "VL_CHAMADA", "NO_BOLSO"], "leftouter")

bll05_dup2 = bll05_join.filter("testejoin is not null")

bll05_df_final = bll05_join.filter("testeJoin is null").select([name for name in schemaString.split(";")])

bll05_duplicados = bll05_dup.unionAll(bll05_dup2.select([name for name in schemaString.split(";")]))

n_dup = bll05_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(bll05_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])
 
##Os dados resultantes sao armazenados na tabela final

bll05_df_final.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_VOZ_FIXA", format="parquet")


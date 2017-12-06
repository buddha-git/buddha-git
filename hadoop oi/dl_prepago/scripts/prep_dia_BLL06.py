###   Nome do programa: prep_dia_BLL06
###
###   O seguinte programa le os arquivos da BLL06, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc.addPyFile(conf_path+"/conf_prep_dia_BLL06.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_BLL06 import *

arquivo = arquivo.replace("dataArquivo",data_arquivo)

for chave in sqlContext_conf_list:
	sqlContext.setConf(chave, sqlContext_conf_list[chave])

dt_execucao = str(datetime.now()).split(".")[0] 
dt_execucao_formatada = dt_execucao.replace("-","").replace(":","").replace(" ","").split(".")[0] 

#def add_bolso(cd_origem):
#	if cd_origem in cod_bolso_franquia:
#		return cod_bolso_franquia
#	else:
#		return "1"

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

conf.setAppName("Valida BLL06") 

def get_bolso(cd_tipo_produto,tp_requisicao,tp_valor):
	tp_requisicao = int(tp_requisicao)
	tp_valor = int(tp_valor)
	cd_tipo_produto = int(cd_tipo_produto)
	if (cd_tipo_produto,tp_requisicao,tp_valor) in bolso_principal:
		return "1"
	if (cd_tipo_produto,tp_requisicao,tp_valor) in bolso_franquia:
		return cod_bolso_franquia
	return "0"

sqlContext.registerFunction("get_bolso",get_bolso)


dir_arquivo = dir_processamento+arquivo

bll06 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "MSISDN;VALOR;DT_REQUISICAO;NSU_REQUISICAO;TP_VALOR;DT_PROCESSAMENTO;CD_ERRO;TP_REQUISICAO;CD_ORIGEM;CD_CANAL;DT_EXTRACAO;SALDO;CD_PRODUTO;CD_TIPO_PRODUTO;get_bolso(cd_tipo_produto,tp_requisicao,tp_valor) as NO_BOLSO;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[1].dataType = DecimalType(10,2)
fields[2].dataType = TimestampType()
fields[5].dataType = TimestampType()
fields[10].dataType = TimestampType()
fields[11].dataType = DecimalType(10,2)
#fields[14].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes
def bll06_map(x):
	try:
		return (x[0], valida_valor(x[1]), valida_data_traco(x[2]), x[3], x[4], valida_data_traco(x[5]),x[6],x[7],x[8],x[9], valida_data_traco(x[10]), valida_valor(x[11]),x[12],x[13], get_bolso(x[13],x[7],x[4]),dt_execucao, str(valida_data_traco(x[2]))[:10])
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

bll06_temp = bll06.map(lambda k: k.split(";")).map(bll06_map).cache()

bll06_rej_estrutura = bll06_temp.filter(lambda x: len(x) <= 1)

registra_erros(bll06_rej_estrutura," ","ESTRUTURA_INVALIDA")

bll06_ace = bll06_temp.filter(lambda x : len(x) > 1)

bll06_rej_msisdn = bll06_ace.filter(lambda x : x[0] == "")

registra_erros(bll06_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

bll06_ace = bll06_ace.filter(lambda x : x[0] != "")	

bll06_rej_valor = bll06_ace.filter(lambda x: type(x[1]) is unicode)

registra_erros(bll06_rej_valor,"VALOR","VALOR_INVALIDO")

bll06_ace = bll06_ace.filter(lambda x : type(x[1]) is not unicode)

bll06_rej_dt_requisicao = bll06_ace.filter(lambda x: type(x[2]) is unicode)

registra_erros(bll06_rej_dt_requisicao,"DT_REQUISICAO","DT_REQUISICAO_INVALIDA")

bll06_ace = bll06_ace.filter(lambda x : type(x[2]) is not unicode)

bll06_rej_tempo_guarda = bll06_ace.filter(lambda x: x[2] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(bll06_rej_tempo_guarda,"DT_REQUISICAO","REGISTRO_FORA_TEMPO_GUARDA")

bll06_ace = bll06_ace.filter(lambda x : x[2] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

bll06_rej_antigos = bll06_ace.filter(lambda x: x[2] < valida_data(data_producao))

registra_erros(bll06_rej_antigos,"DT_REQUISICAO","REGISTRO_ANTIGO")

bll06_ace = bll06_ace.filter(lambda x : x[2] >= valida_data(data_producao))

bll06_rej_dt_processamento = bll06_ace.filter(lambda x: type(x[5]) is unicode)

registra_erros(bll06_rej_dt_processamento,"DT_PROCESSAMENTO","DT_PROCESSAMENTO_INVALIDA")

bll06_ace = bll06_ace.filter(lambda x : type(x[5]) is not unicode)

bll06_rej_cd_origem = bll06_ace.filter(lambda x : x[8] == "")

registra_erros(bll06_rej_cd_origem,"CD_ORIGEM","CD_ORIGEM_INVALIDO")

bll06_ace = bll06_ace.filter(lambda x : x[8] != "")	

bll06_rej_dh_extracao = bll06_ace.filter(lambda x: type(x[10]) is unicode)

registra_erros(bll06_rej_dh_extracao,"DH_EXTRACAO","DH_EXTRACAO_INVALIDA")

bll06_ace = bll06_ace.filter(lambda x : type(x[10]) is not unicode)

bll06_rej_saldo = bll06_ace.filter(lambda x: type(x[11]) is unicode)

registra_erros(bll06_rej_saldo,"SALDO","SALDO_INVALIDO")

bll06_ace = bll06_ace.filter(lambda x : type(x[11]) is not unicode)

bll06_df = sqlContext.createDataFrame(bll06_ace, schema).cache() 

##Agrupa registros por msisdn, valor, dt_requisicao e cd_origem para verificar duplicidade

windowSpec = Window.partitionBy(bll06_df['MSISDN'], bll06_df['VALOR'], bll06_df['DT_REQUISICAO'],bll06_df['CD_ORIGEM'],bll06_df['NSU_REQUISICAO'])

bll06_window = bll06_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

bll06_dup = bll06_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

bll06_df_uniq = bll06_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

bll06_old = sqlContext.sql("select MSISDN, VALOR, DT_REQUISICAO, CD_ORIGEM,NSU_REQUISICAO,NSU_REQUISICAO as testeJoin from "+db_prepago+".TB_TAXAS_BLL")

bll06_join = bll06_df_uniq.join(bll06_old, ["MSISDN", "VALOR", "DT_REQUISICAO", "CD_ORIGEM","NSU_REQUISICAO"], "leftouter")

bll06_dup2 = bll06_join.filter("testejoin is not null")

bll06_df_final = bll06_join.filter("testeJoin is null").select([name for name in schemaString.split(";")])

bll06_duplicados = bll06_dup.unionAll(bll06_dup2.select([name for name in schemaString.split(";")]))

n_dup = bll06_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(bll06_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])

##Os dados resultantes sao armazenados na tabela final

bll06_df_final.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_TAXAS_BLL", format="parquet")

###   Nome do programa: prep_dia_FORTUNA03
###
###   O seguinte programa le os arquivos da interface FORTUNA03, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc.addPyFile(conf_path+"/conf_prep_dia_FORTUNA03.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_FORTUNA03 import *

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

conf.setAppName("Valida FORTUNA03") 

dir_arquivo = dir_processamento+arquivo

fortuna03 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final

schemaString = "MSISDN;VL_DIREITO;DH_SOLICITACAO;ID_RECARGA;DH_EFETIVACAO;CD_ID_DEBITO;DH_EXTRACAO;VL_SALDO;CD_PLANO_PRECO;CD_TIPO_PRODUTO;CD_PRODUTO_RECARGA;CD_DIREITO;ID_BOLSO;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[1].dataType = DecimalType(10,2)
fields[2].dataType = TimestampType()
fields[4].dataType = TimestampType()
fields[6].dataType = TimestampType()
fields[7].dataType = DecimalType(10,2)
#fields[16].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes
def fortuna03_map(x):
	try:
		return (x[0], valida_valor_divide(x[1]), valida_data(x[2]), x[3], valida_data(x[4]), x[5], valida_data(x[6]), valida_valor_divide(x[7]), x[8], x[9], x[10], x[11], "1", dt_execucao, str(valida_data(x[2]))[:10])
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

fortuna03_temp = fortuna03.map(lambda k: k.split(";")).map(fortuna03_map).cache()

fortuna03_rej_estrutura = fortuna03_temp.filter(lambda x: len(x) <= 1)

registra_erros(fortuna03_rej_estrutura," ","ESTRUTURA_INVALIDA")

fortuna03_ace = fortuna03_temp.filter(lambda x : len(x) > 1)

fortuna03_rej_msisdn = fortuna03_ace.filter(lambda x : x[0] == "")

registra_erros(fortuna03_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

fortuna03_ace = fortuna03_ace.filter(lambda x : x[0] != "")	

fortuna03_rej_vl_direito = fortuna03_ace.filter(lambda x: type(x[1]) is unicode)

registra_erros(fortuna03_rej_vl_direito,"VL_DIREITO","VL_DIREITO_INVALIDO")

fortuna03_ace = fortuna03_ace.filter(lambda x : type(x[1]) is not unicode)

fortuna03_rej_dh_solicitacao = fortuna03_ace.filter(lambda x: type(x[2]) is unicode)

registra_erros(fortuna03_rej_dh_solicitacao,"DH_SOLICITACAO","DH_SOLICITACAO_INVALIDA")

fortuna03_ace = fortuna03_ace.filter(lambda x : type(x[2]) is not unicode)

fortuna03_rej_tempo_guarda = fortuna03_ace.filter(lambda x: x[2] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(fortuna03_rej_tempo_guarda,"DH_SOLICITACAO","REGISTRO_FORA_TEMPO_GUARDA")

fortuna03_ace = fortuna03_ace.filter(lambda x : x[2] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

fortuna03_rej_antigos = fortuna03_ace.filter(lambda x: x[2] < valida_data(data_producao))

registra_erros(fortuna03_rej_antigos,"DH_SOLICITACAO","REGISTRO_ANTIGO")

fortuna03_ace = fortuna03_ace.filter(lambda x : x[2] >= valida_data(data_producao))

fortuna03_rej_id_recarga = fortuna03_ace.filter(lambda x : x[3] == "")

registra_erros(fortuna03_rej_id_recarga,"ID_RECARGA","ID_RECARGA_INVALIDO")

fortuna03_ace = fortuna03_ace.filter(lambda x : x[3] != "")	

fortuna03_rej_dh_efetivacao = fortuna03_ace.filter(lambda x: type(x[4]) is unicode)

registra_erros(fortuna03_rej_dh_efetivacao,"DH_EFETIVACAO","DH_EFETIVACAO_INVALIDA")

fortuna03_ace = fortuna03_ace.filter(lambda x : type(x[4]) is not unicode)

fortuna03_rej_cd_id_debito = fortuna03_ace.filter(lambda x : x[5] == "")

registra_erros(fortuna03_rej_cd_id_debito,"CD_ID_DEBITO","CD_ID_DEBITO_INVALIDO")

fortuna03_ace = fortuna03_ace.filter(lambda x : x[5] != "")

fortuna03_rej_dh_extracao = fortuna03_ace.filter(lambda x: type(x[6]) is unicode)

registra_erros(fortuna03_rej_dh_extracao,"DH_EXTRACAO","DH_EXTRACAO_INVALIDA")

fortuna03_ace = fortuna03_ace.filter(lambda x : type(x[6]) is not unicode)

fortuna03_rej_vl_saldo = fortuna03_ace.filter(lambda x: type(x[7]) is unicode)

registra_erros(fortuna03_rej_vl_saldo,"VL_SALDO","VL_SALDO_INVALIDO")

fortuna03_ace = fortuna03_ace.filter(lambda x : type(x[7]) is not unicode)

fortuna03_df = sqlContext.createDataFrame(fortuna03_ace, schema).cache() 

##Agrupa registros por msisdn, vl_direto, dh_solicitacao, id_recarga e cd_id_debito para verificar duplicidade

windowSpec = Window.partitionBy(fortuna03_df['MSISDN'], fortuna03_df['VL_DIREITO'], fortuna03_df['DH_SOLICITACAO'],fortuna03_df['ID_RECARGA'], fortuna03_df['CD_ID_DEBITO'])

fortuna03_window = fortuna03_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

fortuna03_dup = fortuna03_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

fortuna03_df_uniq = fortuna03_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

fortuna03_old = sqlContext.sql("select MSISDN, VL_DIREITO, DH_SOLICITACAO,ID_RECARGA, CD_ID_DEBITO, CD_PLANO_PRECO as testeJoin from "+db_prepago+".TB_TAXAS")

fortuna03_join = fortuna03_df_uniq.join(fortuna03_old, ["MSISDN", "VL_DIREITO", "DH_SOLICITACAO", "ID_RECARGA", "CD_ID_DEBITO"], "leftouter")

fortuna03_dup2 = fortuna03_join.filter("testejoin is not null")

fortuna03_df_final = fortuna03_join.filter("testeJoin is null").select([name for name in schemaString.split(";")])

fortuna03_duplicados = fortuna03_dup.unionAll(fortuna03_dup2.select([name for name in schemaString.split(";")]))

n_dup = fortuna03_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(fortuna03_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])

##Os dados resultantes sao armazenados na tabela final

fortuna03_df_final.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_TAXAS", format="parquet")



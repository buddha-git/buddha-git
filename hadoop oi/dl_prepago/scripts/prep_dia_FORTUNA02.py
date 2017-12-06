###   Nome do programa: prep_dia_FORTUNA02
###
###   O seguinte programa le os arquivos da interface FORTUNA02, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc.addPyFile(conf_path+"/conf_prep_dia_FORTUNA02.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_FORTUNA02 import *

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

fortuna02 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final

schemaString = "DH_EXTRACAO;TP_DOCUMENTO;COD_DISTRIBUIDOR_SAP;ORG_VENDAS;CANAL_DISTRIBUICAO;SETOR_ATIVIDADE;CENTRO;DATA_SOLIC_ORDEM_VENDA;NUMERO_PEDIDO;MATERIAL;TP_CONDICAO;ID_RECARGA;CD_ORIGEM;DT_EXECUCAO;atualizou;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[7].dataType = TimestampType()
#fields[13].dataType = IntegerType() atualizou virou str
#fields[13].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes

def for_map(p):
	try:
		return (valida_data(p[0]), p[1], p[2], p[3], p[4], p[5], p[6], valida_data(p[7]), p[8], p[9], p[10], p[11], "0",dt_execucao, "0", str(valida_data(p[7]))[:10])
	except IndexError:
		linha = ""
		for campo in p:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

fortuna02_temp = fortuna02.map(lambda k: k.split(";")).map(for_map).cache()

fortuna02_rej_estrutura = fortuna02_temp.filter(lambda x: len(x) <= 1)

registra_erros(fortuna02_rej_estrutura," ","ESTRUTURA_INVALIDA")

fortuna02_ace = fortuna02_temp.filter(lambda x : len(x) > 1)

fortuna02_rej_dh_extracao = fortuna02_ace.filter(lambda x: type(x[0]) is unicode)

registra_erros(fortuna02_rej_dh_extracao,"DH_EXTRACAO","DH_EXTRACAO_INVALIDA")

fortuna02_ace = fortuna02_ace.filter(lambda x : type(x[0]) is not unicode)

fortuna02_rej_cod_distribuidor_sap = fortuna02_ace.filter(lambda x : x[2] == "")

registra_erros(fortuna02_rej_cod_distribuidor_sap,"COD_DISTRIBUIDOR_SAP","COD_DISTRIBUIDOR_SAP_INVALIDO")

fortuna02_ace = fortuna02_ace.filter(lambda x : x[2] != "")	

fortuna02_rej_org_vendas = fortuna02_ace.filter(lambda x : x[3] == "")

registra_erros(fortuna02_rej_org_vendas,"ORG_VENDAS","ORGANIZACAO_VENDA_INVALIDO")

fortuna02_ace = fortuna02_ace.filter(lambda x : x[3] != "")

fortuna02_rej_canal_distribuicao = fortuna02_ace.filter(lambda x : x[4] == "")

registra_erros(fortuna02_rej_canal_distribuicao,"CANAL_DISTRIBUICAO","CANAL_DISTRIBUICAO_INVALIDO")

fortuna02_ace = fortuna02_ace.filter(lambda x : x[4] != "")

fortuna02_rej_setor_atividade = fortuna02_ace.filter(lambda x : x[5] == "")

registra_erros(fortuna02_rej_setor_atividade,"SETOR_ATIVIDADE","SETOR_ATIVIDADE_INVALIDO")

fortuna02_ace = fortuna02_ace.filter(lambda x : x[5] != "")

fortuna02_rej_dt_solic_ordem_venda = fortuna02_ace.filter(lambda x: type(x[7]) is unicode)

registra_erros(fortuna02_rej_dt_solic_ordem_venda,"DATA_SOLIC_ORDEM_VENDA","DATA_SOLIC_ORDEM_VENDA_INVALIDA")

fortuna02_ace = fortuna02_ace.filter(lambda x : type(x[7]) is not unicode)

fortuna02_rej_tempo_guarda = fortuna02_ace.filter(lambda x: x[7] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(fortuna02_rej_tempo_guarda,"DATA_SOLIC_ORDEM_VENDA","REGISTRO_FORA_TEMPO_GUARDA")

fortuna02_ace = fortuna02_ace.filter(lambda x : x[7] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

fortuna02_rej_antigos = fortuna02_ace.filter(lambda x: x[7] < valida_data(data_producao))

registra_erros(fortuna02_rej_antigos,"DATA_SOLIC_ORDEM_VENDA","REGISTRO_ANTIGO")

fortuna02_ace = fortuna02_ace.filter(lambda x : x[7] >= valida_data(data_producao))

fortuna02_rej_numero_pedido = fortuna02_ace.filter(lambda x : x[8] == "")

registra_erros(fortuna02_rej_numero_pedido,"NUMERO_PEDIDO","NUMERO_PEDIDO_INVALIDO")

fortuna02_ace = fortuna02_ace.filter(lambda x : x[8] != "")	

fortuna02_rej_id_recarga = fortuna02_ace.filter(lambda x : x[11] == "")

registra_erros(fortuna02_rej_id_recarga,"ID_RECARGA","ID_RECARGA_INVALIDO")

fortuna02_ace = fortuna02_ace.filter(lambda x : x[11] != "")	

fortuna02_df = sqlContext.createDataFrame(fortuna02_ace, schema).cache()

##Agrupa registros por dh_solicitacao_ordem_venda e id_recarga para verificar duplicidade

windowSpec = Window.partitionBy(fortuna02_df['DATA_SOLIC_ORDEM_VENDA'], fortuna02_df['ID_RECARGA'])

fortuna02_window = fortuna02_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

fortuna02_dup = fortuna02_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

fortuna02_df_uniq = fortuna02_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

fortuna02_old = sqlContext.sql("select DATA_SOLIC_ORDEM_VENDA, ID_RECARGA, ID_RECARGA as teste from "+db_prepago+".TB_RECARGA_PEDIDO").withColumnRenamed("teste", "testeJoin")

fortuna02_join = fortuna02_df_uniq.join(fortuna02_old, ["DATA_SOLIC_ORDEM_VENDA", "ID_RECARGA"], "leftouter")

fortuna02_dup2 = fortuna02_join.filter("testejoin is not null")

fortuna02_df_final = fortuna02_join.filter("testeJoin is null").select([name for name in schemaString.split(";")])

fortuna02_duplicados = fortuna02_dup.unionAll(fortuna02_dup2.select([name for name in schemaString.split(";")]))

n_dup = fortuna02_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(fortuna02_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])

##Os dados resultantes sao armazenados na tabela final

fortuna02_df_final.write.mode("append").partitionBy("atualizou","dt").saveAsTable(db_prepago+".TB_RECARGA_PEDIDO", format="parquet")



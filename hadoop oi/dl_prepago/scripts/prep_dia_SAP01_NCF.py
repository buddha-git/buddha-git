###   Nome do programa: prep_dia_SAP01_NCF
###
###   O seguinte programa le os arquivos da interface SAP01_NCF, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela de stage
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

sc.addPyFile(conf_path+"/conf_prep_dia_SAP01_NCF.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_SAP01_NCF import *

arquivo = arquivo.replace("dataArquivo",data_arquivo)

for chave in sqlContext_conf_list:
	sqlContext.setConf(chave, sqlContext_conf_list[chave])

dt_execucao = str(datetime.now()).split(".")[0] 
dt_execucao_formatada = dt_execucao.replace("-","").replace(":","").replace(" ","").split(".")[0] 
		
##Realiza a gravacao de um registro na tabela de log

def grava_log(tp_ocorrencia,cod_ocorrencia,mensagem):
	dh_ocorrencia = dt_execucao
	nome_arquivo = arquivo
	dt = str(dh_ocorrencia)[:10]
	dup_rdd = sc.parallelize([[dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo, cod_programa, dt]])
	dup_df = sqlContext.createDataFrame(dup_rdd).withColumnRenamed("_7","cod_programa").withColumnRenamed("_8","dt")
	dup_df.write.mode("append").partitionBy("cod_programa","dt").saveAsTable(db_prepago+".TB_LOG", format="parquet")


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

def valida_material(material_string):
	if(material_string == ""):
		return Decimal("0")
	try:
		return Decimal(material_string)
	except InvalidOperation:
		return material_string

def grava_rej(rdd,path):
	gravar2 = rdd.map(str_rej)
	
	gravar2.saveAsTextFile(path)

fieldsLog = [StructField(field_name, StringType(), True) for field_name in schemaLog.split(';')]

fieldsLog[0].dataType = TimestampType()

schemaLog = StructType(fieldsLog)

conf.setAppName("Valida SAP01 NCF") 

dir_arquivo = dir_processamento+arquivo

sap = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "DH_EXTRACAO_SAP;DH_EXTRACAO_INTEGRACAO;NUMERO_NF;NUM_ORDEM_VENDA;DATA_HORA_NF;ID_ITEM_NF;VALOR_ITEM_NF;VALOR_NF;VALOR_MATERIAL;COD_DISTRIBUIDOR_SAP;ORGANIZACAO_VENDA;SETOR_ATIVIDADE;CANAL_DISTRIBUICAO;UF;TIPO_ORDEM;MATERIAL;DESC_MATERIAL;SERIE;SUBSERIE;CFOP;NUM_PEDIDO;NUMERO_NF_CAN;NUM_ORDEM_VENDA_CAN;DATA_HORA_NF_CAN;TIPO_ORDEM_CAN;STATUS;MANDT;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[1].dataType = TimestampType()
fields[6].dataType = DecimalType(10,2)
fields[7].dataType = DecimalType(10,2)
fields[8].dataType = DecimalType(10,2)
fields[4].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes

def sap_map(x):
	try:
		return (valida_data(x[0]), valida_data_nula(x[1]), x[2], x[3], valida_data(x[4]), x[5], valida_valor(x[6]),valida_valor(x[7]), valida_material(x[8]), x[9], x[10], x[11], x[12], x[13], x[14], x[17], x[18], x[19], x[20], x[21], x[22], "", "", "", "", "VAL", x[23], dt_execucao, str(valida_data(x[4]))[:10])
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

sap_temp = sap.map(lambda k: k.split(";")).map(sap_map).cache()

sap_rej_estrutura = sap_temp.filter(lambda x: len(x) <= 1)

registra_erros(sap_rej_estrutura," ","ESTRUTURA_INVALIDA")

sap_ace = sap_temp.filter(lambda x : len(x) > 1)

sap_rej_valor_item = sap_ace.filter(lambda x: type(x[6]) is unicode)

registra_erros(sap_rej_valor_item,"VALOR_ITEM_NF","VALOR_ITEM_NF_INVALIDO")

sap_ace = sap_ace.filter(lambda x : type(x[6]) is not unicode)

sap_rej_extracao = sap_ace.filter(lambda x: type(x[0]) is unicode)

registra_erros(sap_rej_extracao,"DH_EXTRACAO_SAP","DH_EXTRACAO_SAP_INVALIDA") #rdd, nome_campo, cod_erro

sap_ace = sap_ace.filter(lambda x : type(x[0]) is not unicode)

sap_rej_valor_nf = sap_ace.filter(lambda x: type(x[7]) is unicode)

registra_erros(sap_rej_valor_nf,"VALOR_NF","VALOR_NF_INVALIDO")

sap_ace = sap_ace.filter(lambda x : type(x[7]) is not unicode)

sap_rej_valor_material = sap_ace.filter(lambda x: type(x[8]) is unicode)

registra_erros(sap_rej_valor_material,"VALOR_MATERIAL","VALOR_MATERIAL_INVALIDO")

sap_ace = sap_ace.filter(lambda x : type(x[8]) is not unicode)

sap_rej_data = sap_ace.filter(lambda x: type(x[4]) is unicode)

registra_erros(sap_rej_data,"DATA_HORA_NF","DATA_HORA_NF_INVALIDA")

sap_ace = sap_ace.filter(lambda x : type(x[4]) is not unicode)

sap_rej_tempo_guarda = sap_ace.filter(lambda x: x[4] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(sap_rej_tempo_guarda,"DATA_HORA_NF","REGISTRO_FORA_TEMPO_GUARDA")

sap_ace = sap_ace.filter(lambda x : x[4] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

sap_rej_antigos = sap_ace.filter(lambda x: x[4] < valida_data(data_producao))

registra_erros(sap_rej_antigos,"DATA_HORA_NF","REGISTRO_ANTIGO")

sap_ace = sap_ace.filter(lambda x : x[4] >= valida_data(data_producao))

sap_rej_nf = sap_ace.filter(lambda x: x[2] == "")

registra_erros(sap_rej_nf,"NUMERO_NF","NUMERO_NF_INVALIDO")

sap_ace = sap_ace.filter(lambda x : x[2] != "")

sap_rej_ordem = sap_ace.filter(lambda x: x[3] == "")

registra_erros(sap_rej_ordem,"NUM_ORDEM_VENDA","NUM_ORDEM_VENDA_INVALIDO")

sap_ace = sap_ace.filter(lambda x : x[3] != "")

sap_rej_id_item = sap_ace.filter(lambda x: x[5] == "")

registra_erros(sap_rej_id_item,"ID_ITEM_NF","ID_ITEM_NF_INVALIDO")

sap_ace = sap_ace.filter(lambda x : x[5] != "")

sap_rej_cod_dist = sap_ace.filter(lambda x: x[9] == "")

registra_erros(sap_rej_cod_dist,"COD_DISTRIBUIDOR_SAP","CD_DISTRIBUIDOR_SAP_INVALIDO")

sap_ace = sap_ace.filter(lambda x : x[9] != "")

sap_rej_num_pedido = sap_ace.filter(lambda x: x[20] == "")

registra_erros(sap_rej_num_pedido,"NUM_PEDIDO","NUMERO_PEDIDO_INVALIDO")

sap_ace = sap_ace.filter(lambda x : x[20] != "")

sap_df = sqlContext.createDataFrame(sap_ace, schema).cache()

##Agrupa registros por data_hora_nf, num_ordem_venda e id_item_nf para verificar duplicidade

windowSpec = Window.partitionBy(sap_df['DATA_HORA_NF'], sap_df['NUM_ORDEM_VENDA'], sap_df['ID_ITEM_NF'])

sap_window = sap_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

sap_dup = sap_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

sap_df_uniq = sap_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

sap_old = sqlContext.sql("select DATA_HORA_NF, NUM_ORDEM_VENDA, ID_ITEM_NF, num_pedido from "+db_stg+".STG_PP_NFV_NCF").withColumnRenamed("num_pedido", "num")

sap_join = sap_df_uniq.join(sap_old, ["DATA_HORA_NF","NUM_ORDEM_VENDA","ID_ITEM_NF"], "leftouter")

sap_dup2 = sap_join.filter("num is not null")

sap_df_final = sap_join.filter("num is null").select([name for name in schemaString.split(";")]) #num sera nulo se o join nao tiver sido feito

sap_duplicados = sap_dup.unionAll(sap_dup2.select([name for name in schemaString.split(";")]))

n_dup = sap_duplicados.count()

if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(sap_duplicados, dir_rejeitado+arquivo+"."+dt_execucao_formatada+"."+cod_programa + cod_erro["REGISTRO_DUPLICADO"])

sap_df_final.registerTempTable("sap_df_final")

nfv_df = sqlContext.sql("select DH_EXTRACAO_SAP,DH_EXTRACAO_INTEGRACAO,NUMERO_NF,NUM_ORDEM_VENDA,DATA_HORA_NF,ID_ITEM_NF,VALOR_ITEM_NF,VALOR_NF,VALOR_MATERIAL,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,UF,TIPO_ORDEM,'' as NUM_SERIE_INICIAL,'' as NUM_SERIE_FINAL,MATERIAL,DESC_MATERIAL,SERIE,SUBSERIE,CFOP,NUM_PEDIDO,dt_execucao,dt from  sap_df_final")

##Os dados resultantes sao armazenados na tabela de stage e final

sap_df_final.drop("MANDT").write.mode("append").partitionBy("dt").saveAsTable(db_stg+".STG_PP_NFV_NCF", format="parquet")

nfv_df.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_NFV", format="parquet")

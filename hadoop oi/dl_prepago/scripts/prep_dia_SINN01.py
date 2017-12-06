###   Nome do programa: prep_dia_SINN01
###
###   O seguinte programa le os arquivos da interface SINN01, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

data_processa = sys.argv[2]+"000000"
conf_path = sys.argv[1]
data_arquivo = sys.argv[4]

sc.addPyFile(conf_path+"/conf_prep_dia_SINN01.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_SINN01 import *

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

conf.setAppName("Valida SINN01") 

dir_arquivo = dir_processamento+arquivo

sinn01 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "DATA_HORA_CONSUMO,DATA_NF,NUMERO_NF,HASH_CODE,SERIE,SUBSERIE,CFOP,MSISDN,NOME,CPF_CNPJ,VALOR_BOLSO,VALOR_NF,VALOR_ICMS,ALIQUOTA,UF,ID_CONSUMIDOR,ENCONTROU,DT_EXECUCAO,dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(',')]

fields[0].dataType = TimestampType()
fields[1].dataType = TimestampType()
fields[10].dataType = DecimalType(10,2)
fields[11].dataType = DecimalType(10,2)
fields[12].dataType = DecimalType(10,2)
fields[16].dataType = BooleanType()
#fields[17].dataType = TimestampType()

schema = StructType(fields)

def sinn_map(p):
	try:
		return (valida_data(p[0]), valida_so_data(p[1]), p[2], p[3], p[4], p[5], p[6], "55"+p[7], p[8], p[9], valida_valor(p[10]),valida_valor(p[11]),valida_valor(p[12]),p[13],p[14],p[15],False,dt_execucao,str(valida_data(p[0]))[:10])
	except IndexError:
		linha = ""
		for campo in p:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

##Mapeia os campos para o formato da tabela e inicia as validacoes

sinn01_temp = sinn01.map(lambda k: k.split(";")).map(sinn_map).cache()

sinn_rej_estrutura = sinn01_temp.filter(lambda x: len(x) <= 1)

registra_erros(sinn_rej_estrutura," ","ESTRUTURA_INVALIDA")

sinn01_ace = sinn01_temp.filter(lambda x : len(x) > 1)

sinn01_rej_consumo = sinn01_ace.filter(lambda x: type(x[0]) is unicode)

registra_erros(sinn01_rej_consumo,"DATA_HORA_CONSUMO","DATA_HORA_CONSUMO_INVALIDA")

sinn01_ace = sinn01_ace.filter(lambda x : type(x[0]) is not unicode)

sinn01_rej_tempo_guarda = sinn01_ace.filter(lambda x: x[0] < valida_data(data_processa) - timedelta(days=int( tempo_guarda)))

registra_erros(sinn01_rej_tempo_guarda,"DATA_HORA_CONSUMO","REGISTRO_FORA_TEMPO_GUARDA")

sinn01_ace = sinn01_ace.filter(lambda x : x[0] >= valida_data(data_processa) - timedelta(days=int( tempo_guarda)))

sinn01_rej_producao = sinn01_ace.filter(lambda x: x[0] < valida_data(data_producao))

registra_erros(sinn01_rej_producao,"DATA_HORA_CONSUMO","REGISTRO_ANTIGO")

sinn01_ace = sinn01_ace.filter(lambda x : x[0] >= valida_data(data_producao))

sinn01_rej_data_nf = sinn01_ace.filter(lambda x: type(x[1]) is unicode)

registra_erros(sinn01_rej_data_nf,"DATA_NF","DATA_NF_INVALIDA")

sinn01_ace = sinn01_ace.filter(lambda x : type(x[1]) is not unicode)

sinn01_rej_msisdn = sinn01_ace.filter(lambda x : x[get_indice_campo("MSISDN",schemaString.replace(",",";"))] == "55")

registra_erros(sinn01_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

sinn01_ace = sinn01_ace.filter(lambda x : x[get_indice_campo("MSISDN",schemaString.replace(",",";"))] != "55")

sinn01_rej_valor_bolso = sinn01_ace.filter(lambda x : type(x[10]) is unicode)

registra_erros(sinn01_rej_valor_bolso,"VALOR_BOLSO","VALOR_BOLSO_INVALIDO")

sinn01_ace = sinn01_ace.filter(lambda x : type(x[10]) is not unicode)

sinn01_rej_valor_nf = sinn01_ace.filter(lambda x : type(x[11]) is unicode)

registra_erros(sinn01_rej_valor_nf,"VALOR_NF","VALOR_NF_INVALIDO")

sinn01_ace = sinn01_ace.filter(lambda x : type(x[11]) is not unicode)

sinn01_rej_valor_icms = sinn01_ace.filter(lambda x : type(x[12]) is unicode)

registra_erros(sinn01_rej_valor_icms,"VALOR_ICMS","VALOR_ICMS_INVALIDO")

sinn01_ace = sinn01_ace.filter(lambda x : type(x[12]) is not unicode)

sinn01_df = sqlContext.createDataFrame(sinn01_ace, schema).cache() 

##Agrupa registros por cd_distribuidor_sap, cd_organizacao_venda, cd_canal_sap, cd_setor_atividade e dh_atualizacao para verificar duplicidade

windowSpec = Window.partitionBy(sinn01_df['DATA_HORA_CONSUMO'], sinn01_df['MSISDN'], sinn01_df['VALOR_BOLSO'])

sinn01_window = sinn01_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(",")])

sinn01_dup = sinn01_window.filter("row_number <> 1").select([name for name in schemaString.split(",")])

sinn01_df_uniq = sinn01_window.filter("row_number = 1").select([name for name in schemaString.split(",")])

sinn01_old = sqlContext.sql("select DATA_HORA_CONSUMO, MSISDN, VALOR_BOLSO, ID_CONSUMIDOR as testeJoin from "+db_prepago+".TB_NFC_OITV")

sinn01_join = sinn01_df_uniq.join(sinn01_old, ["DATA_HORA_CONSUMO", "MSISDN", "VALOR_BOLSO"], "leftouter")

sinn01_dup2 = sinn01_join.filter("testejoin is not null")

sinn01_df_final = sinn01_join.filter("testeJoin is null").select([name for name in schemaString.split(",")])

sinn01_duplicados = sinn01_dup.unionAll(sinn01_dup2.select([name for name in schemaString.split(",")]))

n_dup = sinn01_duplicados.count()

if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(sinn01_duplicados, dir_rejeitado+arquivo+"."+dt_execucao_formatada+"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])

sinn01_df_final = sinn01_df_final.cache()
sinn01_df_final.count()

sinn01_tabela = sqlContext.sql("select * from "+db_prepago+".TB_NFC_OITV").cache()
sinn01_tabela.count()

sinn01_df_completo = sinn01_df_final.unionAll(sinn01_tabela)

sinn01_encontraram_antes = sinn01_df_completo.filter("encontrou = true")

sinn01_podem_encontrar = sinn01_df_completo.filter("encontrou = false")

nfpre_df = sqlContext.sql("select DATA_HORA_CONSUMO, MSISDN, tipo_cdr as testeJoin from "+db_prepago+".TB_NFC where TIPO_CONSUMO = 'Oi TV'")

sinn01_join_nfc = sinn01_podem_encontrar.join(nfpre_df, ["DATA_HORA_CONSUMO","MSISDN"], "leftouter")

sinn01_encontraram = sinn01_join_nfc.filter("testeJoin is not null")

sinn01_encontraram.registerTempTable("sinn01_encontraram")

sinn01_encontraram_final = sqlContext.sql("select DATA_HORA_CONSUMO,DATA_NF,NUMERO_NF,HASH_CODE,SERIE,SUBSERIE,CFOP,MSISDN,NOME,CPF_CNPJ,VALOR_BOLSO,VALOR_NF,VALOR_ICMS,ALIQUOTA,UF,ID_CONSUMIDOR,True as ENCONTROU,DT_EXECUCAO,dt from sinn01_encontraram")

sinn01_nao_encontraram = sinn01_join_nfc.filter("testeJoin is null").select([name for name in schemaString.split(",")])

n_nao_localizados = sinn01_nao_encontraram.count()

sinn01_df_gravar = sinn01_encontraram_final.unionAll(sinn01_nao_encontraram).unionAll(sinn01_encontraram_antes)

sinn01_df_gravar.registerTempTable("sinn01_df_gravar")

if n_nao_localizados > 0:
	mensagem = str(n_nao_localizados) + " Registros nao localizados no NFPRE"
	#grava_log("ERRO","111022",mensagem)
	grava_log("ERRO",cod_programa + cod_erro["REGISTRO_NAO_LOCALIZADOS_NFPRE"],mensagem)

##Define os campos e os tipos para o formato da tabela final 

sqlContext.sql(create_TMP_TB_NFC_OITV)

sinn01_df_gravar.write.mode("append").partitionBy("dt").saveAsTable(db_stg+".TMP_TB_NFC_OITV", format="parquet")

sqlContext.sql("drop table "+db_prepago+".TB_NFC_OITV")

sqlContext.sql("ALTER TABLE "+db_stg+".TMP_TB_NFC_OITV RENAME TO "+db_prepago+".TB_NFC_OITV")


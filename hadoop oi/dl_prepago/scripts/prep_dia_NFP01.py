###   Nome do programa: prep_dia_NFP01
###
###   O seguinte programa le os arquivos da da interface NFP01, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc.addPyFile(conf_path+"/conf_prep_dia_NFP01.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_NFP01 import *

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

def get_tipo_consumo(IND_TELECOM,TIPO_CDR,ORIGEM):
	if IND_TELECOM == "Nao Telecom" and ORIGEM in cod_oi_tv:
		return "Oi TV"
	elif IND_TELECOM == "Nao Telecom" and TIPO_CDR == "SMS":
		return "SVA"
	else:
		return IND_TELECOM

##Mensagem erro de um registro na tabela de log

def to_tb_log(linha):
	#[dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo,dt]
	
	dh_ocorrencia = dt_execucao
	
	tp_ocorrencia = "ERRO"
	
	cod_ocorrencia = cod_programa + cod_erro["REGISTRO_DUPLICADO"]
	
	mensagem = "NFC numero "+linha[2]+" encontrada em duplicidade"
	
	nome_arquivo = arquivo
	
	dt = str(dh_ocorrencia)[:10]
	
	return (dh_ocorrencia, tp_ocorrencia, cod_ocorrencia, modulo, mensagem, nome_arquivo,cod_programa,dt)

fieldsLog = [StructField(field_name, StringType(), True) for field_name in schemaLog.split(';')]

fieldsLog[0].dataType = TimestampType()

schemaLog = StructType(fieldsLog)

conf.setAppName(nome) 

dir_arquivo = dir_processamento+arquivo

nfp01 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "DATA_HORA_CONSUMO;DATA_HORA_NF;NUMERO_NF;HASH_CODE;SERIE;SUBSERIE;CFOP;MSISDN;NOME;CPF_CNPJ;IND_TELECOM;TIPO_CDR;TGC;ORIGEM;VALOR;VALOR_ICMS;ALIQUOTA;UF;ID_RECARGA;MSISDN_B;NUMERO_ITEM_NF;ID_CONSUMIDOR;DATA_HORA_EXTRACAO;TIPO_CONSUMO;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[1].dataType = TimestampType()
fields[14].dataType = DecimalType(10,2)
fields[15].dataType = DecimalType(10,2)
fields[16].dataType = DecimalType(10,2)

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes
def nfp01_map(x):
   try:
     return (valida_data(x[0]), valida_data_nula(x[1]), x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], valida_telecom(x[10]), x[11], x[12], x[13], valida_valor_divide(x[14]), valida_valor_nulo_ou_divide(x[15]), valida_valor_nulo(x[16]), x[17], x[18], x[19], x[20], x[21],x[22],get_tipo_consumo(x[10],x[11],x[13]),dt_execucao, str(valida_data(x[0]))[:10])
   except IndexError:
     linha = ""
     for campo in x:
       linha += campo+";"
     return [linha] #precisa ser array por conta da funcao str_rej

nfp01_temp = nfp01.map(lambda k: k.split(";")).map(nfp01_map).cache()

nfp01_rej_estrutura = nfp01_temp.filter(lambda x: len(x) <= 1)

registra_erros(nfp01_rej_estrutura," ","ESTRUTURA_INVALIDA")

nfp01_ace = nfp01_temp.filter(lambda x : len(x) > 1)

nfp01_rej_dh_consumo = nfp01_temp.filter(lambda x: type(x[0]) is unicode)

registra_erros(nfp01_rej_dh_consumo,"DATA_HORA_CONSUMO","DATA_HORA_CONSUMO_INVALIDA")

nfp01_ace = nfp01_temp.filter(lambda x : type(x[0]) is not unicode)

nfp01_rej_tempo_guarda = nfp01_ace.filter(lambda x: x[0] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(nfp01_rej_tempo_guarda,"DATA_HORA_CONSUMO","REGISTRO_FORA_TEMPO_GUARDA")

nfp01_ace = nfp01_ace.filter(lambda x : x[0] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

nfp01_rej_antigos = nfp01_ace.filter(lambda x: x[0] < valida_data(data_producao))

registra_erros(nfp01_rej_antigos,"DATA_HORA_CONSUMO","REGISTRO_ANTIGO")

nfp01_ace = nfp01_ace.filter(lambda x : x[0] >= valida_data(data_producao))

nfp01_rej_data_hora_nf = nfp01_ace.filter(lambda x: type(x[1]) is unicode)

registra_erros(nfp01_rej_data_hora_nf,"DATA_HORA_NF","DATA_HORA_NF_INVALIDA")

nfp01_ace = nfp01_ace.filter(lambda x : type(x[1]) is not unicode)

nfp01_rej_numero_nf = nfp01_ace.filter(lambda x : x[2] == "" and x[10] == "Telecom" )

registra_erros(nfp01_rej_numero_nf,"NUMERO_NF","NUMERO_NF_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : x[2] != "" or x[10] != "Telecom")	

nfp01_rej_hash_code = nfp01_ace.filter(lambda x : x[3] == "" and x[10] == "Telecom" )

registra_erros(nfp01_rej_hash_code,"HASH_CODE","HASH_CODE_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : x[3] != "" or x[10] != "Telecom")	

nfp01_rej_serie = nfp01_ace.filter(lambda x : (x[4] == "" and x[10] == "Telecom"))

registra_erros(nfp01_rej_serie,"SERIE","SERIE_INVALIDA")

nfp01_ace = nfp01_ace.filter(lambda x : x[4] != "" or x[10] != "Telecom")	

nfp01_rej_subserie = nfp01_ace.filter(lambda x : x[5] == "" and x[10] == "Telecom" )

registra_erros(nfp01_rej_subserie,"SUBSERIE","SUBSERIE_INVALIDA")

nfp01_ace = nfp01_ace.filter(lambda x : x[5] != "" or x[10] != "Telecom")

nfp01_rej_cfop = nfp01_ace.filter(lambda x : x[6] == "" and x[10] == "Telecom" )

registra_erros(nfp01_rej_cfop,"CFOP","CFOP_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : x[6] != "" or x[10] != "Telecom")	

nfp01_rej_msisdn = nfp01_ace.filter(lambda x : x[7] == "")

registra_erros(nfp01_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : x[7] != "")	

nfp01_rej_nome = nfp01_ace.filter(lambda x : x[8] == "")

registra_erros(nfp01_rej_nome,"NOME","NOME_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : x[8] != "")	

nfp01_rej_cpf_cnpj = nfp01_ace.filter(lambda x : x[9] == "")

registra_erros(nfp01_rej_cpf_cnpj,"CPF_CNPJ","CPF_CNPJ_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : x[9] != "")	

nfp01_rej_ind_telecom = nfp01_ace.filter(lambda x : x[10] not in ("Telecom", "Nao Telecom"))

registra_erros(nfp01_rej_ind_telecom,"IND_TELECOM","IND_TELECOM_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : x[10] in ("Telecom", "Nao Telecom"))	

nfp01_rej_tp_cdr = nfp01_ace.filter(lambda x : x[11] not in ("VOZ", "SMS", "MMS", "GPRS", "TAXAS"))

registra_erros(nfp01_rej_tp_cdr,"TIPO_CDR","TIPO_CDR_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : x[11] in ("VOZ", "SMS", "MMS", "GPRS", "TAXAS"))	

nfp01_rej_tgc = nfp01_ace.filter(lambda x : x[12] == "" and x[11] != "TAXAS" )

registra_erros(nfp01_rej_tgc,"TGC","TGC_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : x[12] != "" or x[11] == "TAXAS")	

nfp01_rej_origem = nfp01_ace.filter(lambda x : x[13] == "" and x[11] == "TAXAS")

registra_erros(nfp01_rej_origem,"ORIGEM","ORIGEM_INVALIDA")

nfp01_ace = nfp01_ace.filter(lambda x : x[13] != "" or x[11] != "TAXAS")	

nfp01_rej_valor = nfp01_ace.filter(lambda x: type(x[14]) is unicode)

registra_erros(nfp01_rej_valor,"VALOR","VALOR_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : type(x[14]) is not unicode)

nfp01_rej_valor_icms = nfp01_ace.filter(lambda x : x[15] == "" and x[10] == "Telecom" )

registra_erros(nfp01_rej_valor_icms,"VALOR_ICMS","VALOR_ICMS_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : x[16] != "" or x[10] != "Telecom")	

nfp01_rej_aliquota = nfp01_ace.filter(lambda x : x[16] == "" and x[10] == "Telecom" )

registra_erros(nfp01_rej_aliquota,"ALIQUOTA","ALIQUOTA_INVALIDA")

nfp01_ace = nfp01_ace.filter(lambda x : x[16] != "" or x[10] != "Telecom")	

nfp01_rej_uf = nfp01_ace.filter(lambda x : x[17] == "")

registra_erros(nfp01_rej_uf,"UF","UF_INVALIDA")

nfp01_ace = nfp01_ace.filter(lambda x : x[17] != "")

nfp01_rej_numero_item_nf = nfp01_ace.filter(lambda x : x[20] == "" and x[10] == "Telecom" )

registra_erros(nfp01_rej_numero_item_nf,"NUMERO_ITEM_NF","NUMERO_ITEM_NF_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : x[20] != "" or x[10] != "Telecom")

nfp01_rej_id_consumidor = nfp01_ace.filter(lambda x : x[21] == "")

registra_erros(nfp01_rej_id_consumidor,"ID_CONSUMIDOR","ID_CONSUMIDOR_INVALIDO")

nfp01_ace = nfp01_ace.filter(lambda x : x[21] != "")	

nfp01_df = sqlContext.createDataFrame(nfp01_ace, schema).cache()

##Agrupa registros por data_hora_consumo, numero_nf, msisdn, origem, valor e msisdn_b para verificar duplicidade

windowSpec = Window.partitionBy(nfp01_df['ID_RECARGA'],nfp01_df['DATA_HORA_CONSUMO'], nfp01_df['NUMERO_NF'], nfp01_df['MSISDN'], nfp01_df['ORIGEM'], nfp01_df['VALOR'], nfp01_df['MSISDN_B'])

nfp01_window = nfp01_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

nfp01_dup = nfp01_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

nfp01_df_uniq = nfp01_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

nfp01_old = sqlContext.sql("select ID_RECARGA,DATA_HORA_CONSUMO, NUMERO_NF, MSISDN, ORIGEM, VALOR, MSISDN_B, ID_CONSUMIDOR as testeJoin from "+db_prepago+".TB_NFC")

nfp01_join = nfp01_df_uniq.join(nfp01_old, ["ID_RECARGA","DATA_HORA_CONSUMO", "NUMERO_NF","MSISDN", "ORIGEM", "VALOR", "MSISDN_B"], "leftouter")

nfp01_dup2 = nfp01_join.filter("testejoin is not null")

nfp01_df_final = nfp01_join.filter("testeJoin is null").select([name for name in schemaString.split(";")])

nfp01_duplicados = nfp01_dup.unionAll(nfp01_dup2.select([name for name in schemaString.split(";")]))

#if nfp01_duplicados.count() > 0:
#	nfp01_duplicados_log = sqlContext.createDataFrame(nfp01_duplicados.map(to_tb_log)).withColumnRenamed("_7","dt")
#	nfp01_duplicados_log.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".tb_log", format="parquet")

n_dup = nfp01_duplicados.count()   ##Nao rejeitar registros do NFPRE
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO","110111",mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(nfp01_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])
    
##Os dados resultantes sao armazenados na tabela final

nfp01_df_final.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_NFC", format="parquet")

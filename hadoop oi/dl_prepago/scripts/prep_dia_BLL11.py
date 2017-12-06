###   Nome do programa: prep_dia_BLL11
###
###   O seguinte programa le os arquivos da interface bll11, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
###
###   Tambem verifica a duplicidade dos registros, tanto dentro do proprio arquivo quanto em relacao a tabela final
###
###   Os registros rejeitados na validacao serao gravados no diretorio de rejeicoes no HDFS
###
###   Os erros geram registros na tabela de log com a data de ocorrencia

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext, Row
from pyspark.sql.types import *  
from pyspark import SparkFiles
import sys

conf = SparkConf()

#conf.setMaster("yarn-client")

conf.setAppName("Valida BLL11") 

sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

data_processa = sys.argv[2]+"000000"
conf_path = sys.argv[1]
data_arquivo = sys.argv[4]

sc.addPyFile(conf_path+"/valida_lib.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/conf_prep_dia_BLL11.py")

from conf_prep_dia_BLL11 import *

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

def get_bolso(cd_tipo_produto,tipo_recarga):
	spl = tipo_recarga.split(".")
	tp_valor = int(spl[2])
	tp_requisicao = int(spl[1])
	cd_tipo_produto = int(cd_tipo_produto)
	if (cd_tipo_produto,tp_requisicao,tp_valor) in bolso_principal:
		return "1"
	if (cd_tipo_produto,tp_requisicao,tp_valor) in bolso_franquia:
		return cod_bolso_franquia
	return "0"

sqlContext.registerFunction("get_bolso",get_bolso)

#config.read(SparkFiles.get(conf_path))

dir_arquivo = dir_processamento+arquivo

bll11 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final

schemaString = "DH_EXTRACAO;MSISDN;VALOR_FACE;DT_SOLICITACAO_RECARGA;NSU_REQUISICAO;ID_RECARGA;DT_EFETIVACAO_RECARGA;PRODUTO_RECARGA;CANAL_RECARGA;SUB_CANAL;SALDO;CD_PRODUTO;CD_CAMPANHA;CD_TIPO_PRODUTO;TIPO_RECARGA;CD_PONTO_VENDA;COD_DISTRIBUIDOR_SAP;ORGANIZACAO_VENDA;SETOR_ATIVIDADE;CANAL_DISTRIBUICAO;FLAG_ACEITE_UPSELL;FLAG_UPSELL_HABILITADO;VALOR_ORIGINAL;MEIO_PAGAMENTO;MSISDN_PAGADOR;CD_CELL_ID;CD_AREA_CELL_ID;ID_PLANO_FAT;NUMERO_PEDIDO;DATA_SOLIC_ORDEM_VENDA;NUMERO_FATURA;DATA_HORA_FATURA;CNPJ_EMISSOR;NU_SERIE;NU_VOUCHER;ID_BOLSO;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[2].dataType = DecimalType(10,2)
fields[3].dataType = TimestampType()
fields[6].dataType = TimestampType()
fields[10].dataType = DecimalType(10,2)
fields[29].dataType = TimestampType()
fields[31].dataType = TimestampType()

schema = StructType(fields)

def bll_map(p):
	try:
		return (valida_data_traco(p[10]), p[0], valida_valor(p[1]), valida_data_traco(p[2]), p[3], p[3], valida_data_traco(p[5]), p[8], p[9], "", valida_valor_nulo(p[12]), p[14], p[18], p[15], p[16]+"."+p[7]+"."+p[4], p[19], "", "", "", "", "", "", "", "", "", "", "", "", "", valida_data("19600101000000"), "", valida_data("19600101000000"), "","","","", dt_execucao, str(valida_data_traco(p[2]))[:10])
	except IndexError:
		linha = ""
		for campo in p:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

##Mapeia os campos para o formato da tabela e inicia as validacoes

bll11_temp = bll11.map(lambda k: k.split(";")).map(bll_map).cache()

bll11_rej_estrutura = bll11_temp.filter(lambda x: len(x) <= 1)

registra_erros(bll11_rej_estrutura," ","ESTRUTURA_INVALIDA")

bll11_ace = bll11_temp.filter(lambda x : len(x) > 1)

bll11_rej_dh_extracao = bll11_ace.filter(lambda x: type(x[0]) is unicode)

registra_erros(bll11_rej_dh_extracao,"DH_EXTRACAO","DH_EXTRACAO_INVALIDA")

bll11_ace = bll11_ace.filter(lambda x : type(x[0]) is not unicode)

bll11_rej_msisdn = bll11_ace.filter(lambda x : x[1] == "")

registra_erros(bll11_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

bll11_ace = bll11_ace.filter(lambda x : x[1] != "")	

bll11_rej_vl_face = bll11_ace.filter(lambda x: type(x[2]) is unicode)

registra_erros(bll11_rej_vl_face,"VALOR_FACE","VALOR_FACE_INVALIDO")

bll11_ace = bll11_ace.filter(lambda x : type(x[2]) is not unicode)

bll11_rej_id_recarga = bll11_ace.filter(lambda x : x[5] == "")

registra_erros(bll11_rej_id_recarga,"ID_RECARGA","ID_RECARGA_INVALIDO")

bll11_ace = bll11_ace.filter(lambda x : x[5] != "")

bll11_rej_dt_efetivacao_recarga = bll11_ace.filter(lambda x: type(x[6]) is unicode)

registra_erros(bll11_rej_dt_efetivacao_recarga,"DT_EFETIVACAO_RECARGA","DT_EFETIVACAO_RECARGA_INVALIDA")

bll11_ace = bll11_ace.filter(lambda x : type(x[6]) is not unicode)

bll11_rej_tempo_guarda = bll11_ace.filter(lambda x: x[6] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(bll11_rej_tempo_guarda,"DT_EFETIVACAO_RECARGA","REGISTRO_FORA_TEMPO_GUARDA")

bll11_ace = bll11_ace.filter(lambda x : x[6] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

bll11_rej_antigos = bll11_ace.filter(lambda x: x[6] < valida_data(data_producao))

registra_erros(bll11_rej_antigos,"DT_EFETIVACAO_RECARGA","REGISTRO_ANTIGO")

bll11_ace = bll11_ace.filter(lambda x : x[6] >= valida_data(data_producao))

bll11_df = sqlContext.createDataFrame(bll11_ace, schema).cache() 

##Agrupa registros por id_recarga e dt_efetivacao_recarga para verificar duplicidade

windowSpec = Window.partitionBy(bll11_df['ID_RECARGA'], bll11_df['DT_EFETIVACAO_RECARGA'])

bll11_window = bll11_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

bll11_dup = bll11_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

bll11_df_uniq = bll11_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

#bll11_old_ps = sqlContext.sql("select ID_RECARGA, DT_EFETIVACAO_RECARGA, ID_RECARGA as testeJoin from "+db_stg+".STG_PP_RECARGA_PS")
#bll11_old = bll11_old_ps

bll11_old = sqlContext.sql("select ID_RECARGA, DT_EFETIVACAO_RECARGA, ID_RECARGA as testeJoin from "+db_prepago+".TB_RECARGA")

bll11_join = bll11_df_uniq.join(bll11_old, ["ID_RECARGA", "DT_EFETIVACAO_RECARGA"], "leftouter")

bll11_dup2 = bll11_join.filter("testejoin is not null")

schema_final = "DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,PRODUTO_RECARGA,\
CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,\
SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,Valor_Original_Recarga,Meio_de_Pagamento,Meio_de_Acesso_pagador,\
CD_CELL_ID,CD_AREA_CELL_ID,ID_PLANO_FAT,NUMERO_PEDIDO,DATA_SOLIC_ORDEM_VENDA,NUM_FATURA,DATA_HORA_FATURA,CNPJ_EMISSOR,NU_SERIE,NU_VOUCHER,DT_EXECUCAO,dt"

bll11_df_final = bll11_join.filter("testeJoin is null").select([name for name in schemaString.split(";")])

bll11_duplicados = bll11_dup.unionAll(bll11_dup2.select([name for name in schemaString.split(";")]))

n_dup = bll11_duplicados.count()

if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(bll11_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])

bll11_df_final.registerTempTable("bll11_df_final")

# Adiciona bolso

bll11_df_final = sqlContext.sql("select DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,\
PRODUTO_RECARGA,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,\
ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,VALOR_ORIGINAL,MEIO_PAGAMENTO,MSISDN_PAGADOR,\
CD_CELL_ID,CD_AREA_CELL_ID,ID_PLANO_FAT,NUMERO_PEDIDO,DATA_SOLIC_ORDEM_VENDA,NUMERO_FATURA,DATA_HORA_FATURA,CNPJ_EMISSOR,NU_SERIE,NU_VOUCHER,\
get_bolso(cd_tipo_produto,tipo_recarga) AS ID_BOLSO,DT_EXECUCAO,dt from bll11_df_final")

bll11_df_final_ps = bll11_df_final.filter("PRODUTO_RECARGA != %s" %(cod_origem_emprestimo))

bll11_df_final_rec = bll11_df_final.filter("PRODUTO_RECARGA = %s" %(cod_origem_emprestimo))

bll11_recarga = sqlContext.sql("select DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,\
PRODUTO_RECARGA,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,\
SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,VALOR_ORIGINAL,MEIO_PAGAMENTO,MSISDN_PAGADOR,CD_CELL_ID,CD_AREA_CELL_ID,\
NUMERO_FATURA,DATA_HORA_FATURA,CNPJ_EMISSOR,NU_SERIE,NU_VOUCHER,get_bolso(cd_tipo_produto,tipo_recarga) as ID_BOLSO,dt_execucao, dt from bll11_df_final") 

##Os dados resultantes sao armazenados na tabela final

bll11_df_final_ps.write.mode("append").partitionBy("dt").saveAsTable(db_stg+".STG_PP_RECARGA_PS", format="parquet")

bll11_df_final_rec.write.mode("append").partitionBy("dt").saveAsTable(db_stg+".STG_PP_RECARGA_REC", format="parquet")

bll11_recarga.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_RECARGA", format="parquet")

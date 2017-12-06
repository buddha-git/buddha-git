###   Nome do programa: prep_dia_FORTUNA06
###
###   O seguinte programa le os arquivos da interface FORTUNA06, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela de stage
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

sc.addPyFile(conf_path+"/conf_carga_FORTUNA06.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_carga_FORTUNA06 import *

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

conf.setAppName("Valida FORTUNA06")

dir_arquivo = dir_processamento+arquivo

fortuna06 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "CD_DISTRIBUIDOR_SAP;CD_ORGANIZACAO_VENDA;CD_CANAL_SAP;CD_SETOR_ATIVIDADE;VL_SALDO;DH_EXTRACAO;DT_EXECUCAO"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[4].dataType = DecimalType(10,2)
fields[5].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes
def for_map(x):
	try:
		return (x[0], x[1], x[2], x[3], valida_valor(x[4]), valida_data(x[5]),dt_execucao)
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

##Mapeia os campos para o formato da tabela e inicia as validacoes

fortuna06_temp = fortuna06.map(lambda k: k.split(";")).map(for_map).cache()

fortuna06_rej_estrutura = fortuna06_temp.filter(lambda x: len(x) <= 1)

registra_erros(fortuna06_rej_estrutura," ","ESTRUTURA_INVALIDA")

fortuna06_ace = fortuna06_temp.filter(lambda x: len(x) > 1)

fortuna06_rej_cd_distribuidor_sap = fortuna06_ace.filter(lambda x : x[0] == "")

registra_erros(fortuna06_rej_cd_distribuidor_sap,"CD_DISTRIBUIDOR_SAP","CD_DISTRIBUIDOR_SAP_INVALIDO")

fortuna06_ace = fortuna06_ace.filter(lambda x : x[0] != "")	

fortuna06_rej_cd_org_vendas = fortuna06_ace.filter(lambda x : x[1] == "")

registra_erros(fortuna06_rej_cd_org_vendas,"CD_ORGANIZACAO_VENDA","CD_ORGANIZACAO_VENDA_INVALIDO")

fortuna06_ace = fortuna06_ace.filter(lambda x : x[1] != "")

fortuna06_rej_cd_canal_sap = fortuna06_ace.filter(lambda x : x[2] == "")

registra_erros(fortuna06_rej_cd_canal_sap,"CD_CANAL_SAP","CD_CANAL_SAP_INVALIDO")

fortuna06_ace = fortuna06_ace.filter(lambda x : x[2] != "")

fortuna06_rej_cd_setor_atividade = fortuna06_ace.filter(lambda x : x[3] == "")

registra_erros(fortuna06_rej_cd_setor_atividade,"CD_SETOR_ATIVIDADE","CD_SETOR_ATIVIDADE_INVALIDO")

fortuna06_ace = fortuna06_ace.filter(lambda x : x[3] != "")

fortuna06_rej_vl_saldo = fortuna06_ace.filter(lambda x: type(x[4]) is unicode)

registra_erros(fortuna06_rej_vl_saldo,"VL_SALDO","VL_SALDO_INVALIDO")

fortuna06_ace = fortuna06_ace.filter(lambda x : type(x[4]) is not unicode)

fortuna06_rej_dh_extracao = fortuna06_ace.filter(lambda x: type(x[5]) is unicode)

registra_erros(fortuna06_rej_dh_extracao,"DH_EXTRACAO","DH_EXTRACAO_INVALIDA")

fortuna06_ace = fortuna06_ace.filter(lambda x : type(x[5]) is not unicode)

fortuna06_df = sqlContext.createDataFrame(fortuna06_ace, schema).cache() 

##Agrupa registros por cd_distibuidor_sap, cd_organizacao_venda, cd_canal_sap e cd_setor_atividade para verificar duplicidade

windowSpec = Window.partitionBy(fortuna06_df['CD_DISTRIBUIDOR_SAP'])

fortuna06_window = fortuna06_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

fortuna06_duplicados = fortuna06_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

n_dup = fortuna06_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(fortuna06_duplicados, dir_rejeitado+arquivo+"."+dt_execucao_formatada+"."+cod_programa + cod_erro["REGISTRO_DUPLICADO"])

fortuna06_df_uniq = fortuna06_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

#fortuna06_df_final.write.mode("append").saveAsTable("STG_PP_NFV_NCF", format="parquet")

fortuna06_df_uniq.registerTempTable("fortuna06_df_uniq")

dt = str(valida_data(data_producao))[:10]

##Define os campos e os tipos para o formato da tabela final 

fortuna06_final = sqlContext.sql("select CD_DISTRIBUIDOR_SAP, CD_ORGANIZACAO_VENDA, CD_CANAL_SAP, CD_SETOR_ATIVIDADE, VL_SALDO, DH_EXTRACAO, dt_execucao,'"+ dt+ "' as dt from fortuna06_df_uniq")

fortuna06_final.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_SALDO_DISTRIBUIDOR", format="parquet")

fortuna06_gravar_aux = sqlContext.sql("select VL_SALDO as valor_remanescente,'"+id_recarga_saldo_inicial+"' as numero_nf,'"+id_recarga_saldo_inicial+"' as num_ordem_venda,DH_EXTRACAO \
as data_hora_nf,'' as id_item_nf,VL_SALDO as valor_item_nf, VL_SALDO as valor_nf,0 as valor_material,CD_DISTRIBUIDOR_SAP as \
cod_distribuidor_sap,CD_ORGANIZACAO_VENDA as organizacao_venda,CD_SETOR_ATIVIDADE as setor_atividade,CD_CANAL_SAP as canal_distribuicao,\
substr(CD_ORGANIZACAO_VENDA,3,2) as UF,'' as tipo_ordem,'' as material,''as desc_material,'' as serie,'' as subserie,'' as cfop,\
'' as num_pedido,'VAL' as status,false as in_evt_sistemico_nfv,dt_execucao,'"+ dt+ "' as dt from fortuna06_df_uniq where VL_SALDO > 0")

fortuna06_gravar_aux.write.mode("append").partitionBy("dt").saveAsTable(db_sva+".tb_aux_nfv_cv", format="parquet")

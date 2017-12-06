###   Nome do programa: prep_dia_SAP03
###
###   O seguinte programa le os arquivos da interface SAP03, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc.addPyFile(conf_path+"/conf_prep_dia_SAP03.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_SAP03 import *

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

conf.setAppName("Valida SAP03") 

dir_arquivo = dir_processamento+arquivo

sap03 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "DH_EXTRACAO;CD_DISTRIBUIDOR_SAP;NO_FANTASIA;CD_CNPJ;CD_ORGANIZACAO_VENDA;DS_ORGANIZACAO_VENDA;CD_CANAL_SAP;DS_CANAL_SAP;CD_SETOR_ATIVIDADE;DS_SETOR_ATIVIDADE;DS_TP_DISTRIBUIDOR;DS_LOGRADOURO;NU_ENDERECO;DS_COMPLEMENTO;DS_CIDADE;DS_BAIRRO;DS_ESTADO;NU_CEP;IN_ELIMINACAO;DH_ELIMINACAO;DH_ATUALIZACAO;Modelo_de_Contrato;Centro_de_Distribuidor;Empresa;DT_EXECUCAO"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[20].dataType = TimestampType()
#fields[24].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes

def sap_map(p):
	try:
		return (valida_data(p[0]), str(int(p[1])), p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19], valida_data(p[20]), p[21], p[22], p[23],dt_execucao)
	except:
		linha = ""
		for campo in p:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

sap03_temp = sap03.map(lambda k: k.split(";")).map(sap_map).cache()

sap_rej_estrutura = sap03_temp.filter(lambda x: len(x) <= 1)

registra_erros(sap_rej_estrutura," ","ESTRUTURA_INVALIDA")

sap03_ace = sap03_temp.filter(lambda x : len(x) > 1)

sap03_rej_dh_extracao = sap03_ace.filter(lambda x: type(x[0]) is unicode)

registra_erros(sap03_rej_dh_extracao,"DH_EXTRACAO","DH_EXTRACAO_INVALIDA")

sap03_ace = sap03_ace.filter(lambda x : type(x[0]) is not unicode)

sap03_rej_cod_distribuidor_sap = sap03_ace.filter(lambda x : x[1] == "")

registra_erros(sap03_rej_cod_distribuidor_sap,"COD_DISTRIBUIDOR_SAP","COD_DISTRIBUIDOR_SAP_INVALIDO")

sap03_ace = sap03_ace.filter(lambda x : x[1] != "")	

sap03_rej_no_fantasia = sap03_ace.filter(lambda x : x[2] == "")

registra_erros(sap03_rej_no_fantasia,"NO_FANTASIA","NO_FANTASIA_INVALIDO")

sap03_ace = sap03_ace.filter(lambda x : x[2] != "")	

sap03_rej_cd_cnpj = sap03_ace.filter(lambda x : x[3] == "")

registra_erros(sap03_rej_cd_cnpj,"CD_CNPJ","CD_CNPJ_INVALIDO")

sap03_ace = sap03_ace.filter(lambda x : x[3] != "")	

sap03_rej_cd_org_vendas = sap03_ace.filter(lambda x : x[4] == "")

registra_erros(sap03_rej_cd_org_vendas,"CD_ORG_VENDAS","CD_ORGANIZACAO_VENDA_INVALIDO")

sap03_ace = sap03_ace.filter(lambda x : x[4] != "")

sap03_rej_cd_canal_sap = sap03_ace.filter(lambda x : x[6] == "")

registra_erros(sap03_rej_cd_canal_sap,"CD_CANAL_SAP","CD_CANAL_SAP_INVALIDO")

sap03_ace = sap03_ace.filter(lambda x : x[6] != "")

sap03_rej_cd_setor_atividade = sap03_ace.filter(lambda x : x[8] == "")

registra_erros(sap03_rej_cd_setor_atividade,"CD_SETOR_ATIVIDADE","CD_SETOR_ATIVIDADE_INVALIDO")

sap03_ace = sap03_ace.filter(lambda x : x[8] != "")

sap03_rej_ds_estado = sap03_ace.filter(lambda x : x[16] == "")

registra_erros(sap03_rej_ds_estado,"DS_ESTADO","DS_ESTADO_INVALIDO")

sap03_ace = sap03_ace.filter(lambda x : x[16] != "")

sap03_rej_dh_atualizacao = sap03_ace.filter(lambda x: type(x[20]) is unicode)

registra_erros(sap03_rej_dh_atualizacao,"DH_ATUALIZACAO","DH_ATUALIZACAO_INVALIDA")

sap03_ace = sap03_ace.filter(lambda x : type(x[20]) is not unicode)

sap03_rej_modelo_contrato = sap03_ace.filter(lambda x : x[21] == "")

registra_erros(sap03_rej_modelo_contrato,"Modelo de Contrato","MODELO_CONTRATO_INVALIDO")

sap03_ace = sap03_ace.filter(lambda x : x[21] != "")

sap03_df = sqlContext.createDataFrame(sap03_ace, schema).cache()

#sap03_old = sqlContext.sql("select DH_EXTRACAO as DH_EXTRACAO02,CD_DISTRIBUIDOR_SAP,NO_FANTASIA as NO_FANTASIA02,CD_CNPJ as CD_CNPJ02,CD_ORGANIZACAO_VENDA,DS_ORGANIZACAO_VENDA as DS_ORGANIZACAO_VENDA02,CD_CANAL_SAP,DS_CANAL_SAP as DS_CANAL_SAP02,CD_SETOR_ATIVIDADE,DS_SETOR_ATIVIDADE as DS_SETOR_ATIVIDADE02,DS_TP_DISTRIBUIDOR as DS_TP_DISTRIBUIDOR02,DS_LOGRADOURO as DS_LOGRADOURO02,NU_ENDERECO as NU_ENDERECO02,DS_COMPLEMENTO as DS_COMPLEMENTO02,DS_CIDADE as DS_CIDADE02,DS_BAIRRO as DS_BAIRRO02,DS_ESTADO as DS_ESTADO02,NU_CEP as NU_CEP02,IN_ELIMINACAO as IN_ELIMINACAO02,DH_ELIMINACAO as DH_ELIMINACAO02,DH_ATUALIZACAO as DH_ATUALIZACAO02,Modelo_de_Contrato as Modelo_de_Contrato02,Centro_de_Distribuidor as Centro_de_Distribuidor02,Empresa as Empresa02, DT_EXECUCAO as DT_EXECUCAO02 from "+db_prepago+".TB_DISTRIBUIDORES")

#sap03_df_union = sap03_df.unionAll(sap03_old)

##Agrupa registros por cd_distribuidor_sap, cd_organizacao_venda, cd_canal_sap, cd_setor_atividade e dh_atualizacao para verificar duplicidade

windowSpec = Window.partitionBy(sap03_df['CD_DISTRIBUIDOR_SAP'], sap03_df['CD_ORGANIZACAO_VENDA'], sap03_df['CD_CANAL_SAP'], sap03_df['CD_SETOR_ATIVIDADE']).orderBy(sap03_df['DH_ATUALIZACAO'].desc())

sap03_window = sap03_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

sap03_dup = sap03_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

sap03_df_uniq = sap03_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

#def get_mais_recente(linha):
#sap03_join = sap03_df_uniq.join(sap03_old, ["CD_DISTRIBUIDOR_SAP", "CD_ORGANIZACAO_VENDA", "CD_CANAL_SAP", "CD_SETOR_ATIVIDADE"], "leftouter")
###sap_03_mais_recente = sap03_join.map(get_mais_recente)

#sap03_dup2 = sap03_join.filter("DH_EXTRACAO02 is not null")

#sap03_df_final = sap03_join.filter("DH_EXTRACAO02 is null").select([name for name in schemaString.split(";")])

sap03_df_final = sap03_df_uniq

sap03_duplicados = sap03_dup #.unionAll(sap03_dup2.select([name for name in schemaString.split(";")]))

n_dup = sap03_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(sap03_duplicados, dir_rejeitado+arquivo+"."+dt_execucao_formatada+"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])

##Define os campos e os tipos para o formato da tabela final 

sqlContext.sql(create_TMP_TB_DISTRIBUIDORES)

sap03_df_final.write.mode("append").format("parquet").saveAsTable(db_stg+".TMP_TB_DISTRIBUIDORES")

sqlContext.sql("drop table "+db_prepago+".TB_DISTRIBUIDORES")

sqlContext.sql("ALTER TABLE "+db_stg+".TMP_TB_DISTRIBUIDORES RENAME TO "+db_prepago+".TB_DISTRIBUIDORES")

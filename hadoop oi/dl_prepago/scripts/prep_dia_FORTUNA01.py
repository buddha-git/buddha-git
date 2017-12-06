###   Nome do programa: prep_dia_FORTUNA01
###
###   O seguinte programa le os arquivos da interface FORTUNA01, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

conf.setAppName("Valida FORTUNA01") 

sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

# Seta parametros

data_processa = sys.argv[2]+"000000"
conf_path = sys.argv[1]
data_arquivo = sys.argv[4]

# Adiciona arquivos de configuracao

sc.addPyFile(conf_path+"/conf_prep_dia_FORTUNA01.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_prep_dia_FORTUNA01 import *

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

#config.read(SparkFiles.get(conf_path))

dir_arquivo = dir_processamento+arquivo

# Le arquivo do HDFS

fortuna01 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final

schemaString = "DH_EXTRACAO;MSISDN;VALOR_FACE;DT_SOLICITACAO_RECARGA;NSU_REQUISICAO;ID_RECARGA;DT_EFETIVACAO_RECARGA;PRODUTO_RECARGA;CANAL_RECARGA;SUB_CANAL;SALDO;CD_PRODUTO;CD_CAMPANHA;CD_TIPO_PRODUTO;TIPO_RECARGA;CD_PONTO_VENDA;COD_DISTRIBUIDOR_SAP;ORGANIZACAO_VENDA;SETOR_ATIVIDADE;CANAL_DISTRIBUICAO;NUM_FATURA;DATA_HORA_FATURA;CNPJ_EMISSOR;Flag_Aceite_Upsell;Flag_Upsell_habilitado;Valor_Original_Recarga;Meio_de_Pagamento;Meio_de_Acesso_pagador;NU_SERIE;NU_VOUCHER;CD_CELL_ID;CD_AREA_CELL_ID;ID_PLANO_FAT;NUMERO_PEDIDO;DATA_SOLIC_ORDEM_VENDA;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[2].dataType = DecimalType(10,2)
fields[3].dataType = TimestampType()
fields[6].dataType = TimestampType()
fields[10].dataType = DecimalType(10,2)
fields[21].dataType = TimestampType()
#fields[32].dataType = DecimalType(10,2) antigo valor fatura
fields[34].dataType = TimestampType()
#fields[35].dataType = TimestampType()

schema = StructType(fields)

def for_map(p):
	try:
		return (valida_data(p[0]), p[1], valida_valor(p[2]), valida_data(p[3]), p[4], p[5], valida_data(p[6]), p[7], p[8], p[9], valida_valor(p[10]), p[11], p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19], p[20], valida_dt_fatura(p[21]), p[22], p[23], p[24], p[25], p[26], p[27], remove_zeros_serie(p[28]), p[29], p[30], p[31], "", "", valida_data("19600101000000"), dt_execucao, str(valida_data(p[6]))[:10])
	except IndexError:
		linha = ""
		for campo in p:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

##Mapeia os campos para o formato da tabela e inicia as validacoes

fortuna01_temp = fortuna01.map(lambda k: k.split(";")).map(for_map).cache()

fortuna01_rej_estrutura = fortuna01_temp.filter(lambda x: len(x) <= 1)

registra_erros(fortuna01_rej_estrutura," ","ESTRUTURA_INVALIDA")

fortuna01_ace = fortuna01_temp.filter(lambda x : len(x) > 1)

fortuna01_rej_dh_extracao = fortuna01_ace.filter(lambda x: type(x[0]) is unicode)

registra_erros(fortuna01_rej_dh_extracao,"DH_EXTRACAO","DH_EXTRACAO_INVALIDA")

fortuna01_ace = fortuna01_ace.filter(lambda x : type(x[0]) is not unicode)

fortuna01_rej_msisdn = fortuna01_ace.filter(lambda x : x[1] == "")

registra_erros(fortuna01_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

fortuna01_ace = fortuna01_ace.filter(lambda x : x[1] != "")	

fortuna01_rej_vl_face = fortuna01_ace.filter(lambda x: type(x[2]) is unicode)

registra_erros(fortuna01_rej_vl_face,"VALOR_FACE","VALOR_FACE_INVALIDO")

fortuna01_ace = fortuna01_ace.filter(lambda x : type(x[2]) is not unicode)

fortuna01_rej_id_recarga = fortuna01_ace.filter(lambda x : x[5] == "")

registra_erros(fortuna01_rej_id_recarga,"ID_RECARGA","ID_RECARGA_INVALIDO")

fortuna01_ace = fortuna01_ace.filter(lambda x : x[5] != "")

fortuna01_rej_dt_efetivacao_recarga = fortuna01_ace.filter(lambda x: type(x[6]) is unicode)

registra_erros(fortuna01_rej_dt_efetivacao_recarga,"DT_EFETIVACAO_RECARGA","DT_EFETIVACAO_RECARGA_INVALIDA")

fortuna01_ace = fortuna01_ace.filter(lambda x : type(x[6]) is not unicode)

fortuna01_rej_tempo_guarda = fortuna01_ace.filter(lambda x: x[6] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(fortuna01_rej_tempo_guarda,"DT_EFETIVACAO_RECARGA","REGISTRO_FORA_TEMPO_GUARDA")

fortuna01_ace = fortuna01_ace.filter(lambda x : x[6] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

fortuna01_rej_antigos = fortuna01_ace.filter(lambda x: x[6] < valida_data(data_producao))

registra_erros(fortuna01_rej_antigos,"DT_EFETIVACAO_RECARGA","REGISTRO_ANTIGO")

fortuna01_ace = fortuna01_ace.filter(lambda x : x[6] >= valida_data(data_producao))

fortuna01_rej_saldo = fortuna01_ace.filter(lambda x: type(x[10]) is unicode)

registra_erros(fortuna01_rej_saldo,"SALDO","SALDO_INVALIDO")

fortuna01_ace = fortuna01_ace.filter(lambda x : type(x[10]) is not unicode)

fortuna01_rej_serie = fortuna01_ace.filter(lambda x: x[28][:4] == "erro")

registra_erros(fortuna01_rej_serie,"NU_SERIE","SERIE_INVALIDA")

fortuna01_ace = fortuna01_ace.filter(lambda x : x[28][:4] != "erro")

fortuna01_rej_tipo_recarga = fortuna01_ace.filter(lambda x : x[14] not in ("FIS", "BAT", "ELT"))

registra_erros(fortuna01_rej_tipo_recarga,"TIPO_RECARGA","TIPO_RECARGA_INVALIDO")

fortuna01_ace = fortuna01_ace.filter(lambda x : x[14] in ("FIS", "BAT", "ELT"))	

#fortuna01_rej_cod_distribuidor_sap = fortuna01_ace.filter(lambda x : x[16] == "")

#registra_erros(fortuna01_rej_cod_distribuidor_sap,"COD_DISTRIBUIDOR_SAP","106016")

#fortuna01_ace = fortuna01_ace.filter(lambda x : x[16] != "")	

#fortuna01_rej_organizacao_venda = fortuna01_ace.filter(lambda x : x[17] == "")

#registra_erros(fortuna01_rej_organizacao_venda,"ORGANIZACAO_VENDA","105017")

#fortuna01_ace = fortuna01_ace.filter(lambda x : x[17] != "")

#fortuna01_rej_setor_atividade = fortuna01_ace.filter(lambda x : x[18] == "")

#registra_erros(fortuna01_rej_setor_atividade,"SETOR_ATIVIDADE","105018")

#fortuna01_ace = fortuna01_ace.filter(lambda x : x[18] != "")

#fortuna01_rej_canal_distribuicao = fortuna01_ace.filter(lambda x : x[19] == "")

#registra_erros(fortuna01_rej_canal_distribuicao,"CANAL_DISTRIBUICAO","105019")

#fortuna01_ace = fortuna01_ace.filter(lambda x : x[19] != "")

# Cria dataframe a partir de registros nao rejeitados

fortuna01_df = sqlContext.createDataFrame(fortuna01_ace, schema).cache() 

##Agrupa registros por id_recarga e dt_efetivacao_recarga para verificar duplicidade

windowSpec = Window.partitionBy(fortuna01_df['ID_RECARGA'], fortuna01_df['DT_EFETIVACAO_RECARGA'])

fortuna01_window = fortuna01_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

fortuna01_dup = fortuna01_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

fortuna01_df_uniq = fortuna01_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

#fortuna01_old = sqlContext.sql("select ID_RECARGA, DT_EFETIVACAO_RECARGA, ID_RECARGA as teste from STG_PP_RECARGA_CV union select ID_RECARGA, DT_EFETIVACAO_RECARGA, ID_RECARGA as teste from STG_PP_RECARGA_FIS union select ID_RECARGA, DT_EFETIVACAO_RECARGA, ID_RECARGA as teste from STG_PP_RECARGA_REC union select ID_RECARGA, DT_EFETIVACAO_RECARGA, ID_RECARGA as teste from STG_PP_RECARGA_PS").withColumnRenamed("teste", "testeJoin")

# Le registros que ja estavam em tabela para verificar duplicidade

fortuna01_old_cv = sqlContext.sql("select ID_RECARGA, DT_EFETIVACAO_RECARGA, ID_RECARGA as testeJoin from "+db_stg+".STG_PP_RECARGA_CV")
fortuna01_old_fis = sqlContext.sql("select ID_RECARGA, DT_EFETIVACAO_RECARGA, ID_RECARGA as testeJoin from "+db_stg+".STG_PP_RECARGA_FIS")
fortuna01_old_ps = sqlContext.sql("select ID_RECARGA, DT_EFETIVACAO_RECARGA, ID_RECARGA as testeJoin from "+db_stg+".STG_PP_RECARGA_PS")
fortuna01_old_rec = sqlContext.sql("select ID_RECARGA, DT_EFETIVACAO_RECARGA, ID_RECARGA as testeJoin from "+db_stg+".STG_PP_RECARGA_REC")

fortuna01_old = fortuna01_old_cv.unionAll(fortuna01_old_fis).unionAll(fortuna01_old_ps).unionAll(fortuna01_old_rec)

#fortuna01_old = fortuna01_old.cache()
#fortuna01_old.count()

fortuna01_join = fortuna01_df_uniq.join(fortuna01_old, ["ID_RECARGA", "DT_EFETIVACAO_RECARGA"], "leftouter")

fortuna01_dup2 = fortuna01_join.filter("testejoin is not null")

schema_final = "DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,PRODUTO_RECARGA,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,Valor_Original_Recarga,Meio_de_Pagamento,Meio_de_Acesso_pagador,CD_CELL_ID,CD_AREA_CELL_ID,ID_PLANO_FAT,NUMERO_PEDIDO,DATA_SOLIC_ORDEM_VENDA,NUM_FATURA,DATA_HORA_FATURA,CNPJ_EMISSOR,NU_SERIE,NU_VOUCHER,DT_EXECUCAO,dt"

fortuna01_df_final = fortuna01_join.filter("testeJoin is null").select([name for name in schema_final.split(",")])

fortuna01_duplicados = fortuna01_dup.unionAll(fortuna01_dup2.select([name for name in schema_final.split(",")]))

n_dup = fortuna01_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(fortuna01_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])

#fortuna01_df_final.registerTempTable("fortuna01_df_final")

#!#!#!#adiciona bolso principal. alterar com a chegada do novo bolso

produto_recarga = sqlContext.sql("select cd_produto_recarga as PRODUTO_RECARGA, cd_direito from {db}.tb_produto_recarga where cd_direito in {cds_direito}".format(db = db_prepago,cds_direito = str(cd_direito_franquia+cd_direito_principal).replace("[","(").replace("]",")").format(db = db_prepago))).dropDuplicates(['PRODUTO_RECARGA'])

#assumindo que todas as recargas sao do bolso principal ou de franquia

fortuna01_df_final_join = fortuna01_df_final.join(produto_recarga,["PRODUTO_RECARGA"],"left_outer")

fortuna01_df_final_join.registerTempTable("fortuna01_df_final_join")

fortuna01_df_final = sqlContext.sql("select DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,PRODUTO_RECARGA,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,Valor_Original_Recarga,Meio_de_Pagamento,Meio_de_Acesso_pagador,CD_CELL_ID,CD_AREA_CELL_ID,ID_PLANO_FAT,NUMERO_PEDIDO,DATA_SOLIC_ORDEM_VENDA,NUM_FATURA,DATA_HORA_FATURA,CNPJ_EMISSOR,NU_SERIE,NU_VOUCHER,if(CD_DIREITO IN {cd_direito_franquia},2,1) AS ID_BOLSO,DT_EXECUCAO,dt from fortuna01_df_final_join".format(cd_direito_franquia = str(cd_direito_franquia).replace("[","(").replace("]",")")))

fortuna01_recarga = sqlContext.sql("select DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,PRODUTO_RECARGA,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,Valor_Original_Recarga as VALOR_ORIGINAL,Meio_de_Pagamento as MEIO_PAGAMENTO,Meio_de_Acesso_pagador as MSISDN_PAGADOR,CD_CELL_ID,CD_AREA_CELL_ID,NUM_FATURA as NUMERO_FATURA,DATA_HORA_FATURA,CNPJ_EMISSOR,NU_SERIE,NU_VOUCHER,if(CD_DIREITO IN {cd_direito_franquia},2,1) as ID_BOLSO,dt_execucao, dt from fortuna01_df_final_join".format(cd_direito_franquia = str(cd_direito_franquia).replace("[","(").replace("]",")"))) 

##Seleciona a estrutura final pelo tipo_recarga

fortuna01_df_bat = fortuna01_df_final.filter("TIPO_RECARGA = 'BAT'")

fortuna01_df_fis = fortuna01_df_final.filter("TIPO_RECARGA = 'FIS'")

fortuna01_df_elt = fortuna01_df_final.filter("TIPO_RECARGA = 'ELT'")

schema_bolso = "DH_EXTRACAO,MSISDN,VALOR_FACE,DT_SOLICITACAO_RECARGA,NSU_REQUISICAO,ID_RECARGA,DT_EFETIVACAO_RECARGA,PRODUTO_RECARGA,CANAL_RECARGA,SUB_CANAL,SALDO,CD_PRODUTO,CD_CAMPANHA,CD_TIPO_PRODUTO,TIPO_RECARGA,CD_PONTO_VENDA,COD_DISTRIBUIDOR_SAP,ORGANIZACAO_VENDA,SETOR_ATIVIDADE,CANAL_DISTRIBUICAO,FLAG_ACEITE_UPSELL,FLAG_UPSELL_HABILITADO,Valor_Original_Recarga,Meio_de_Pagamento,Meio_de_Acesso_pagador,CD_CELL_ID,CD_AREA_CELL_ID,ID_PLANO_FAT,NUMERO_PEDIDO,DATA_SOLIC_ORDEM_VENDA,NUM_FATURA,DATA_HORA_FATURA,CNPJ_EMISSOR,NU_SERIE,NU_VOUCHER,ID_BOLSO,DT_EXECUCAO,dt"

sap03_df = sqlContext.sql("select CD_DISTRIBUIDOR_SAP as COD_DISTRIBUIDOR_SAP, CD_CANAL_SAP as CANAL_DISTRIBUICAO, CD_SETOR_ATIVIDADE as SETOR_ATIVIDADE, CD_ORGANIZACAO_VENDA as ORGANIZACAO_VENDA, empresa as testeJoin from {db}.tb_distribuidores where modelo_de_contrato = 'Prest.Serv.Com Limit' or modelo_de_contrato = 'Prest.Serv.Sem Limit'".format(db = db_prepago))

fortuna01_df_elt_join = fortuna01_df_elt.join(sap03_df,["COD_DISTRIBUIDOR_SAP","CANAL_DISTRIBUICAO","SETOR_ATIVIDADE", "ORGANIZACAO_VENDA"],"left_outer")

# Separa recargas eletronicas em prestacao de servico e compra e venda

fortuna01_df_prest = fortuna01_df_elt_join.filter("testeJoin is not null").select([name for name in schema_bolso.split(",")])

fortuna01_df_cv = fortuna01_df_elt_join.filter("testeJoin is null").select([name for name in schema_bolso.split(",")])

##Os dados resultantes sao armazenados na tabela final

fortuna01_df_prest.write.mode("append").partitionBy("dt").saveAsTable(db_stg+".STG_PP_RECARGA_PS", format="parquet")

fortuna01_df_fis.write.mode("append").partitionBy("dt").saveAsTable(db_stg+".STG_PP_RECARGA_FIS", format="parquet")

fortuna01_df_bat.write.mode("append").partitionBy("dt").saveAsTable(db_stg+".STG_PP_RECARGA_REC", format="parquet")

fortuna01_df_cv.write.mode("append").partitionBy("dt").saveAsTable(db_stg+".STG_PP_RECARGA_CV", format="parquet")

fortuna01_recarga.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_RECARGA", format="parquet")

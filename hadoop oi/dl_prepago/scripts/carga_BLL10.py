###   Nome do programa: carga_BLL10
###
###   O seguinte programa le os arquivos da interface , de um dia especifico, realiza validacoes dos campos e grava os registros na tabela de stage
###
###   Tambem verifica a duplicidade dos registros, tanto dentro do proprio arquivo quanto em relacao a tabela de stage
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

sc.addPyFile(conf_path+"/conf_carga_BLL10.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_carga_BLL10 import *

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

conf.setAppName("Carga saldo cliente")

dir_arquivo = dir_processamento+arquivo

bll10 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "MSISDN;STATUS;PLANO_PRECO;DT_ATIVACAO;DT_EXPIRACAO_CREDITO;DT_DESATIVACAO;CD_OPLD;SALDOATUAL;DT_LAD;STATUS_DEDUZIDO;DT_MOVIMENTO;DT_ULTIMA_RECARGA;FG_EMPRESTIMO;QT_SALDO_OI_PREDILETO;VL_REC_MES_ANTERIOR;VL_REC_MES_ATUAL;SALDOFRANQUIA;VL_TAXA_PENDENTE;DT_TAXA_PENDENTE;IND_LIGADOR;ID_BENEFICIO_IN;ID_PROGRAMA_IN;CD_TIPO_PRODUTO;QTE_VEZES_NSE;DT_ADESAO_OFERTA_IN;NODE_IN;DT_EXPIRACAO_OFERTA_IN;DS_PLANO_PRECO_FIXA;DT_BOLSO_FRANQUIA_CTRL;SALDO_FRANQUIA_CTRL;DT_EXECUCAO;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[3].dataType = TimestampType()
fields[4].dataType = TimestampType()
fields[5].dataType = TimestampType()
fields[7].dataType = DecimalType(10,2)
fields[8].dataType = TimestampType()
fields[10].dataType = TimestampType()
fields[11].dataType = TimestampType()
fields[14].dataType = DecimalType(10,2)
fields[15].dataType = DecimalType(10,2)
fields[24].dataType = TimestampType()
fields[26].dataType = TimestampType()
fields[28].dataType = TimestampType()
fields[29].dataType = DecimalType(10,2)

schema = StructType(fields)

##Mapeia os campos para o formato da tabela e inicia as validacoes
def for_map(x):
	try:
		if len(x) == 25:
			return (x[0],x[1],x[2],valida_data_traco_nula(x[3]),valida_data_traco_nula(x[4]),valida_data_traco_nula(x[5]),x[6],valida_valor_divide(x[7]),valida_data_traco_nula(x[8]),x[9],valida_data_traco(x[10]),valida_data_traco_nula(x[11]),x[12],x[13],valida_valor_nulo(x[14]),valida_valor_nulo(x[15]),x[16],x[17],x[18],x[19],x[20],x[21],x[22],x[23],valida_data_traco("01-01-1960 00:00:00"),'',valida_data_traco("01-01-1960 00:00:00"),'',valida_data_traco("01-01-1960 00:00:00"),valida_valor("0"),dt_execucao, str(valida_data_traco(x[10]))[:10])
		else:
			return (x[0],x[1],x[2], valida_data_traco_nula(x[3]),valida_data_traco_nula(x[4]),valida_data_traco_nula(x[5]),x[6],valida_valor_divide(x[7]),valida_data_traco_nula(x[8]),x[9],valida_data_traco(x[10]),valida_data_traco_nula(x[11]),x[12],x[13],valida_valor_nulo(x[14]),valida_valor_nulo(x[15]),x[16],x[17],x[18],x[19],x[20],x[21],x[22],x[23],valida_data_traco_min(x[24]),x[25],valida_data_traco_min(x[26]),x[27],valida_data_traco_nula(x[28]),valida_valor_nulo(x[29]),dt_execucao, str(valida_data_traco(x[10]))[:10])
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

##Mapeia os campos para o formato da tabela e inicia as validacoes

bll10_temp = bll10.map(lambda k: k.split(";")).map(for_map).cache()

bll10_rej_estrutura = bll10_temp.filter(lambda x: len(x) <= 1)

registra_erros(bll10_rej_estrutura," ","ESTRUTURA_INVALIDA")

bll10_ace = bll10_temp.filter(lambda x: len(x) > 1)

bll10_rej_msisdn = bll10_ace.filter(lambda x : x[0] == "")

registra_erros(bll10_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

bll10_ace = bll10_ace.filter(lambda x : x[0] != "")	

bll10_rej_status = bll10_ace.filter(lambda x : x[1] not in ("DES","Active","APR", "NSE", "SE", "NCE", "CE", "Pre-Active", "TPL"))

registra_erros(bll10_rej_status,"STATUS","STATUS_INVALIDO")

bll10_ace = bll10_ace.filter(lambda x : x[1] in ("DES","Active","APR", "NSE", "SE", "NCE", "CE", "Pre-Active", "TPL"))

bll10_rej_dt_ativacao = bll10_ace.filter(lambda x: type(x[3]) is unicode)

registra_erros(bll10_rej_dt_ativacao,"DT_ATIVACAO","DT_ATIVACAO_INVALIDA")

bll10_ace = bll10_ace.filter(lambda x : type(x[3]) is not unicode)

bll10_rej_dt_expiracao_credito = bll10_ace.filter(lambda x: type(x[4]) is unicode)

registra_erros(bll10_rej_dt_expiracao_credito,"DT_EXPIRACAO_CREDITO","DT_EXPIRACAO_CREDITO_INVALIDA")

bll10_ace = bll10_ace.filter(lambda x : type(x[4]) is not unicode)

bll10_rej_dt_desativacao = bll10_ace.filter(lambda x: type(x[5]) is unicode)

registra_erros(bll10_rej_dt_desativacao,"DT_DESATIVACAO","DT_DESATIVACAO_INVALIDA")

bll10_ace = bll10_ace.filter(lambda x : type(x[5]) is not unicode)

bll10_rej_saldoatual = bll10_ace.filter(lambda x: type(x[7]) is unicode)

registra_erros(bll10_rej_saldoatual,"SALDOATUAL","SALDOATUAL_INVALIDO")

bll10_ace = bll10_ace.filter(lambda x : type(x[7]) is not unicode)

bll10_rej_dt_lad = bll10_ace.filter(lambda x: type(x[8]) is unicode)

registra_erros(bll10_rej_dt_lad,"DT_LAD","DT_LAD_INVALIDA")

bll10_ace = bll10_ace.filter(lambda x : type(x[8]) is not unicode)

bll10_rej_dt_movimento = bll10_ace.filter(lambda x: type(x[10]) is unicode)

registra_erros(bll10_rej_dt_movimento,"DT_MOVIMENTO","DT_MOVIMENTO_INVALIDA")

bll10_ace = bll10_ace.filter(lambda x : type(x[10]) is not unicode)

bll10_rej_dt_ultima_recarga = bll10_ace.filter(lambda x: type(x[11]) is unicode)

registra_erros(bll10_rej_dt_ultima_recarga,"DT_ULTIMA_RECARGA","DT_ULTIMA_RECARGA_INVALIDA")

bll10_ace = bll10_ace.filter(lambda x : type(x[11]) is not unicode)

bll10_rej_vl_rec_anterior = bll10_ace.filter(lambda x: type(x[14]) is unicode)

registra_erros(bll10_rej_vl_rec_anterior,"VL_REC_MES_ANTERIOR","VL_REC_MES_ANTERIOR_INVALIDO")

bll10_ace = bll10_ace.filter(lambda x : type(x[14]) is not unicode)

bll10_rej_vl_rec_mes_atual = bll10_ace.filter(lambda x: type(x[15]) is unicode)

registra_erros(bll10_rej_saldoatual,"VL_REC_MES_ATUAL","VL_REC_MES_ATUAL_INVALIDO")

bll10_ace = bll10_ace.filter(lambda x : type(x[15]) is not unicode)

bll10_rej_dt_bolso_franquia = bll10_ace.filter(lambda x: type(x[28]) is unicode)

registra_erros(bll10_rej_dt_bolso_franquia,"DT_BOLSO_FRANQUIA_CTRL","DT_BOLSO_FRANQUIA_CTRL_INVALIDA")

bll10_ace = bll10_ace.filter(lambda x : type(x[28]) is not unicode)

bll10_rej_saldo_franquia = bll10_ace.filter(lambda x: type(x[29]) is unicode)

registra_erros(bll10_rej_saldo_franquia,"SALDO_FRANQUIA_CTRL","SALDO_FRANQUIA_CTRL_INVALIDO")

bll10_ace = bll10_ace.filter(lambda x : type(x[29]) is not unicode)

bll10_df = sqlContext.createDataFrame(bll10_ace, schema).cache() 

##Agrupa registros por dt_movimento e msisdn ordenado pelo status_deduzido para verificar duplicidade

windowSpec = Window.partitionBy(bll10_df['DT_MOVIMENTO'], bll10_df['MSISDN']).orderBy(bll10_df['STATUS_DEDUZIDO']) 

bll10_window = bll10_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

#bll10_dup = bll10_window.filter("row_number <> 1").coalesce(num_partition).select([name for name in schemaString.split(";")])

#bll10_df_uniq = bll10_window.filter("row_number = 1").coalesce(num_partition).select([name for name in schemaString.split(";")])

#verificacao de duplicidade dentro da tabela desativada 

#bll10_old = sqlContext.sql("select MSISDN, DT_MOVIMENTO, CD_TIPO_PRODUTO as testeJoin from "+db_prepago+".TB_SALDO_CLIENTE").coalesce(num_partition)

#bll10_join = bll10_df_uniq.join(bll10_old, ["MSISDN", "DT_MOVIMENTO"], "leftouter").coalesce(num_partition)

#bll10_dup2 = bll10_join.filter("testejoin is not null")

#bll10_df_final = bll10_join.filter("testeJoin is null").select([name for name in schemaString.split(";")]).coalesce(num_partition)

bll10_duplicados = bll10_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

n_dup = bll10_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(bll10_duplicados, dir_rejeitado+arquivo+"."+dt_execucao_formatada+"."+cod_programa + cod_erro["REGISTRO_DUPLICADO"])

bll10_df_final = bll10_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

bll10_df_final.registerTempTable("bll10_df_final")

###bll10_gravar = sqlContext.sql("select concat(substr(dt_movimento,1,10),' 00:00:00') as DH_EXTRACAO,MSISDN,SALDOATUAL as VALOR_FACE,DT_MOVIMENTO as DT_SOLICITACAO_RECARGA,'' as NSU_REQUISICAO,'999999' as ID_RECARGA,DT_MOVIMENTO as DT_EFETIVACAO_RECARGA,'' as PRODUTO_RECARGA,'' as CANAL_RECARGA,'' as SUB_CANAL,SALDOATUAL as SALDO,'' as CD_PRODUTO,'' as CD_CAMPANHA,'' as CD_TIPO_PRODUTO,'' as TIPO_RECARGA,'' as CD_PONTO_VENDA,'' as COD_DISTRIBUIDOR_SAP,'' as ORGANIZACAO_VENDA,'' as SETOR_ATIVIDADE,'' as CANAL_DISTRIBUICAO,'' as FLAG_ACEITE_UPSELL,'' as FLAG_UPSELL_HABILITADO,SALDOATUAL as VALOR_ORIGINAL,'' as MEIO_PAGAMENTO,'' as MSISDN_PAGADOR,'' as CD_CELL_ID,'' as CD_AREA_CELL_ID,'' as ID_PLANO_FAT,'' as NUMERO_PEDIDO,DT_MOVIMENTO as DATA_SOLIC_ORDEM_VENDA,'' as NUMERO_FATURA,DT_MOVIMENTO as DATA_HORA_FATURA,'' as CNPJ_EMISSOR,'' as NU_SERIE,'' as NU_VOUCHER,'{dt_exec}' as dt_execucao,dt from bll10_df_final".format(dt_exec = str(dt_execucao)))

#verifica bolso informado e seleciona entre SALDOATUAL/SALDO_FRANQUIA_CTRL e DT_MOVIMENTO/DT_BOLSO_FRANQUIA_CTRL

if id_bolso == ["1","2"]:
	bll10_gravar_1 = sqlContext.sql("select SALDOATUAL as SALDO_RM_REC,MSISDN,SALDOATUAL as VALOR_FACE,DT_MOVIMENTO as DT_SOLICITACAO_RECARGA,'' as NSU_REQUISICAO,'{num}' as ID_RECARGA,DT_MOVIMENTO as DT_EFETIVACAO_RECARGA,'' as PRODUTO_RECARGA,SALDOATUAL as SALDO_CLIENTE, '' as TIPO_RECARGA,'' as CD_PONTO_VENDA,'' as COD_DISTRIBUIDOR_SAP,'' as ORGANIZACAO_VENDA,'' as SETOR_ATIVIDADE,'' as CANAL_DISTRIBUICAO,'' as NUMERO_PEDIDO,DT_MOVIMENTO as DATA_SOLIC_ORDEM_VENDA,'' as NUMERO_FATURA,DT_MOVIMENTO as DATA_HORA_FATURA,'' as CNPJ_EMISSOR,'' as NU_SERIE,'' as NU_VOUCHER,1 as ID_BOLSO,'{dt_exec}' as dt_execucao,dt from bll10_df_final where SALDOATUAL > 0".format(num = id_recarga_saldo_inicial, dt_exec = str(dt_execucao)))
	bll10_gravar_2 = sqlContext.sql("select SALDO_FRANQUIA_CTRL as SALDO_RM_REC,MSISDN,SALDO_FRANQUIA_CTRL,DT_BOLSO_FRANQUIA_CTRL as DT_SOLICITACAO_RECARGA,'' as NSU_REQUISICAO,'{num}' as ID_RECARGA,DT_BOLSO_FRANQUIA_CTRL as DT_EFETIVACAO_RECARGA,'' as PRODUTO_RECARGA,SALDO_FRANQUIA_CTRL as SALDO_CLIENTE, '' as TIPO_RECARGA,'' as CD_PONTO_VENDA,'' as COD_DISTRIBUIDOR_SAP,'' as ORGANIZACAO_VENDA,'' as SETOR_ATIVIDADE,'' as CANAL_DISTRIBUICAO,'' as NUMERO_PEDIDO,DT_BOLSO_FRANQUIA_CTRL as DATA_SOLIC_ORDEM_VENDA,'' as NUMERO_FATURA,DT_BOLSO_FRANQUIA_CTRL as DATA_HORA_FATURA,'' as CNPJ_EMISSOR,'' as NU_SERIE,'' as NU_VOUCHER,{bolso} as ID_BOLSO,'{dt_exec}' as dt_execucao,dt from bll10_df_final where SALDO_FRANQUIA_CTRL > 0".format(num = id_recarga_saldo_inicial,dt_exec = str(dt_execucao),bolso = cod_bolso_franquia))
	bll10_gravar = bll10_gravar_1.unionAll(bll10_gravar_2)
elif "1" in id_bolso:
	bll10_gravar = sqlContext.sql("select SALDOATUAL as SALDO_RM_REC,MSISDN,SALDOATUAL as VALOR_FACE,DT_MOVIMENTO as DT_SOLICITACAO_RECARGA,'' as NSU_REQUISICAO,'{num}' as ID_RECARGA,DT_MOVIMENTO as DT_EFETIVACAO_RECARGA,'' as PRODUTO_RECARGA,SALDOATUAL as SALDO_CLIENTE, '' as TIPO_RECARGA,'' as CD_PONTO_VENDA,'' as COD_DISTRIBUIDOR_SAP,'' as ORGANIZACAO_VENDA,'' as SETOR_ATIVIDADE,'' as CANAL_DISTRIBUICAO,'' as NUMERO_PEDIDO,DT_MOVIMENTO as DATA_SOLIC_ORDEM_VENDA,'' as NUMERO_FATURA,DT_MOVIMENTO as DATA_HORA_FATURA,'' as CNPJ_EMISSOR,'' as NU_SERIE,'' as NU_VOUCHER,1 as ID_BOLSO,'{dt_exec}' as dt_execucao,dt from bll10_df_final".format(num = id_recarga_saldo_inicial, dt_exec = str(dt_execucao)))
elif "2" in id_bolso:
	bll10_gravar = sqlContext.sql("select SALDO_FRANQUIA_CTRL as SALDO_RM_REC,MSISDN,SALDO_FRANQUIA_CTRL,DT_BOLSO_FRANQUIA_CTRL as DT_SOLICITACAO_RECARGA,'' as NSU_REQUISICAO,'{num}' as ID_RECARGA,DT_BOLSO_FRANQUIA_CTRL as DT_EFETIVACAO_RECARGA,'' as PRODUTO_RECARGA,SALDO_FRANQUIA_CTRL as SALDO_CLIENTE, '' as TIPO_RECARGA,'' as CD_PONTO_VENDA,'' as COD_DISTRIBUIDOR_SAP,'' as ORGANIZACAO_VENDA,'' as SETOR_ATIVIDADE,'' as CANAL_DISTRIBUICAO,'' as NUMERO_PEDIDO,DT_BOLSO_FRANQUIA_CTRL as DATA_SOLIC_ORDEM_VENDA,'' as NUMERO_FATURA,DT_BOLSO_FRANQUIA_CTRL as DATA_HORA_FATURA,'' as CNPJ_EMISSOR,'' as NU_SERIE,'' as NU_VOUCHER,{bolso} as ID_BOLSO,'{dt_exec}' as dt_execucao,dt from bll10_df_final".format(num = id_recarga_saldo_inicial,dt_exec = str(dt_execucao),bolso = cod_bolso_franquia))
else:
	raise ValueError('Bolso informado invalido: '+id_bolso)

#bll10_gravar = sqlContext.sql("select if({bolso} = 1,SALDOATUAL,SALDO_FRANQUIA_CTRL) as SALDO_RM_REC,MSISDN,if({bolso} = 1,SALDOATUAL,SALDO_FRANQUIA_CTRL) as VALOR_FACE,if({bolso} = 1,DT_MOVIMENTO,DT_BOLSO_FRANQUIA_CTRL) as DT_SOLICITACAO_RECARGA,'' as NSU_REQUISICAO,'999999' as ID_RECARGA,if({bolso} = 1,DT_MOVIMENTO,DT_BOLSO_FRANQUIA_CTRL) as DT_EFETIVACAO_RECARGA,'' as PRODUTO_RECARGA,if({bolso} = 1,SALDOATUAL,SALDO_FRANQUIA_CTRL) as SALDO_CLIENTE, '' as TIPO_RECARGA,'' as CD_PONTO_VENDA,'' as COD_DISTRIBUIDOR_SAP,'' as ORGANIZACAO_VENDA,'' as SETOR_ATIVIDADE,'' as CANAL_DISTRIBUICAO,'' as NUMERO_PEDIDO,if({bolso} = 1,DT_MOVIMENTO,DT_BOLSO_FRANQUIA_CTRL) as DATA_SOLIC_ORDEM_VENDA,'' as NUMERO_FATURA,if({bolso} = 1,DT_MOVIMENTO,DT_BOLSO_FRANQUIA_CTRL) as DATA_HORA_FATURA,'' as CNPJ_EMISSOR,'' as NU_SERIE,'' as NU_VOUCHER,{bolso} as ID_BOLSO,'{dt_exec}' as dt_execucao,dt from bll10_df_final".format(dt_exec = str(dt_execucao),bolso = id_bolso))

bll10_gravar.write.mode("append").partitionBy("dt").saveAsTable(db_sva+".tb_aux_recarga", format="parquet")

bll10_gravar.registerTempTable("bll10_gravar")

##Os dados resultantes sao armazenados na tabela final. Testar overwrite

#bll10_df_final.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_SALDO_CLIENTE", format="parquet")
sqlContext.sql("create table {db_stg}.TMP_TB_SALDO_CLIENTE like {db_prepago}.TB_SALDO_CLIENTE".format(db_stg = db_stg,db_prepago = db_prepago))

sqlContext.sql("insert into table "+db_stg+".TMP_TB_SALDO_CLIENTE partition (dt) select * from bll10_df_final")

sqlContext.sql("drop table "+db_prepago+".TB_SALDO_CLIENTE");

sqlContext.sql("alter table {db_stg}.TMP_TB_SALDO_CLIENTE rename to {db_prepago}.TB_SALDO_CLIENTE".format(db_stg = db_stg,db_prepago = db_prepago))

# Calcula a soma de todas as recargas inseridas

valor_total = str(sqlContext.sql("select sum(SALDO_RM_REC) from bll10_gravar").collect()[0][0])

# Insere NFV saldo inicial p/ correlacao no relatorio

nfv_rec = sqlContext.sql("select '{data}' as data_evento, 'recarga' as tipo_registro, \
{num} as numero_nf, {num} as num_ordem_venda, '{data}' as data_hora_nf, '1' as id_item_nf, {valor} as valor_item_nf,\
{valor} as valor_nf, {valor} as valor_material, {num} as cod_distribuidor_sap, {num} as organizacao_venda, \
{num} as setor_atividade, {num} as canal_distribuicao, '' as UF, '' as tipo_ordem, '' as material, '' as desc_material, \
'' as serie, '' as subserie, '' as cfop, '' as num_pedido, 'VAL' as status, false as in_evt_sistemico_nfv, \
{num} as id_recarga, {num} as nsu_requisicao, '' as msisdn, 0 as valor_face, '{data}' as dt_solicitacao_recarga, '{data}' as \
dt_efetivacao_recarga, 0 as valor_debitado, 0 as valor_saldo, 'ELT' as tipo_recarga, 1 as id_bolso, '{dt_exec}' as dt_execucao,\
substr('{data}',1,10) as dt".format(num = num_nfv_saldo_inicial,data = str(valida_data(data_producao)), valor = valor_total,dt_exec = dt_execucao))

nfv_rec.write.mode("append").partitionBy("dt").saveAsTable(db_sva+".tb_nfv_recarga_cv", format="parquet")


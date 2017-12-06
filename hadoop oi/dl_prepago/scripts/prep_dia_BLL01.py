###   Nome do programa: prep_dia_BLL01
###
###   O seguinte programa le os arquivos da BLL01, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc.addPyFile(conf_path+"/conf_prep_dia_BLL01.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")


from conf_prep_dia_BLL01 import *

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

conf.setAppName("prep dia BLL01") 

dir_arquivo = dir_processamento+arquivo

bll01 = sc.textFile(dir_arquivo)

n_regs = bll01.count()

if n_regs == 0:
    mensagem = "Arquivo vazio"
    grava_log("WARN",cod_programa + cod_erro["FORMATO_ARQUIVO_INVALIDO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    exit(0)

##Define o nome e tipo dos campos da tabela final 

schemaString = "MSISDN;msisdn_b;dt_inicio;dt_fim;duracao;cd_plano_preco;tp_tarifacao;vl_chamada;tp_periodo;tp_dia_semana;cd_zona_origem;ds_destino;ds_roaming;cd_opld;tp_chamada;vl_saldo;area_tarifacao;cd_categoria;ds_acobrar;dt_msc;cd_concat;duracao_real;c_opld_b;operadora;msisdn_a;calling_cat;no_bolso;aos;cd_tipo_produto;ddd_cell_id;cell_id;DT_EXTRACAO;dt_execucao;dt"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[2].dataType = TimestampType()
fields[7].dataType = DecimalType(10,2)
fields[15].dataType = DecimalType(10,2)
#fields[29].dataType = TimestampType()

schema = StructType(fields)

##Mapeia os campos lidos para o formato da tabela e chama as funcoes de validacao
def bll01_map(x):
	try:
		return (x[0], x[1], valida_data(x[2]), x[3], x[4], x[5], x[6], valida_valor_divide(x[7]), x[8], x[9], x[10], x[11], x[12], x[13], x[14], valida_valor_divide(x[15]), x[16], x[17], x[18], x[19], x[20], x[21], x[22], x[23], x[24], x[25], x[26], x[27], x[28], x[29],x[30], x[31], dt_execucao, str(valida_data(x[2]))[:10])
	except IndexError:
		linha = ""
		for campo in x:
			linha += campo+";"
		return [linha] #precisa ser array por conta da funcao str_rej

bll01_temp = bll01.map(lambda k: k.split(";")).map(bll01_map).cache()

bll01_rej_estrutura = bll01_temp.filter(lambda x: len(x) <= 1)

registra_erros(bll01_rej_estrutura," ","ESTRUTURA_INVALIDA")

bll01_ace = bll01_temp.filter(lambda x : len(x) > 1)
	
bll01_rej_msisdn = bll01_ace.filter(lambda x : x[0] == "")

registra_erros(bll01_rej_msisdn,"MSISDN","MSISDN_INVALIDO")

bll01_ace = bll01_ace.filter(lambda x : x[0] != "")	

bll01_rej_msisdn_b = bll01_ace.filter(lambda x : x[1] == "")

registra_erros(bll01_rej_msisdn_b,"msisdn_b","MSISDN_B_INVALIDO")

bll01_ace = bll01_ace.filter(lambda x : x[1] != "")	

bll01_rej_dt_inicio = bll01_ace.filter(lambda x: type(x[2]) is unicode)

registra_erros(bll01_rej_dt_inicio,"dt_inicio","DT_INICIO_INVALIDA") 

bll01_ace = bll01_ace.filter(lambda x : type(x[2]) is not unicode)

bll01_rej_tempo_guarda = bll01_ace.filter(lambda x: x[2] < valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

registra_erros(bll01_rej_tempo_guarda,"dt_inicio","REGISTRO_FORA_TEMPO_GUARDA")

bll01_ace = bll01_ace.filter(lambda x : x[2] >= valida_data(data_processa) - timedelta(days=int(tempo_guarda)))

bll01_rej_antigos = bll01_ace.filter(lambda x: x[2] < valida_data(data_producao))

registra_erros(bll01_rej_antigos,"dt_inicio","REGISTRO_ANTIGO")

bll01_ace = bll01_ace.filter(lambda x : x[2] >= valida_data(data_producao))

bll01_rej_tp_tarifacao = bll01_ace.filter(lambda x : x[6] == "")

registra_erros(bll01_rej_tp_tarifacao,"tp_tarifacao","TP_TARIFACAO_INVALIDO")

bll01_ace = bll01_ace.filter(lambda x : x[6] != "")	

bll01_rej_vl_chamada = bll01_ace.filter(lambda x: type(x[7]) is unicode)

registra_erros(bll01_rej_vl_chamada,"vl_chamada","VL_CHAMADA_INVALIDO")

bll01_ace = bll01_ace.filter(lambda x : type(x[7]) is not unicode)

bll01_rej_vl_saldo = bll01_ace.filter(lambda x : type(x[15]) is unicode)

registra_erros(bll01_rej_vl_saldo,"vl_saldo","VL_SALDO_INVALIDO")

bll01_ace = bll01_ace.filter(lambda x : type(x[15]) is not unicode)

bll01_rej_no_bolso = bll01_ace.filter(lambda x : x[26] == "")

registra_erros(bll01_rej_no_bolso,"no_bolso","NO_BOLSO_INVALIDO")

bll01_ace = bll01_ace.filter(lambda x : x[26] != "")	

bll01_df = sqlContext.createDataFrame(bll01_ace, schema).cache()

##Agrupa registros por msisdn, msisdn_b,dt_inicio e vl_chamada para verificar duplicidade

windowSpec = Window.partitionBy(bll01_df['MSISDN'], bll01_df['msisdn_b'], bll01_df['dt_inicio'], bll01_df['vl_chamada'], bll01_df['no_bolso'])

bll01_window = bll01_df.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

bll01_dup = bll01_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

bll01_df_uniq = bll01_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

#min_dt = bll01_df_uniq.select("dt").rdd.min()[0]
#pega lista de particoes que aparecem no arquivo p/filtrar tabela
bll01_df_uniq.registerTempTable("bll01_df_uniq")

dt_list = sqlContext.sql("select distinct dt from bll01_df_uniq").collect()

dt_str = "("

for dt in dt_list:
	dt_str += "'{dt}',".format(dt = dt[0])

dt_str = dt_str[:-1] + ")"

bll01_old = sqlContext.sql("select MSISDN, msisdn_b, dt_inicio, vl_chamada, no_bolso, tp_chamada as testeJoin from "+db_prepago+".TB_VOZ where dt in "+dt_str)

bll01_join = bll01_df_uniq.join(bll01_old, ["MSISDN","msisdn_b", "dt_inicio", "vl_chamada", "no_bolso"], "leftouter")

bll01_dup2 = bll01_join.filter("testejoin is not null")

bll01_df_final = bll01_join.filter("testeJoin is null").select([name for name in schemaString.split(";")])

bll01_duplicados = bll01_dup.unionAll(bll01_dup2.select([name for name in schemaString.split(";")]))

n_dup = bll01_duplicados.count()

if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(bll01_duplicados, dir_rejeitado + arquivo +"."+ dt_execucao_formatada +"."+ cod_programa + cod_erro["REGISTRO_DUPLICADO"])

bll01_df_final.write.mode("append").partitionBy("dt").saveAsTable(db_prepago+".TB_VOZ", format="parquet")

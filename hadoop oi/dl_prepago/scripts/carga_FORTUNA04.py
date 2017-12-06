###   Nome do programa: prep_dia_FORTUNA04
###
###   O seguinte programa le os arquivos da interface FORTUNA04, de um dia especifico, realiza validacoes dos campos e grava os registros na tabela final
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

sc.addPyFile(conf_path+"/conf_carga_FORTUNA04.py")
sc.addPyFile(conf_path+"/conf_geral.py")
sc.addPyFile(conf_path+"/valida_lib.py")

from conf_carga_FORTUNA04 import *

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

conf.setAppName("Valida FORTUNA04") 

dir_arquivo = dir_processamento+arquivo

fortuna04 = sc.textFile(dir_arquivo)

##Define a estrutura e dos tipos dos campos da tabela final 

schemaString = "DH_EXTRACAO;CD_PRODUTO_RECARGA;NO_PRODUTO;DT_CRIACAO_PRODUTO;DH_ATUALIZACAO;DT_INICIO_VIGENCIA;DT_FIM_VIGENCIA;NU_VALIDADE;NU_VALOR_FACE;DS_TIPO_RECARGA;IN_MULTIPLO_USO;CD_DIREITO;NU_VALOR_DIREITO;TIPO_USO;VALOR_FACE_FINAL; PERCENTUAL;VALOR_CONTABIL; CATEG_CONTABIL; COD_ID_DEBITO;DT_EXECUCAO"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(';')]

fields[0].dataType = TimestampType()
fields[3].dataType = TimestampType()
fields[4].dataType = TimestampType()
fields[5].dataType = TimestampType()
fields[6].dataType = TimestampType()
#fields[14].dataType = TimestampType()

schema = StructType(fields)

def for_map(x):
        try:
                return (valida_data_nula(x[0]), x[1], x[2], valida_ano_mes_dia(x[3]), valida_ano_mes_dia(x[4]), valida_ano_mes_dia(x[5]), valida_data_max(x[6]), x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18], dt_execucao)
        except IndexError:
                linha = ""
                for campo in x:
                        linha += campo+";"
                return [linha] #precisa ser array por conta da funcao str_rej

##Mapeia os campos para o formato da tabela e inicia as validacoes

fortuna04_temp = fortuna04.map(lambda k: k.split(";")).map(for_map).cache()

fortuna04_rej_estrutura = fortuna04_temp.filter(lambda x: len(x) <= 1)

registra_erros(fortuna04_rej_estrutura," ","ESTRUTURA_INVALIDA")

fortuna04_ace = fortuna04_temp.filter(lambda x: len(x) > 1)

fortuna04_rej_dh_extracao = fortuna04_ace.filter(lambda x: type(x[0]) is unicode)

registra_erros(fortuna04_rej_dh_extracao,"DH_EXTRACAO","DH_EXTRACAO_INVALIDA")

fortuna04_ace = fortuna04_ace.filter(lambda x : type(x[0]) is not unicode)

fortuna04_rej_cod_produto_recarga = fortuna04_ace.filter(lambda x : x[1] == "")

registra_erros(fortuna04_rej_cod_produto_recarga,"CD_PRODUTO_RECARGA","CD_PRODUTO_RECARGA_INVALIDO")

fortuna04_ace = fortuna04_ace.filter(lambda x : x[1] != "")

fortuna04_rej_no_produto = fortuna04_ace.filter(lambda x : x[2] == "")

registra_erros(fortuna04_rej_no_produto,"NO_PRODUTO","NO_PRODUTO_INVALIDO")

fortuna04_ace = fortuna04_ace.filter(lambda x : x[2] != "")

fortuna04_rej_dt_criacao_produto = fortuna04_ace.filter(lambda x: type(x[3]) is unicode)

registra_erros(fortuna04_rej_dt_criacao_produto,"DT_CRIACAO_PRODUTO","DT_CRIACAO_PRODUTO_INVALIDA")

fortuna04_ace = fortuna04_ace.filter(lambda x : type(x[3]) is not unicode)

fortuna04_rej_dh_atualizacao = fortuna04_ace.filter(lambda x: type(x[4]) is unicode)

registra_erros(fortuna04_rej_dh_atualizacao,"DH_ATUALIZACAO","DH_ATUALIZACAO_INVALIDA")

fortuna04_ace = fortuna04_ace.filter(lambda x : type(x[4]) is not unicode)

fortuna04_rej_dt_inicio_vigencia = fortuna04_ace.filter(lambda x: type(x[5]) is unicode)

registra_erros(fortuna04_rej_dt_inicio_vigencia,"DT_INICIO_VIGENCIA","DT_INICIO_VIGENCIA_INVALIDA")

fortuna04_ace = fortuna04_ace.filter(lambda x : type(x[5]) is not unicode)

fortuna04_rej_dt_fim_vigencia = fortuna04_ace.filter(lambda x: type(x[6]) is unicode)

registra_erros(fortuna04_rej_dt_fim_vigencia,"DT_FIM_VIGENCIA","DT_FIM_VIGENCIA_INVALIDA")

fortuna04_ace = fortuna04_ace.filter(lambda x : type(x[6]) is not unicode)

fortuna04_df = sqlContext.createDataFrame(fortuna04_ace, schema) 

fortuna04_old = sqlContext.sql("select DH_EXTRACAO as DH_EXTRACAO02,CD_PRODUTO_RECARGA as CD_PRODUTO_RECARGA02,NO_PRODUTO as NO_PRODUTO02,DT_CRIACAO_PRODUTO as DT_CRIACAO_PRODUTO02,DH_ATUALIZACAO as DH_ATUALIZACAO02,DT_INICIO_VIGENCIA as DT_INICIO_VIGENCIA02,DT_FIM_VIGENCIA as DT_FIM_VIGENCIA02,NU_VALIDADE as NU_VALIDADE02,NU_VALOR_FACE as NU_VALOR_FACE02,DS_TIPO_RECARGA as DS_TIPO_RECARGA02,IN_MULTIPLO_USO as IN_MULTIPLO_USO02,CD_DIREITO as CD_DIREITO02,NU_VALOR_DIREITO as NU_VALOR_DIREITO02,TIPO_USO as TIPO_USO02,VALOR_FACE_FINAL,PERCENTUAL,VALOR_CONTABIL,CATEG_CONTABIL,COD_ID_DEBITO, '{dt_exec}' as dt_execucao from "+db_prepago+".tb_produto_recarga".format(dt_exec = str(dt_execucao)))

fortuna04_df_union = fortuna04_df.unionAll(fortuna04_old).cache()

##Agrupa registros por cd_produto_recarga e dh_atualizacao para verificar duplicidade

windowSpec = Window.partitionBy(fortuna04_df_union['CD_PRODUTO_RECARGA'], fortuna04_df_union['CD_DIREITO']).orderBy(fortuna04_df_union['DH_ATUALIZACAO'].desc())

fortuna04_window = fortuna04_df_union.select([func.rowNumber().over(windowSpec).alias("row_number")]+[name for name in schemaString.split(";")])

fortuna04_duplicados = fortuna04_window.filter("row_number <> 1").select([name for name in schemaString.split(";")])

fortuna04_df_final = fortuna04_window.filter("row_number = 1").select([name for name in schemaString.split(";")])

n_dup = fortuna04_duplicados.count()
if n_dup > 0:
    mensagem = str(n_dup) + " registros duplicados rejeitados"
    grava_log("ERRO",cod_programa + cod_erro["REGISTRO_DUPLICADO"],mensagem) #tp_ocorrencia, cod_ocorrencia, mensagem
    grava_rej(fortuna04_duplicados, dir_rejeitado+arquivo+"."+dt_execucao_formatada+"."+cod_programa + cod_erro["REGISTRO_DUPLICADO"])

##Define os campos e os tipos para o formato da tabela final 

sqlContext.sql(create_tmp_tb_produto_recarga)

fortuna04_df_final.write.mode("append").saveAsTable(db_stg+".tmp_tb_produto_recarga", format="parquet")

sqlContext.sql("drop table "+db_prepago+".tb_produto_recarga")

##Os dados resultantes sao armazenados na tabela final

sqlContext.sql("ALTER TABLE "+db_stg+".tmp_tb_produto_recarga RENAME TO "+db_prepago+".tb_produto_recarga")

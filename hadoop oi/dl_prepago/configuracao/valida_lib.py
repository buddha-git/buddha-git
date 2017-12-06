from decimal import *
from datetime import *
import sys
import pyspark.sql.functions as func
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
import ConfigParser

schemaLog = "DH_OCORRENCIA;TIPO_OCORRENCIA;COD_OCORRENCIA;MODULO;MENSAGEM;NOME_ARQUIVO;dt"

def remove_zeros_serie(serie):
	if serie == "":
		return ""
	try:
		return str(long(serie))
	except ValueError:
		return "erro"+serie

def str_rej(linha):
	gravar = ""
	
	for item in linha:
		if type(item) is datetime:
			item = item.strftime("%Y%m%d%H%M%S")
		elif type(item) is not unicode:
			item = str(item)
		gravar += item + ";"
	
	#gravar += "\n"
	return gravar

def valida_data_barra(datetime_string): 
	try:
		return datetime.strptime(datetime_string,"%d/%m/%Y %H:%M:%S")
	except ValueError:
		return datetime_string

def valida_data_traco(datetime_string): 
	try:
		return datetime.strptime(datetime_string,"%d-%m-%Y %H:%M:%S")
	except ValueError:
		return datetime_string

def valida_data_traco_min(datetime_string): 
	try:
		return datetime.strptime(datetime_string,"%d-%m-%Y %H:%M:%S")
	except ValueError:
		return datetime.strptime("19600101000000","%Y%m%d%H%M%S")

def valida_data_traco_nula(datetime_string): 
	if datetime_string == "" :
		return datetime.strptime("19700101000000","%Y%m%d%H%M%S")
	try:
		return datetime.strptime(datetime_string,"%d-%m-%Y %H:%M:%S")
	except ValueError:
		return datetime_string

def valida_so_data(datetime_string): 
	try:
		return datetime.strptime(datetime_string,"%Y%m%d")
	except ValueError:
		return datetime_string


def valida_data(datetime_string): 
    try:
        return datetime.strptime(datetime_string,"%Y%m%d%H%M%S")
    except ValueError:
         return datetime_string

def valida_data_nula(datetime_string):
	if datetime_string == "" :
		return datetime.strptime("19600101000000","%Y%m%d%H%M%S")
	try:
		return datetime.strptime(datetime_string,"%Y%m%d%H%M%S")
	except ValueError:
		return datetime_string

def valida_ano_mes_dia(datetime_string):
	try:
		return datetime.strptime(datetime_string+"000000","%Y%m%d%H%M%S")
	except ValueError:
		return datetime.strptime("19600101000000","%Y%m%d%H%M%S")

def valida_data_max(datetime_string):
	try:
		return datetime.strptime(datetime_string+"000000","%Y%m%d%H%M%S")
	except ValueError:
		return datetime.strptime("20990101000000","%Y%m%d%H%M%S")

def valida_dt_fatura(datetime_string):
	if len(datetime_string) == 6 :
		return datetime.strptime(datetime_string+"01000000","%Y%m%d%H%M%S")
	try:
		return datetime.strptime(datetime_string,"%Y%m%d%H%M%S")
	except ValueError:
		return datetime.strptime("19600101000000","%Y%m%d%H%M%S")

for param in sys.argv:
	conf_path = param

#conf_path = "/home/SVA/scripts/conf_FORTUNA01.cfg"

#config = ConfigParser.ConfigParser()
#config.read(conf_path)
#arquivo = config.get("Section10","dir_entrada")+config.get("Section10","arquivo")

#getcontext().prec = 2

def valida_valor(valor_string):
	try:
		return Decimal(valor_string)
	except InvalidOperation:
		return valor_string

def valida_valor_nulo(valor_string):
	if valor_string == "" :
		return Decimal("0")
	try:
		return Decimal(valor_string)
	except InvalidOperation:
		return valor_string


def valida_valor_divide(valor_string):
	try:
		return Decimal(valor_string)/100
	except InvalidOperation:
		return valor_string

def valida_valor_nulo_ou_divide(valor_string):
	if valor_string == "" :
		return Decimal("0")
	try:
		return Decimal(valor_string)/100
	except InvalidOperation:
		return valor_string

def valida_bolso( bolso_string ):
	try:
		return int(bolso_string)
	except ValueError:
		return bolso_string

def grava_rej(rdd,path):
	gravar2 = rdd.map(str_rej)
	gravar2.saveAsTextFile(path)

"""def grava_rej(rdd,path):
	arq = open(path, 'a')
	
	gravar2 = rdd.map(str_rej).collect()
	
	for item in gravar2:
		arq.write(item.encode('utf-8'))
	
	arq.close()"""

def invalida_data(data_timestamp): #converte timestamp para o formato YYYYMMDDHHMMSS
	return data_timestamp.strftime('%Y%m%d%H%M%S')

def valida_telecom (ind_telecom):
	if ind_telecom == "1":
		return "Telecom" 
	else:
		return "Nao Telecom"

def get_indice_campo(nome_campo,layout):
	campos = layout.split(";")
	for i in range (0,len(campos)):
		if campos[i].lower() == nome_campo.lower():
			return i
	raise ValueError('Campo '+nome_campo+' nao encontrado')
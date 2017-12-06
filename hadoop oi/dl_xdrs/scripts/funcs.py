import hashlib
from datetime import *

sep = "^"

@outputSchema("word:chararray")
def criaByteArray(s):
	if s is None: return ''
	arr = s.split("|")
	for i in range(0,len(arr)):
		arr[i] = int(arr[i])
 	return ''.join(chr(x) for x in arr)

@outputSchema("word:chararray")
def getMd5(*arg):
	text = ""
	for item in arg:
		if item is None:
			item = ""
		text += item
	m = hashlib.md5()
	m.update(text)
	return m.hexdigest()

@outputSchema("word:chararray")
def getChaveVozMovel(data,num,perna,g,tipo,cr,numRegions):
	if data is None: data = ''
	if num is None: num = ''
	if perna is None: perna = ''
	if g is None: g = ''
	if tipo is None: tipo = ''
	if cr is None: cr = ''
	chave = data[:8] + sep + num + sep + perna + sep  + g + sep  + data[-6:] + sep  + tipo + sep  + cr
	soma = 0
	for c in chave:
		soma += ord(c)
	prefixo = str(soma%numRegions)
	if len(prefixo) == 1:
		prefixo = "0" + prefixo
	return prefixo + sep + chave

@outputSchema("word:chararray")
def getChaveSms(data,a,b,numRegions):
	if data is None: data = ''
	if a is None: a = ''
	if b is None: b = ''
	chave = data[:8]  + sep + a  + sep + b  + sep + data[-6:]
	soma = 0
	for c in chave:
		soma += ord(c)
	prefixo = str(soma%numRegions)
	if len(prefixo) == 1:
		prefixo = "0" + prefixo
	return prefixo + sep + chave

@outputSchema("word:chararray")
def getChaveMms(data,a,b,numRegions):
	if data is None: data = ''
	if a is None: a = ''
	if b is None: b = ''
	chave = data[:8]  + sep + a  + sep + b  + sep + data[-6:]
	soma = 0
	for c in chave:
		soma += ord(c)
	prefixo = str(soma%numRegions)
	if len(prefixo) == 1:
		prefixo = "0" + prefixo
	return prefixo + sep + chave

@outputSchema("word:chararray")
def getChaveOcs(data,a,b,numRegions):
	if data is None: data = ''
	if a is None: a = ''
	if b is None: b = ''
	chave = data[:8]  + sep + a  + sep + b  + sep + data[-6:]
	soma = 0
	for c in chave:
		soma += ord(c)
	prefixo = str(soma%numRegions)
	if len(prefixo) == 1:
		prefixo = "0" + prefixo
	return prefixo + sep + chave

@outputSchema("word:chararray")
def getChaveVozFixa(data,num,perna,bilhetador,num_outra_perna,numRegions):
	if data is None: data = ''
	if num is None: num = ''
	if perna is None: perna = ''
	if bilhetador is None: bilhetador = ''
	if num_outra_perna is None: num_outra_perna = ''
	chave = data[:8]  + sep + num  + sep + perna  + sep + bilhetador  + sep + data[-6:]  + sep + num_outra_perna
	soma = 0
	for c in chave:
		soma += ord(c)
	prefixo = str(soma%numRegions)
	if len(prefixo) == 1:
		prefixo = "0" + prefixo
	return prefixo + sep + chave

@outputSchema("word:chararray")
def getChaveGprs(tipo,num1,data,num2,numRegions): # numX pode ser ip ou telefone
	if data is None: data = ''
	if num1 is None: num1 = ''
	if num2 is None: num2 = ''
	chave = str(tipo)  + sep + num1  + sep + data[:8]  + sep + num2  + sep + data[-6:]
	soma = 0
	for c in chave:
		soma += ord(c)
	prefixo = str(soma%numRegions)
	if len(prefixo) == 1:
		prefixo = "0" + prefixo
	return prefixo + sep + chave

@outputSchema("word:chararray")
def getChaveSyslog(ie,ippu,ippr,data,porta,numRegions):
	if ie is None: ie = ''
	if data is None: data = ''
	if ippu is None: ippu = ''
	if ippr is None: ippr = ''
	if porta is None: porta = ''
	if ie == "2012":
		chave = ie + sep + ippu + sep + data[:8] + sep + data[-6:] + sep + porta
	else: # ie == 2021
		chave = ie  + sep + ippr + sep + data[:8] + sep + data[-6:]
	soma = 0
	for c in chave:
		soma += ord(c)
	prefixo = str(soma%numRegions)
	if len(prefixo) == 1:
		prefixo = "0" + prefixo
	return prefixo + sep + chave

@outputSchema("word:chararray")
def getChaveDiretaAdsl(id_sessao_md5,data,tipo_log,numRegions):
	if data is None: data = ''
	if id_sessao_md5 is None: id_sessao_md5 = ''
	if tipo_log is None: tipo_log = ''
	chave = id_sessao_md5  + sep + data[:8] + sep + data[-6:]  + sep + tipo_log
	soma = 0
	for c in chave:
		soma += ord(c)
	prefixo = str(soma%numRegions)
	if len(prefixo) == 1:
		prefixo = "0" + prefixo
	return prefixo + sep + chave

@outputSchema("word:chararray")
def getChaveIndiretaAdsl(tipo,num,data,id_sessao_md5,tl,numRegions): #num pode ser ip ou telefone
	if data is None: data = ''
	if num is None: num = ''
	if id_sessao_md5 is None: id_sessao_md5 = ''
	if tl is None: tl = ''
	chave = str(tipo)  + sep + num  + sep + data[:8]  + sep + id_sessao_md5  + sep + data[-6:]  + sep + tl
	soma = 0
	for c in chave:
		soma += ord(c)
	prefixo = str(soma%numRegions)
	if len(prefixo) == 1:
		prefixo = "0" + prefixo
	return prefixo + sep + chave

@outputSchema("word:chararray")
def getChaveDiretaDial(sessao_md5,data,tipo_log,numRegions):
	if data is None: data = ''
	if sessao_md5 is None: sessao_md5 = ''
	if tipo_log is None: tipo_log = ''
	chave = sessao_md5 + sep + data[:8] + sep + data[-6:] + sep + tipo_log
	soma = 0
	for c in chave:
		soma += ord(c)
	prefixo = str(soma%numRegions)
	if len(prefixo) == 1:
		prefixo = "0" + prefixo
	return prefixo + sep + chave

@outputSchema("word:chararray")
def getChaveIndiretaDial(tipo,num,data,id_sessao_md5,tl,numRegions): #num pode ser ip ou telefone
	if data is None: data = ''
	if num is None: num = ''
	if id_sessao_md5 is None: id_sessao_md5 = ''
	if tl is None: tl = ''
	chave = str(tipo)  + sep + num  + sep + data[:8]  + sep + id_sessao_md5  + sep + data[-6:]  + sep + tl
	soma = 0
	for c in chave:
		soma += ord(c)
	prefixo = str(soma%numRegions)
	if len(prefixo) == 1:
		prefixo = "0" + prefixo
	return prefixo + sep + chave

@outputSchema("word:chararray")
def getChave(numRegions,*arg):
	chave = ""
	for item in arg:
		if item is None: 
			item = ''
		chave += str(item) + sep
	chave = chave[:-1] # remove ultimo character da chave (separador)
	soma = 0
	for c in chave:
		soma += ord(c)
	prefixo = str(soma%numRegions)
	if len(prefixo) == 1:
		prefixo = "0" + prefixo
	return prefixo + sep + chave

@outputSchema("word:chararray")
def adicionaZeros(string,tamanho):
	if string is None:
		string = ""
	while len(string) < tamanho:
		string = "0" + string
	return string[:tamanho]

@outputSchema('m:map[]')
def aplicaSchema(vetorAtts, *atts):
	keyArray = vetorAtts.split(",")
	fields = {}
	for x in range (0,len(keyArray)): 
		if (atts[x] != None):
			fields[keyArray[x]] = atts[x]
	return fields

@outputSchema("word:chararray")
def retorna_data_corte_hbase(dias):
	dt_corte_str = str(datetime.now() - timedelta(days=dias))
	dt_corte_formatada = dt_corte_str[0:10].replace("-","")
	return dt_corte_formatada

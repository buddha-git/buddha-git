#!/bin/bash

source /data1/etl_hdp/dl_prepago/scripts/sh/env_SVA.sh

dir_processamento_sap02=$dir_processamento/sap02
dir_processado_sap02=$dir_processado/sap02
dir_rejeitado_sap02=$dir_rejeitado/sap02

# Funcao para listar os arquivos de um diretorio HDFS
lista_arq(){

  local p=0
  local i

  # Lista os arqquivos 
  for i in `hadoop fs -ls $1`
  do
    # Pega o nome do arquivo
    if [ $p == "7" ]; then
      echo $i
      p=0
    else
      ((p=$p+1))
    fi
  done
}

data_start=`date +%Y%m%d_%H%M%S`
nome_modulo="separa_SAP02"
data_ref=$1

arq_log=$dir_log/${nome_modulo}_${data_ref}_${data_start}.log

touch ${arq_log}
if [ ${?} -ne 0 ]
then
	echo "Erro na criacao do arquivo de Log [${arq_log}]"
	exit ${ars_ret_code}
fi

# Verifica numero de parametros

if [ $# -lt 1 ]
then
        echo "usage: DATA_PROCESSA" >> $arq_log
        exit ${ars_ret_code}
fi

# Renova ticket Kerberos
if [ ! -s ${keytab_path} ] ; then
   echo "Arquivo keytab $keytab_path nao encontrado" >> $arq_log
   exit ${ars_ret_code}
else
   echo "Arquivo keytab $keytab_path encontrado"  >> $arq_log
   echo "Executando kinit para $keytab_path $keytab_user/$(hostname)" >> $arq_log
   kinit -kt $keytab_path $keytab_user/$(hostname)
   ParamRetCode=$?
   if [ $ParamRetCode -ne 0 ] ; then
      echo "Erro $ParamRetCode ao executar kinit" >> $arq_log
      exit ${ars_ret_code}
   fi
fi

/data1/etl_hdp/dl_prepago/scripts/sh/verifica_entrada_interface.sh sap02  >> $arq_log 
ParamRetCode=$?
if [ $ParamRetCode -ne 0 ] ; then
   echo "Erro $ParamRetCode ao executar verifica_entrada_interface.sh sap02" >> $arq_log
   exit ${ars_ret_code}
fi

arquivos=$(lista_arq $dir_processamento_sap02/SAP_NF_DEVOLVIDA_${data_ref}*)

for i in $arquivos
do

  echo "Arquivo $i encontrado"

  arquivo="${i##*\/}"

  # Arquivo de saida de cartao fisico
  saida_cf="$dir_processamento_sap02/${arquivo%%.*}_CF.${arquivo##*.}"

  echo "Gerando arquivo $saida_cf" >> $arq_log

  # Cartao fisico
  hadoop fs -cat $i | awk -F";" 'function ltrim(s) { sub(/^[ \t\r\n]+/, "", s); return s } function rtrim(s) { sub(/[ \t\r\n]+$/, "", s); return s } function trim(s)  { return rtrim(ltrim(s)); } BEGIN{} { if (trim($10) != "" && trim($11) != ""){ print $0 } } END{}' | hadoop fs -put - $saida_cf
  
  ParamRetCode=$?
  if [ $ParamRetCode -ne 0 ] ; then
          echo "*** ${nome_modulo} <Erro>: Condicao de erro ${ParamRetCode} na geracao do arquivo ${saida_cf}" >> $arq_log
          exit ${ars_ret_code}
  fi 
  
  # Arquivo de saida sem ser cartao fisico
  saida_ncf="$dir_processamento_sap02/${arquivo%%.*}_NCF.${arquivo##*.}"

  echo "Gerando arquivo $saida_ncf" >> $arq_log

  # Nao e cartao fisico
  hadoop fs -cat $i | awk -F";" 'function ltrim(s) { sub(/^[ \t\r\n]+/, "", s); return s } function rtrim(s) { sub(/[ \t\r\n]+$/, "", s); return s } function trim(s)  { return rtrim(ltrim(s)); } BEGIN{} { if (trim($10) == "" && trim($11) == ""){ print $0 } } END{}' | hadoop fs -put - $saida_ncf

  ParamRetCode=$?
  if [ $ParamRetCode -ne 0 ] ; then
          echo "*** ${nome_modulo} <Erro>: Condicao de erro ${ParamRetCode} na geracao do arquivo ${saida_ncf}" >> $arq_log
          exit ${ars_ret_code}
  fi 
  
  # Arquivo de saida dos rejeitados
  saida_rej="$dir_rejeitado_sap02/${arquivo%%.*}.120001"

  echo "Gerando arquivo $saida_rej" >> $arq_log

  # Rejeicao
  hadoop fs -cat $i | awk -F";" 'function ltrim(s) { sub(/^[ \t\r\n]+/, "", s); return s } function rtrim(s) { sub(/[ \t\r\n]+$/, "", s); return s } function trim(s)  { return rtrim(ltrim(s)); } BEGIN{} { if ((trim($10) != "" && trim($11) == "") || (trim($10) == "" && trim($11) != "")){ print $0 } } END{}' | hadoop fs -put - $saida_rej

  ParamRetCode=$?
  if [ $ParamRetCode -ne 0 ] ; then
          echo "*** ${nome_modulo} <Erro>: Condicao de erro ${ParamRetCode} na geracao do arquivo ${saida_rej}" >> $arq_log
          exit ${ars_ret_code}
  fi 
  # Destino do arquivo original
  echo "Movendo o arquivo original para $dir_processado_sap02" >> $arq_log

  # Move o arquivo original para o destino
  hadoop fs -mv $i $dir_processado_sap02
  
  ParamRetCode=$?
  if [ $ParamRetCode -ne 0 ] ; then
          echo "*** ${nome_modulo} <Erro>: Condicao de erro ${ParamRetCode} na movimentacao do arquivo ${i}" >> $arq_log
          exit ${ars_ret_code}
  fi 

done



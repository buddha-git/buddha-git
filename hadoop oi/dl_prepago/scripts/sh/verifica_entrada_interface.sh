#!/bin/bash
# So use em caso de debug
#set -ex
#################################################################
#                                                               #
# Versao: 1.0	                                                #
# Data: 09/06/2016                                              #
# 			                                        #
# Script para verificar a duplicidade de arquivos no BigData.	  #
#								                                                #
# A seguinte estrutura de diretorio deve ser seguida no HDFS:	  #
#								                                                #
# /dir_raiz							                                        #
# /dir_raiz/dir_entrada/interface1				                      #
# /dir_raiz/dir_entrada/interface2				                      #
# ...								                                            #
# /dir_raiz/dir_rejeitado/interface1				                    #
# /dir_raiz/dir_rejeirado/interface2				                    #
# ...								                                            #
# /dir_raiz/dir_processamento/interface1			                  #
# /dir_raiz/dir_processamento/interface2			                  #
#								                                                #
# Os arquivos que serao entregues no diretorio $dir_entrada.	  #
# O arquivo sera movido para o diretorio $dir_processamento,	  #
# se nao for duplicado, ou sera movido para o diretorio 	      #
# $dir_rejeitado, se for duplicado.				                      #
#								                                                #
#################################################################
# Importa variaveis de ambiente
source /data1/etl_hdp/dl_prepago/scripts/sh/env_SVA.sh

# Data de execucao
dt_execucao=$(date "+%Y-%m-%d %H:%M:%S")
echo $dt_execucao " DT_EXECUCAO"

# Connect String beeline
bl_conn_str="jdbc:hive2://$(hostname):10000/dl_prepago;principal=hive/_HOST@$(hostname)"

# Codigo de erro dos arquivos rejeitados
cod_erro="138001"

# Tabela de controle
tb_ctl="dl_prepago.tb_ctl_arq_proc"

# Funcao para listar os arquivos de um diretorio HDFS
lista_arq(){

  local p=0
  local i

  # Lista de forma recursiva o diretorio
  for i in `hadoop fs -ls -R $1`
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

# Funcao para gerar o MD5 de um arquivo no HDFS
gerar_md5(){

  # Lista nome arquivo e checksum
  echo `hadoop fs -checksum $1|cut -f 3`
  
}

hinit() {
    rm -f $dir_log/hashmap.${interface}.$1
}

hput() {
    echo "$2 $3" >> $dir_log/hashmap.${interface}.$1
}

hget() {
    grep "^$2 " $dir_log/hashmap.${interface}.$1 | awk '{ print $2 };'
}



#kinit -kt $keytab_path $keytab_user/$(hostname)
#ret_code=$?
#if [ $ret_code -ne "0"  ]
#then
#	echo "Erro na chamada do kinit"
#	exit $ret_code
#fi

interface=$1

  lst_arqs_ent=$dir_log/lst_ent_$interface.lst

  echo "Tratando interface: $interface"
  
  echo "Diretorio: $dir_entrada/$interface"

  # Pegas os nomes dos arquivos e checksum do diretorio de entrada
  #arquivos=$(lista_arq $dir_entrada/$interface)
  hadoop fs -checksum $dir_entrada/$interface/* | cut -f 1,3 > $lst_arqs_ent
  
  arquivos=`cat $lst_arqs_ent | cut -f 1`
  checksums=`cat $lst_arqs_ent | cut -f 2`
 
  echo "Arquivos:"

  if [ -n "$arquivos" ]; then
    echo "$arquivos"
  else
    echo "Diretorio de entrada $dir_entrada/$interface vazio!"
    exit 0
  fi

  # Query para o controle dos arquivos
  query="Select checksum from $tb_ctl where modulo='$interface' and checksum in ("
  
  # Condicao da query com o valor MD5 dos arquivos
  query_condicao=''

  # Array de md5 dos arquivos
  hinit md5
  indice=0
 
  # Lista todos os arquivos identificados
  for j in $checksums
  do
 
    echo $j
    # Se nao for nulo, entao tem mais de um elemento
    if [ -n "$query_condicao" ]; then
      query_condicao+=", "
    fi
   
    # guarda o valor md5 do arquivo 
    hput md5 $indice $j
 
    # Adiciona o MD5 na condicao da query
    query_condicao+="'$j'"

    ((indice=indice+1))

  done
  
echo $query_condicao
  
  hinit nome_arqs
  indice=0

  for i in $arquivos
  do
    # Armazena o nome do arquivo
    hput nome_arqs $indice $i
    ((indice=indice+1))
  done

  # Completa a query
  query+="$query_condicao);"

  echo "Verificando no banco: $query"

  # Se nao for nulo, entao tem arquivos para verificar
  if [ -n "$query_condicao" ]; then

    # Nome do arquivo de catalogados
    lst_tb_ctl_arq=$dir_log/$interface"_lst_tb_arq_"$(date -d "$dt_execucao" "+%Y%m%d")".txt"

    echo "Arquivo de catalogados: $lst_tb_ctl_arq"
   
    # Chama o beeline para executar a query e manda o resultado para o arquivo de catalogados 
    beeline -u ${bl_conn_str} --showheader=false --outputformat=csv2 -e "\"${query}\"" > $lst_tb_ctl_arq
    
    rejeicao=''
    indice_rej=''

    ind=0

 # Lista de arquivos do dir de entrada
    for y in $checksums
    do
      duplicado=0
      # Lista de arquivos catalogados
      for x in `cat $lst_tb_ctl_arq`
      do
        # se os arquivos tiverem o mesmo md5
        if [ $x == $y ]; then
           duplicado=1
           break
        fi
      done
      
      if [ $duplicado -eq 1 ]; then
          if [ -n "$rejeicao" ]; then
            rejeicao+=" "
          fi          
          
          arq=$(echo `hget nome_arqs $ind`)
         
          # Verifica se arquivo eh de 0 bytes e so rejeita se nao for 
          if $(hadoop fs -test -z $arq) ; then
               echo "Arquivo $arq com zero bytes, nao foi rejeitado"
          else
               rejeicao+="$arq"  
          fi          

      fi 
    
      ((ind=$ind+1)) 

    done

    echo $rejeicao

    echo "Rejeitando arquivos:"

    # Verifica se tem arquivos para rejeitar
    if [ -n "$rejeicao" ]; then
      echo "$rejeicao"
    else
      echo "Sem arquivos para rejeicao"
    fi

    # Query para o controle dos arquivos
    query="insert into table dl_prepago.tb_log partition (cod_programa = '138',dt = '$(date -d "$dt_execucao" "+%Y-%m-%d")') values "

    query_condicao=''

    # Lista os arquivos de rejeicao
    for k in $rejeicao
    do

      arq_rejeitado="${k##*\/}"

      arq_rejeitado="${arq_rejeitado%%.*}.$(date -d "$dt_execucao" "+%Y%m%d%H%M%S").$cod_erro"

      echo "Movendo $k para $dir_rejeitado/$interface/$arq_rejeitado"
      
      # Move o arquivo para o diretorio de rejeicao
      hadoop fs -mv $k $dir_rejeitado/$interface/$arq_rejeitado

      if [ -n "$query_condicao" ]; then
        query_condicao+=", "
      fi

      # Valores da data de processamento, Erro, Numero do erro, interface, Descricao do erro, arquivo, e data
      query_condicao+="('$(date -d "$dt_execucao" "+%Y-%m-%d %H:%M:%S")','ERRO','000001','$interface','Arquivo duplicado','$k')" 
    done

    # Completa a query
    query+="$query_condicao;"

    if [ -n "$query_condicao" ]; then
      echo "Inserindo no banco:"
      echo $query

      # Chama o beeline para executar a query
   #  beeline -u jdbc:hive2:// -e "\"$query\""
      beeline -u ${bl_conn_str} -e "\"$query\""
    fi

  fi

  # Pega o nome dos arquivos que nao foram rejeitados
  arquivos=$(lista_arq $dir_entrada/$interface)

  echo "Arquivos para processamento:"
  echo "$arquivos"

  # Query para o controle dos arquivos
  query="insert into table $tb_ctl values "

  query_condicao=''

  # Lista os arquivos nao rejeitados
  for l in $arquivos
  do
    echo "Movendo $l para $dir_processamento/$interface"
    
    md5_l=$(gerar_md5 $l)
    nome_arquivo=$(basename $l)
    
    if [ $interface == "sap01" ]; then
    	# Altera encoding e move o arquivo para o diretorio de processamento
    	hadoop fs -cat $l | iconv -f LATIN1 -t UTF-8 | hadoop fs -put -f - $dir_processamento/$interface/$nome_arquivo
    	# Remove arquivo do diretorio de entrada
    	hadoop fs -rm $l
    else
	    # Move o arquivo para o diretorio de processamento
	    hadoop fs -mv $l $dir_processamento/$interface
    fi

    # Se nao for nulo, entao tem mais de um elemento
    if [ -n "$query_condicao" ]; then
      query_condicao+=", "
    fi

    # Valores Data, nome do arquivo, MD5 do arquivo e Interface
    query_condicao+="('$(date -d "$dt_execucao" "+%Y-%m-%d %H:%M:%S")','$l','$md5_l','$interface')" 
  done

  # Completa a query
  query+="$query_condicao;"

  # Verifica se tem alguma coisa para inserir
  if [ -n "$query_condicao" ]; then
    echo "Inserindo no banco:"
    echo $query

    # Chama o beeline para executar a query
    beeline -u ${bl_conn_str} -e "\"$query\""
  fi

echo "Final verifica entrada ${interface}!"


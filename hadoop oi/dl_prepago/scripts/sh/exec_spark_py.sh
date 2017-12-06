#! /usr/bin/bash
#==============================================================
# SCRIPT ID                  : exec_spark_py.sh
# JOB ID                     : exec_spark_py.sh
# FUNCTION                   :
# ASSUMPTIONS                : 
# DATE ORIGINATED            : 2016/12/20
# AUTHOR                     : Natalia Pipas
# VERSION                    : 1.0
# CHANGE LOG:-
# VER          DATE                 WHO                    COMMENTS
# ==============================================================
#  1.0         2016/12/20                       Versao inicial.
# ==============================================================
#
#
# ==============================================================
# Execucao de script python via spark-submit
# ==============================================================

echo "Starting  : [$0 $*]" 
echo "Start Date: [`date "+%c"`]"
echo "Start user: [`whoami`]"
echo "PID       : [${PID}]"

source /data1/etl_hdp/dl_prepago/scripts/sh/env_SVA.sh

script=$(basename $0)
data_start=`date +%Y%m%d_%H%M%S`
nome_modulo=$1
data_ref=$2
data_guarda=20171220

ParamArqLog=$dir_log/${nome_modulo}_${data_ref}_${data_start}.log
echo "Log       : [${ParamArqLog}]"

formata_data_processa()
{
  data=$1

  ano=`echo $1|cut -c 1-4`
  mes=`echo $1|cut -c 5-6`
  dia=`echo $1|cut -c 7-8`

  data_processa=${ano}-${mes}-${dia}
}

get_tipo()
{
  modulo=$1

  if [ -s $dir_conf/conf_${modulo} ] ; then
     echo "Arquivo de configuracao $dir_conf/conf_${modulo} encontrado"
     tipo=$(cat $dir_conf/conf_${modulo}|grep tipo| head -1| cut -f2 -d=|cut -f2 -d\")
     if [ "${tipo}X" == "X" ] ; then 
        echo "tipo do modulo nao encontrado no arquivo de configuracao" 
        exit ${ars_ret_code}
     else 
        echo ${tipo}
     fi
  else
     echo "Arquivo de configuracao $dir_conf/conf_${modulo} nao encontrado"
     exit ${ars_ret_code}
  fi
}

get_interface()
{
  modulo=$1

  if [ -s $dir_conf/conf_${modulo} ] ; then
     echo "Arquivo de configuracao $dir_conf/conf_${modulo} encontrado"
     interface=$(cat $dir_conf/conf_${modulo}|grep interface| head -1| cut -f2 -d=|cut -f2 -d\")
     if [ "${interface}X" == "X" ] ; then 
        echo "codigo de interface do modulo nao encontrado no arquivo de configuracao" 
        exit ${ars_ret_code}
     else 
        echo "Interface ${interface}"
     fi
  else
     echo "Arquivo de configuracao $dir_conf/conf_${modulo} nao encontrado"
     exit ${ars_ret_code}
  fi
}

# Tenta criar arquivo de log

touch ${ParamArqLog}
if [ ${?} -ne 0 ]
then
	echo "Erro na criacao do arquivo de Log [${ParamArqLog}]"
	exit ${ars_ret_code}
fi
 
exec </dev/null
exec >${ParamArqLog} 2>&1

# Verifica numero de parametros

if [ $# -lt 2 ]
then
        echo "usage: ${script} NOME_MODULO DATA_PROCESSA"
        exit ${ars_ret_code}
fi

formata_data_processa ${data_ref}
get_tipo ${nome_modulo}
data_arquivo=${data_ref}

# Renova ticket Kerberos
if [ ! -s ${keytab_path} ] ; then
   echo "Arquivo keytab $keytab_path nao encontrado"
   exit ${ars_ret_code}
else
   echo "Arquivo keytab $keytab_path encontrado"
   echo "Executando kinit para $keytab_path $keytab_user/$(hostname)"
   kinit -kt $keytab_path $keytab_user/$(hostname)
   ParamRetCode=$?
   if [ $ParamRetCode -ne 0 ] ; then
      echo "Erro $ParamRetCode ao executar kinit"
      exit ${ars_ret_code}
   fi
fi

# Executa programa Spark

if [ $tipo == "rel" ] ; then
        echo "rel"
        jars=$6
        spark-submit --queue $queue --driver-memory $driver_memory --jars $jars $dir_scripts/$nome_modulo $dir_conf $data_processa $tipo $5 $7 2> ${ParamArqLog}.spark 1> ${ParamArqLog}.spark.err
        ParamRetCode=$?
        if [ $ParamRetCode -ne 0 ] ; then
                echo "*** ${nome_modulo} <Erro>: Condicao de erro ${ParamRetCode} na finalizacao do processo"
                exit ${ars_ret_code}
        fi
elif [ $tipo == "prep_dia" -o $tipo == "carga" ] ; then 
        
        data_arquivo=${data_ref}
        get_interface ${nome_modulo}
        
        if [ ${interface} != "sap01" -a ${interface} != "sap02" ] ; then 
            # Verifica arquivo no dir entrada e move para o processamento
            $dir_scripts/sh/verifica_entrada_interface.sh ${interface}
            
            ParamRetCode=$?
            if [ $ParamRetCode -ne 0 ] ; then
                    echo "*** ${nome_modulo} <Erro>: Condicao de erro ${ParamRetCode} no script verifica_entrada_interface"
                    exit ${ars_ret_code}
            fi 
        fi   
        
        # Verifica existencia do arquivo no diretorio de processamento
        nome_arquivo=$(cat $dir_conf/conf_${nome_modulo}|grep arquivo| head -1| cut -f2 -d=|cut -f2 -d\")
        echo "Nome arquivo:"
        echo $nome_arquivo
        #nome_arquivo_data="${nome_arquivo/"*"/$data_arquivo}"
        nome_arquivo_data=$(echo ${nome_arquivo}|sed "s/dataArquivo/${data_arquivo}/g")
        echo "nome_arquivo_data:"
        echo $nome_arquivo_data
        dir_processamento_completo=$(cat $dir_conf/conf_${nome_modulo}|grep dir_processamento| head -1| cut -f2 -d=|cut -f2 -d\")
        echo "dir_processamento_completo:"
        echo $dir_processamento_completo
        path_arquivo=$dir_base_hdfs$dir_processamento_completo$nome_arquivo_data
        echo "path_arquivo:"
        echo $path_arquivo
        
        if $(hdfs dfs -test -e $path_arquivo) ; then
                echo "Arquivo(s) ${path_arquivo} encontrado"
        else
                echo "Arquivo(s) ${path_arquivo} nao encontrado"
                exit ${ars_ret_code}
        fi
        
        if [ ${tipo} == "carga" -a ${interface} != "fortuna04" -a -e $dir_conf/saldo_exec_${interface}.ctl ] ; then  
           
                   echo "Carga de Saldo ${interface} ja executado"
                   echo "Nao sera executada novamente"
        else            
        
           spark-submit --queue $queue --driver-memory $driver_memory $dir_scripts/$nome_modulo $dir_conf $data_ref $tipo $data_arquivo 2> ${ParamArqLog}.spark 1> "${ParamArqLog}".spark.err
           
           ParamRetCode=$?
           if [ $ParamRetCode -ne 0 ] ; then
                   echo "*** ${nome_modulo} <Erro>: Condicao de erro ${ParamRetCode} na finalizacao do processo"
                   exit ${ars_ret_code}
           fi  
        fi
        
        # Grava arquivo de controle de saldo executado, para que o saldo so seja executado na primeira primeira vez
        if [ ${tipo} == "carga" -a ${interface} != "fortuna04" ] ; then         
            touch $dir_conf/saldo_exec_${interface}.ctl
        fi
        
        # Move arquivos processados
        dir_processado_completo=$(cat $dir_conf/conf_${nome_modulo}|grep dir_processado| head -1| cut -f2 -d=|cut -f2 -d\" )
        hdfs dfs -mv $path_arquivo $dir_base_hdfs$dir_processado_completo
        
        ParamRetCode=$?
        if [ $ParamRetCode -ne 0 ] ; then
                echo "*** ${nome_modulo} <Erro>: Condicao de erro ${ParamRetCode} na movimentacao para processado"
                exit ${ars_ret_code}
        fi 
        
        echo "Arquivos Movidos de "$path_arquivo" para "$dir_base_hdfs$dir_processado_completo

elif [ $tipo == "mv" -o $tipo == "corr" ] ; then 
        echo "tipo: ${tipo}"
        
        if [ $data_ref -gt $data_guarda ] ; then 
            
             spark-submit --queue $queue --driver-memory $driver_memory $dir_scripts/$nome_modulo $dir_conf $data_processa $tipo 1>${ParamArqLog}.spark 2> "${ParamArqLog}".spark.err
	           
             ParamRetCode=$?
             if [ $ParamRetCode -ne 0 ] ; then
                     echo "*** ${nome_modulo} <Erro>: Condicao de erro ${ParamRetCode} na finalizacao do processo"
                     exit ${ars_ret_code}
             fi 
        else
             echo "Fora do tempo de guarda - Data de referencia: $data_processa"
             echo "Job de Correlacao nao executado"
             exit 0
        fi
else   # tipo assoc e cancela
        echo "tipo: ${tipo}"
        
        spark-submit --queue $queue --driver-memory $driver_memory $dir_scripts/$nome_modulo $dir_conf $data_ref $tipo 1> ${ParamArqLog}.spark 2> "${ParamArqLog}".spark.err
	
        ParamRetCode=$?
        if [ $ParamRetCode -ne 0 ] ; then
                echo "*** ${nome_modulo} <Erro>: Condicao de erro ${ParamRetCode} na finalizacao do processo"
                exit ${ars_ret_code}
        fi 

fi

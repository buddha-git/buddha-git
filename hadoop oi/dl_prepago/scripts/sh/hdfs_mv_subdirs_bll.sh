# Importa variaveis de ambiente
source /data1/etl_hdp/dl_prepago/scripts/sh/env_SVA.sh

# Data de execucao
dt_execucao=$(date "+%Y-%m-%d %H:%M:%S")
echo $dt_execucao " DT_EXECUCAO"

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

# Chama o hdfs para ajustar os subdirs bll
hdfs dfs -mv /oi/datalake/prepago/processamento/bll06 /oi/datalake/prepago/entrada/bll06/DEBITO_20171003_20171004030027.txt
hdfs dfs -mv /oi/datalake/prepago/processamento/bll1 /oi/datalake/prepago/processamento/bll01
hdfs dfs -mv /oi/datalake/prepago/processamento/bll2 /oi/datalake/prepago/processamento/bll02
hdfs dfs -mv /oi/datalake/prepago/processamento/bll3 /oi/datalake/prepago/processamento/bll03
hdfs dfs -mv /oi/datalake/prepago/processamento/bll4 /oi/datalake/prepago/processamento/bll04
hdfs dfs -mv /oi/datalake/prepago/processamento/bll5 /oi/datalake/prepago/processamento/bll05
hdfs dfs -mv /oi/datalake/prepago/processamento/bll6 /oi/datalake/prepago/processamento/bll06
hdfs dfs -mv /oi/datalake/prepago/processamento/bll7 /oi/datalake/prepago/processamento/bll07
hdfs dfs -mv /oi/datalake/prepago/processamento/bll8 /oi/datalake/prepago/processamento/bll08
hdfs dfs -mv /oi/datalake/prepago/processamento/bll9 /oi/datalake/prepago/processamento/bll09
hdfs dfs -mv /oi/datalake/prepago/processado/bll1 /oi/datalake/prepago/processado/bll01   
hdfs dfs -mv /oi/datalake/prepago/processado/bll2 /oi/datalake/prepago/processado/bll02   
hdfs dfs -mv /oi/datalake/prepago/processado/bll3 /oi/datalake/prepago/processado/bll03   
hdfs dfs -mv /oi/datalake/prepago/processado/bll4 /oi/datalake/prepago/processado/bll04   
hdfs dfs -mv /oi/datalake/prepago/processado/bll5 /oi/datalake/prepago/processado/bll05   
hdfs dfs -mv /oi/datalake/prepago/processado/bll6 /oi/datalake/prepago/processado/bll06   
hdfs dfs -mv /oi/datalake/prepago/processado/bll7 /oi/datalake/prepago/processado/bll07   
hdfs dfs -mv /oi/datalake/prepago/processado/bll8 /oi/datalake/prepago/processado/bll08   
hdfs dfs -mv /oi/datalake/prepago/processado/bll9 /oi/datalake/prepago/processado/bll09   
hdfs dfs -mv /oi/datalake/prepago/rejeitado/bll1 /oi/datalake/prepago/rejeitado/bll01    
hdfs dfs -mv /oi/datalake/prepago/rejeitado/bll2 /oi/datalake/prepago/rejeitado/bll02    
hdfs dfs -mv /oi/datalake/prepago/rejeitado/bll3 /oi/datalake/prepago/rejeitado/bll03    
hdfs dfs -mv /oi/datalake/prepago/rejeitado/bll4 /oi/datalake/prepago/rejeitado/bll04    
hdfs dfs -mv /oi/datalake/prepago/rejeitado/bll5 /oi/datalake/prepago/rejeitado/bll05    
hdfs dfs -mv /oi/datalake/prepago/rejeitado/bll6 /oi/datalake/prepago/rejeitado/bll06    
hdfs dfs -mv /oi/datalake/prepago/rejeitado/bll7 /oi/datalake/prepago/rejeitado/bll07    
hdfs dfs -mv /oi/datalake/prepago/rejeitado/bll8 /oi/datalake/prepago/rejeitado/bll08    
hdfs dfs -mv /oi/datalake/prepago/rejeitado/bll9 /oi/datalake/prepago/rejeitado/bll09    
hdfs dfs -mv /oi/datalake/prepago/entrada/bll06/DEBITO* /oi/datalake/prepago/processamento/bll06

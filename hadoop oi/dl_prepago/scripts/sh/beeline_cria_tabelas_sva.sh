# Importa variaveis de ambiente
source /data1/etl_hdp/dl_prepago/scripts/sh/env_SVA.sh

# Data de execucao
dt_execucao=$(date "+%Y-%m-%d %H:%M:%S")
echo $dt_execucao " DT_EXECUCAO"

# Connect String beeline
bl_conn_str="jdbc:hive2://$(hostname):10000/dl_prepago;principal=hive/_HOST@$(hostname)"

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

# Chama o beeline para executar a query
beeline -u ${bl_conn_str} -f /data1/etl_hdp/dl_prepago/scripts/create_hive_tables_sva.hql >> /data1/etl_hdp/dl_prepago/log/create_hive_tables_sva.log

   ParamRetCode=$?
   if [ $ParamRetCode -ne 0 ] ; then
      echo "Erro $ParamRetCode ao executar beeline"
      exit ${ars_ret_code}
   fi


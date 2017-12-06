echo "Starting  : [$0 $*]" 
echo "Start Date: [`date "+%c"`]"
echo "Start user: [`whoami`]"
echo "PID       : [$$]"

source /data1/etl_hdp/dl_prepago/scripts/sh/env_SVA.sh

script=$(basename $0)
data_start=`date +%Y%m%d_%H%M%S`
data_ref=$1

ParamArqLog=$dir_log/${script}_${data_ref}_${data_start}.log
echo "Log       : [${ParamArqLog}]"

exec </dev/null
exec >${ParamArqLog} 2>&1

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

echo "Fechamento de Cadeia DL_Prepago para ODATE ${data_ref}" 

echo "Chamando script para compactar arquivos processados $dir_scripts/sh/hdfs_gzip_dir.sh"

$dir_scripts/sh/hdfs_gzip_dir.sh 

exit 0

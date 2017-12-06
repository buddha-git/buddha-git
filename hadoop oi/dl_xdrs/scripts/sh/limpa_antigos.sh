#!/bin/bash
#######################################################
# limpa_antigos.sh
#
# Limpa arquivos carregados no Hbase e Hive a mais de days_2_keep dias
# Limpa logs de de carga no Hbase e Hive gerados a mais de days_2_keep dias
# Limpa seus proprios logs gerados a mais de days_2_keep dias
#
#######################################################

#Incluindo arquivo de configuração passado por parâmetro
source $1

script=$(basename $0)
script=`echo ${script} | rev | cut -c 4- | rev`
data_start=`date +%Y%m%d`
ParamArqLog=$dir_log/${script}_${data_start}.log

exec </dev/null
exec >${ParamArqLog} 2>&1

echo "Preparando arquivo de log: [${ParamArqLog}]"

touch ${ParamArqLog}
if [ $? -ne 0 ] ; then
        echo "Erro na criacao do arquivo de Log [${ParamArqLog}]"
        exit ${ars_ret_code}
fi
 
# Renova ticket Kerberos
if [ ! -s ${keytab_path} ] ; then
   echo "Arquivo keytab $keytab_path nao encontrado"
   exit ${ars_ret_code}
else
   echo "Arquivo keytab $keytab_path encontrado"
   echo "Executando kinit para $keytab_path $keytab_user/$(hostname)"
   /usr/bin/kinit -kt $keytab_path $keytab_user/$(hostname)
   ParamRetCode=$?
   if [ $ParamRetCode -ne 0 ] ; then
      echo "Erro $ParamRetCode ao executar kinit"
      exit ${ars_ret_code}
   fi
fi

echo "Limpando arquivos de xdrs de ${dir_processado_xdrs} mais antigos que ${days_2_keep} dias"

qtd_arqs=`/usr/bin/hadoop jar ${search_jar} org.apache.solr.hadoop.HdfsFindTool -find ${dir_processado_xdrs} -type f -mtime +${days_2_keep} | wc -l`

if [ $qtd_arqs -gt 0 ]; then
   /usr/bin/hadoop jar ${search_jar} org.apache.solr.hadoop.HdfsFindTool -find ${dir_processado_xdrs} -type f -mtime +${days_2_keep} | /usr/bin/xargs hdfs dfs -rm -r -skipTrash
   if [ $? -ne 0 ] ; then
        echo "Erro na remocao de arquivos de xdrs antigos"
        exit ${ars_ret_code}
   fi
fi

echo "Limpando arquivos de log de carga de ${dir_log} mais antigos que ${days_2_keep} dias"
/usr/bin/find ${dir_log} -name 'carrega_hbase_hive_*.log' -mtime +${days_2_keep} -exec rm {} \;

if [ $? -ne 0 ] ; then
        echo "Erro na remocao de arquivos de log de carga antigos"
        exit ${ars_ret_code}
fi

echo "Limpando arquivos de log de limpeza de ${dir_log} mais antigos que ${days_2_keep} dias"
/usr/bin/find ${dir_log} -name '${script}*.log' -mtime +${days_2_keep} -exec rm {} \;

if [ $? -ne 0 ] ; then
        echo "Erro na remocao de arquivos de log de limpeza antigos"
        exit ${ars_ret_code}
fi
exit 0


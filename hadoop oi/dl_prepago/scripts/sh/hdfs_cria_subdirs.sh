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

# Chama o hdfs para criar os sub diretorios
hdfs dfs -mkdir /oi/datalake/prepago/processamento/bll1
hdfs dfs -mkdir /oi/datalake/prepago/processamento/bll2
hdfs dfs -mkdir /oi/datalake/prepago/processamento/bll3
hdfs dfs -mkdir /oi/datalake/prepago/processamento/bll4
hdfs dfs -mkdir /oi/datalake/prepago/processamento/bll5
hdfs dfs -mkdir /oi/datalake/prepago/processamento/bll6
hdfs dfs -mkdir /oi/datalake/prepago/processamento/bll7
hdfs dfs -mkdir /oi/datalake/prepago/processamento/bll8
hdfs dfs -mkdir /oi/datalake/prepago/processamento/bll9
hdfs dfs -mkdir /oi/datalake/prepago/processamento/bll10
hdfs dfs -mkdir /oi/datalake/prepago/processamento/bll11
hdfs dfs -mkdir /oi/datalake/prepago/processamento/fortuna01
hdfs dfs -mkdir /oi/datalake/prepago/processamento/fortuna02
hdfs dfs -mkdir /oi/datalake/prepago/processamento/fortuna03
hdfs dfs -mkdir /oi/datalake/prepago/processamento/fortuna04
hdfs dfs -mkdir /oi/datalake/prepago/processamento/fortuna05
hdfs dfs -mkdir /oi/datalake/prepago/processamento/fortuna06
hdfs dfs -mkdir /oi/datalake/prepago/processamento/arbor01
hdfs dfs -mkdir /oi/datalake/prepago/processamento/nfp01
hdfs dfs -mkdir /oi/datalake/prepago/processamento/sinn01
hdfs dfs -mkdir /oi/datalake/prepago/processamento/sap01
hdfs dfs -mkdir /oi/datalake/prepago/processamento/sap02
hdfs dfs -mkdir /oi/datalake/prepago/processamento/sap03
hdfs dfs -mkdir /oi/datalake/prepago/processamento/servcel01
hdfs dfs -mkdir /oi/datalake/prepago/processado/bll1
hdfs dfs -mkdir /oi/datalake/prepago/processado/bll2
hdfs dfs -mkdir /oi/datalake/prepago/processado/bll3
hdfs dfs -mkdir /oi/datalake/prepago/processado/bll4
hdfs dfs -mkdir /oi/datalake/prepago/processado/bll5
hdfs dfs -mkdir /oi/datalake/prepago/processado/bll6
hdfs dfs -mkdir /oi/datalake/prepago/processado/bll7
hdfs dfs -mkdir /oi/datalake/prepago/processado/bll8
hdfs dfs -mkdir /oi/datalake/prepago/processado/bll9
hdfs dfs -mkdir /oi/datalake/prepago/processado/bll10
hdfs dfs -mkdir /oi/datalake/prepago/processado/bll11
hdfs dfs -mkdir /oi/datalake/prepago/processado/fortuna01
hdfs dfs -mkdir /oi/datalake/prepago/processado/fortuna02
hdfs dfs -mkdir /oi/datalake/prepago/processado/fortuna03
hdfs dfs -mkdir /oi/datalake/prepago/processado/fortuna04
hdfs dfs -mkdir /oi/datalake/prepago/processado/fortuna05
hdfs dfs -mkdir /oi/datalake/prepago/processado/fortuna06
hdfs dfs -mkdir /oi/datalake/prepago/processado/arbor01
hdfs dfs -mkdir /oi/datalake/prepago/processado/nfp01
hdfs dfs -mkdir /oi/datalake/prepago/processado/sinn01
hdfs dfs -mkdir /oi/datalake/prepago/processado/sap01
hdfs dfs -mkdir /oi/datalake/prepago/processado/sap02
hdfs dfs -mkdir /oi/datalake/prepago/processado/sap03
hdfs dfs -mkdir /oi/datalake/prepago/processado/servcel01
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/bll1
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/bll2
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/bll3
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/bll4
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/bll5
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/bll6
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/bll7
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/bll8
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/bll9
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/bll10
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/bll11
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/fortuna01
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/fortuna02
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/fortuna03
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/fortuna04
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/fortuna05
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/fortuna06
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/arbor01
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/nfp01
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/sinn01
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/sap01
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/sap02
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/sap03
hdfs dfs -mkdir /oi/datalake/prepago/rejeitado/servcel01


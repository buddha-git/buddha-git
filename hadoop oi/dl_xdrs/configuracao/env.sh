#==============================================================
# SCRIPT ID                  : env.sh
# JOB ID                     : env.sh
# FUNCTION                   :
# ASSUMPTIONS                :
# DATE ORIGINATED            : 2017/06/26
# AUTHOR                     : 
# VERSION                    : 1.0
# CHANGE LOG:-
# VER          DATE                 WHO                    COMMENTS
# ==============================================================
#  1.0         2017/06/26                         Versao inicial.
# ==============================================================
#
#
# ==============================================================
# Definicao de Variaveis Base
# ==============================================================

keytab_user=$(whoami)
keytab_path=~/$keytab_user.keytab
dir_base="/data4/etl_hdp/dl_xdrs"
dir_scripts=$dir_base/scripts
dir_log=$dir_base/log
dir_pig_incl=$dir_scripts
hostname="hdcpx02"
PIG_OPTS="$PIG_OPTS -Dmapred.job.queue.name=root.carga.users"
  
dir_base_hdfs=/oi/datalake/xdrs
dir_processamento_xdrs=$dir_base_hdfs/processamento
dir_processado_xdrs=$dir_base_hdfs/processado
dir_entrada_xdrs=$dir_base_hdfs/entrada
dir_rejeitado_xdrs=$dir_base_hdfs/rejeitado

export dir_processamento_xdrs
export dir_processado_xdrs
export dir_entrada_xdrs
export dir_rejeitado_xdrs
export hostname
export PIG_OPTS

export dir_base
export dir_scripts
export dir_log
export dir_base_hdfs
export keytab_path
export keytab_user

ars_ret_code=4
export ars_ret_code

# Numero de regions no Hbase
hbase_regions=20
export hbase_regions

#valor expresso em dias.
tempoCorteHbase="365"
export tempoCorteHbase

#Parametros utilizados pelo limpa_antigos.sh
search_jar=/opt/cloudera/parcels/CDH/jars/search-mr-*-job.jar
days_2_keep=15


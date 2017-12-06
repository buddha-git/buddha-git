#==============================================================
# SCRIPT ID                  : env_SVA.sh
# JOB ID                     : env_SVA.sh
# FUNCTION                   :
# ASSUMPTIONS                :
# DATE ORIGINATED            : 2016/12/20
# AUTHOR                     : Natalia Pipas
# VERSION                    : 1.0
# CHANGE LOG:-
# VER          DATE                 WHO                    COMMENTS
# ==============================================================
#  1.0         2016/12/20                          Versao inicial.
# ==============================================================
#
#
# ==============================================================
# Definicao de Variaveis Base
# ==============================================================

if [ $(hostname) == "hdcpx02" ] ; then
   keytab_path="/home/phdpetl/phdpetl.keytab"
   keytab_user="phdpetl"
else
   keytab_path="/home/tr149750/tr149750.keytab"
   keytab_user="tr149750"
fi
dir_base=/data1/etl_hdp/dl_prepago
dir_base_sva=/oi/sva/
dir_scripts=$dir_base/scripts
dir_log=$dir_base/log
dir_conf=$dir_base/configuracao
#dir_conf_hdfs=/oi/sva/scripts/conf
dir_base_hdfs=/oi/datalake/prepago
dir_processamento=$dir_base_hdfs/processamento
dir_processado=$dir_base_hdfs/processado
dir_entrada=$dir_base_hdfs/entrada
dir_rejeitado=$dir_base_hdfs/rejeitado

export dir_base
export dir_scripts
export dir_log
export dir_conf
export dir_conf_hdfs
export dir_base_hdfs
export dir_base_sva
export dir_processamento
export dir_processado
export dir_entrada
export dir_rejeitado
export keytab_path
export keytab_user

# Memoria da instancia driver do Spark
driver_memory="4g"
export driver_memory

# Fila do Yarn para o Spark
queue=root.carga.users
export queue

# Codigo de retorno para abertura de ARS
ars_ret_code=4
export ars_ret_code

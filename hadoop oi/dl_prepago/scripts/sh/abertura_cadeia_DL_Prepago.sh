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

echo "Abertura de cadeia DL_Prepago para ODATE ${data_ref}" > ${ParamArqLog}

exit 0

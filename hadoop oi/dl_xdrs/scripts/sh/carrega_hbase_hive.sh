source /data4/etl_hdp/dl_xdrs/configuracao/env.sh

interface=$1
script=$(basename $0)
script=`echo ${script} | rev | cut -c 4- | rev`
data_start=`date +%Y%m%d_%H%M%S`

# Connect String beeline
bl_conn_str="jdbc:hive2://$(hostname):10000/dl_xdrs;principal=hive/_HOST@$(hostname)"

ParamArqLog=$dir_log/${script}_${interface}_${data_start}.log
echo "Log       : [${ParamArqLog}]"

touch ${ParamArqLog}
if [ $? -ne 0 ] ; then
	echo "*** $0 <Erro>: Erro na criacao do arquivo de Log [${ParamArqLog}]"
	exit 2
fi
 
exec </dev/null
exec >${ParamArqLog} 2>&1

# Obtem numero de region servers

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


if [ $hbase_regions"X" == "X" ] ; then
	echo "*** $0 <Erro>: Erro ao tentar recuperar o numero de Hbase Regions. Variavel hbase_regions nao definida"
        exit ${ars_ret_code}
else 
        numRegions=${hbase_regions}
	echo "*** $0 <sucesso>: Numero de Hbase Regions recuperado com sucesso: $numRegions Regions"
fi

# Verifica se ha arquivos no diretorio de entrada
hdfs dfs -test -e $dir_entrada_xdrs/$interface/*[0-9]
ParamRetCode=$?

if [ $ParamRetCode -eq 0 ] ; then

	# Move arquivos de entrada para processamento

	hdfs dfs -mv $dir_entrada_xdrs/$interface/*[0-9] $dir_processamento_xdrs/$interface/
	ParamRetCode=$?

	if [ $ParamRetCode -eq 0 ] ; then
		echo "*** $0 <sucesso>: Arquivos movidos de $dir_entrada_xdrs/$interface/ para $dir_processamento_xdrs/$interface/"
	else
		echo "*** $0 <Erro>: Erro ao tentar mover os arquivos do $dir_entrada_xdrs/$interface/ para $dir_processamento_xdrs/$interface/"
		exit ${ars_ret_code}
	fi
else
	echo "*** $0 <warn>: Nao ha arquivos em $dir_entrada_xdrs/$interface/"
fi


# Verifica se os arquivos estao no diretorio de processamento.
hdfs dfs -test -e $dir_processamento_xdrs/$interface/*[0-9]
ParamRetCode=$?

# Se os arquivos estiverem no diretorio de processamento, chama o script de insercao hive/hbase, caso contrario, sai do script.

if [ $ParamRetCode -eq 0 ] ; then

		pig -useHCatalog -l $dir_log -param numRegions=$numRegions -param dir_pig_incl=$dir_pig_incl -param dir_processamento=$dir_processamento_xdrs -param tempoCorteHbase=$tempoCorteHbase $dir_scripts/carrega_hbase_hive_$interface.pig


	ParamRetCode=$?
	
	if [ $ParamRetCode -eq 0 ] ; then

		echo "*** $0 <sucesso>: PIG executado com sucesso exit code ${ParamRetCode}. Executando o HQL."
		
                # Move registros do hive p/ tabela final

		if [ $interface = 'syslog' ]; then
			nomehql="insert_select_truncate_syslog.hql" 
			beeline -u ${bl_conn_str} -hiveconf interface=$interface -f $dir_scripts/insert_select_truncate_syslog.hql
		else
			nomehql="insert_select_truncate.hql"
			beeline -u ${bl_conn_str} -hiveconf interface=$interface -f $dir_scripts/insert_select_truncate.hql
		fi

		ParamRetCode=$?
		if [ $ParamRetCode -ne 0 ] ; then
			echo "*** $0 <Erro>: Condicao de erro ${ParamRetCode} na execucao do ${nomehql} para a interface ${interface}"
			ParamRetCode="2"
		else
			echo "*** $0 <sucesso>: Sucesso na execucao do ${nomehql} para a interface ${interface}"
		fi 
		
		# Move arquivos de processamento para processado
		hdfs dfs -mv $dir_processamento_xdrs/$interface/* $dir_processado_xdrs/$interface/

                if [ $ParamRetCode -eq 0 ] ; then
                    echo "*** $0 <sucesso>: Arquivos movidos de $dir_processamento_xdrs/$interface/ para $dir_processado_xdrs/$interface/"
                else
                    echo "*** $0 <Erro>: Erro ao tentar mover os arquivos do $dir_processamento_xdrs/$interface/ para $dir_processado_xdrs/$interface/"
                    exit 2
                fi
		
	else
		if [ $ParamRetCode -eq 1 ] ; then
			echo "*** $0 <Erro>: Condicao de erro ${ParamRetCode} (Retriable failure) execute novamente a interface ${interface}"
			exit 1
		else
			echo "*** $0 <Erro>: Condicao de erro ${ParamRetCode} Erro ao inserir os registros no hive/hbase"
		fi		
	fi
else
	echo "*** $0 <Erro>: Nao ha arquivos em $dir_processamento_xdrs/$interface/ . O Pig nao foi executado."
	exit 0	
fi
exit ${ParamRetCode}

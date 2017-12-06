search_jar=/opt/cloudera/parcels/CDH/jars/search-mr-*-job.jar
days_2_keep=14

echo "Compactando arquivos de prepago de ${dir_processado} mais antigos que ${days_2_keep} dias"

qtd_arqs=`/usr/bin/hadoop jar ${search_jar} org.apache.solr.hadoop.HdfsFindTool -find ${dir_processado}/*/*.txt -type f -mtime +${days_2_keep}|wc -l`

if [ $qtd_arqs -gt 0 ]; then
  
   lst_arqs=`/usr/bin/hadoop jar ${search_jar} org.apache.solr.hadoop.HdfsFindTool -find ${dir_processado}/*/*.txt -type f -mtime +${days_2_keep}`
   
   for arq in $lst_arqs
   do

     echo "Compactando arquivo $arq"

     hdfs dfs -cat $arq | gzip | hdfs dfs -put - $arq.gz

     if [ $? -ne 0 ] ; then
       
        echo "Erro na compactacao do arquivo $arqs"
     
     else 
    
        hdfs dfs -rm -skipTrash $arq

        if [ $? -ne 0 ] ; then
           echo "Erro ao remover arquivo original $arqs"
        fi
     fi

   done

else

   echo "Sem arquivos para compactar"
fi


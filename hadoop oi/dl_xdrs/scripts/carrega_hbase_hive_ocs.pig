Register $dir_pig_incl/funcs.py using jython as myfuncs;

-- Grava registros em tabela hive

hive = LOAD 'hdfs:///$dir_processamento/ocs/*' USING PigStorage(';') AS (l:chararray,f:chararray,data:chararray,a:chararray,b:chararray,t:chararray,c:chararray,ct:chararray,ea:chararray,eb:chararray,ci:chararray,p:chararray,u:chararray,cdr:chararray,dt:chararray);

S1 = STORE hive INTO 'dl_xdrs.tmp_tb_ocs' USING org.apache.hive.hcatalog.pig.HCatStorer();

-- Grava registros no hbase

hiveDataCorte = FILTER hive BY (dt > myfuncs.retorna_data_corte_hbase($tempoCorteHbase));

pernaA = foreach hiveDataCorte generate 
myfuncs.getChave($numRegions,myfuncs.adicionaZeros(data,8),myfuncs.adicionaZeros(a,15),myfuncs.adicionaZeros(b,15),myfuncs.adicionaZeros(SUBSTRING(data, 8, 14),6)) as id, 
myfuncs.aplicaSchema('l,f,a,b,c,ea,eb,ci,p,ct,u,t,cdr',l,f,a,b,c,ea,eb,ci,p,ct,u,t,cdr) as values;

S2 = STORE pernaA INTO 'hbase://ns_oilegal:OCS' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 'p:*' );

-- Atualiza tabela de controle

group_layout_dt = GROUP hive BY (l, dt);

group_layout_dt_counts = FOREACH group_layout_dt GENERATE group.dt as data,COUNT(hive) AS qt_regs,group.l as layout,'ocs' as interface;

S5 = STORE group_layout_dt_counts INTO 'dl_xdrs.tmp_tb_controle_volume_ocs' USING org.apache.hive.hcatalog.pig.HCatStorer();

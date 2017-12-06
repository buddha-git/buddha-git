Register $dir_pig_incl/funcs.py using jython as myfuncs;
Register $dir_pig_incl/Pig_Udf-0.0.1-SNAPSHOT.jar;

DEFINE toHexBin main.java.com.ibm.binarytest.Sample_Eval();

-- Grava registros em tabela hive

hive = LOAD 'hdfs:///$dir_processamento/syslog/*' USING PigStorage(';') AS (l:chararray,f:chararray,data:chararray,ipu:chararray,ipr:chararray,pi:chararray,pf:chararray,ipi:chararray,ipf:chararray,cdr:chararray,dt:chararray,ie:chararray);

hive_gravar = foreach hive generate l,f,data,ipu,ipr,pi,pf,toHexBin(ipi) as ipi,toHexBin(ipf) as ipf,cdr,dt,ie;

S1 = STORE hive_gravar INTO 'dl_xdrs.tmp_tb_syslog' USING org.apache.hive.hcatalog.pig.HCatStorer();

-- Filtra registros para ano menor que data de corte

hiveDataCorte = FILTER hive BY (dt > myfuncs.retorna_data_corte_hbase($tempoCorteHbase));

-- Grava registros no hbase 2021/2012

filtered =  FILTER hiveDataCorte BY ie != '2016';

pernaA = foreach filtered generate 
myfuncs.getChaveSyslog(ie,ipu,ipr,data,pi,$numRegions) as id,
myfuncs.aplicaSchema('l,f,ipu,ipr,pi,pf,ie,ipi,ipf,cdr',l,f,ipu,ipr,pi,pf,ie,ipi,ipf,cdr) as values;

S2 = STORE pernaA INTO 'hbase://ns_oilegal:SYSLOG' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 'p:*');

-- Grava registros no hbase 2016

filtered2016 = FILTER hiveDataCorte BY ie == '2016';

pernaB = foreach filtered2016 generate 
myfuncs.getChave($numRegions,ie,SUBSTRING(data, 0, 8),ipi,SUBSTRING(data, 8, 14)) as id,
myfuncs.aplicaSchema('l,f,ipu,ipr,pi,pf,ie,ipi,ipf,cdr',l,f,ipu,ipr,pi,pf,ie,ipi,ipf,cdr) as values;

S2 = STORE pernaB INTO 'hbase://ns_oilegal:SYSLOG' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 'p:*');

-- Atualiza tabela de controle

group_layout_dt = GROUP hive BY (l, dt);

group_layout_dt_counts = FOREACH group_layout_dt GENERATE group.dt as data,COUNT(hive) AS qt_regs,group.l as layout,'syslog' as interface;

S5 = STORE group_layout_dt_counts INTO 'dl_xdrs.tmp_tb_controle_volume_syslog' USING org.apache.hive.hcatalog.pig.HCatStorer();

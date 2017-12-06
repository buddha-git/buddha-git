Register $dir_pig_incl/funcs.py using jython as myfuncs;

-- Grava registros em tabela hive

hive = LOAD 'hdfs:///$dir_processamento/gprs/*' USING PigStorage(';') AS (l:chararray,f:chararray,data:chararray,a:chararray,ip:chararray,i6:chararray,ie:chararray,im:chararray,ci:chararray,v:chararray,tc:chararray,ct:chararray,ap:chararray,ta:chararray,cdr:chararray,dt:chararray);

S1 = STORE hive INTO 'dl_xdrs.tmp_tb_gprs' USING org.apache.hive.hcatalog.pig.HCatStorer();

-- Grava registros no hbase

hiveDataCorte = FILTER hive BY (dt > myfuncs.retorna_data_corte_hbase($tempoCorteHbase));

pernaA = foreach hiveDataCorte generate 
myfuncs.getChave($numRegions,'0',ip,SUBSTRING(data, 0, 8),myfuncs.getMd5(cdr)) as id,
myfuncs.aplicaSchema('l,f,a,ip,i6,ie,im,ci,v,ct,ap,tc,ta,cdr',l,f,a,ip,i6,ie,im,ci,v,ct,ap,tc,ta,cdr) as values;

S2 = STORE pernaA INTO 'hbase://ns_oilegal:GPRS' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 'p:*' );

pernaB = FILTER hiveDataCorte BY a is not null;

filteredB = foreach pernaB generate myfuncs.getChave($numRegions,'1',a,SUBSTRING(data, 0, 8),myfuncs.getMd5(cdr)) as id,myfuncs.getChave($numRegions,'0',ip,SUBSTRING(data, 0, 8),myfuncs.getMd5(cdr)) as i;

S3 = STORE filteredB INTO 'hbase://ns_oilegal:GPRS' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 'p:i' );

-- Atualiza tabela de controle

group_layout_dt = GROUP hive BY (l, dt);

group_layout_dt_counts = FOREACH group_layout_dt GENERATE group.dt as data,COUNT(hive) AS qt_regs,group.l as layout,'gprs' as interface;

S5 = STORE group_layout_dt_counts INTO 'dl_xdrs.tmp_tb_controle_volume_gprs' USING org.apache.hive.hcatalog.pig.HCatStorer();

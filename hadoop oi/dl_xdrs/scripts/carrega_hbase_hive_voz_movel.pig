Register $dir_pig_incl/funcs.py using jython as myfuncs;

-- Grava registros em tabela hive

hive = LOAD 'hdfs:///$dir_processamento/voz_movel/*' USING PigStorage(';') AS (l:chararray,f:chararray,data:chararray,a:chararray,b:chararray,d:chararray,ie:chararray,im:chararray,ci:chararray,g:chararray,ct:chararray,r:chararray,t:chararray,la:chararray,cg:chararray,call_reference:chararray,cdr:chararray,dt:chararray);

S1 = STORE hive INTO 'dl_xdrs.tmp_tb_voz_movel' USING org.apache.hive.hcatalog.pig.HCatStorer();

-- Grava registros no hbase

hiveDataCorte = FILTER hive BY (dt > myfuncs.retorna_data_corte_hbase($tempoCorteHbase));

pernaA = foreach hiveDataCorte generate 
myfuncs.getChaveVozMovel(data,a,'A',g,t,call_reference,$numRegions) as id,
myfuncs.aplicaSchema('l,f,a,b,d,ie,im,ci,g,ct,r,t,la,cg,cdr',l,f,a,b,d,ie,im,ci,g,ct,r,t,la,cg,cdr) as values;

S2 = STORE pernaA INTO 'hbase://ns_oilegal:VOZ_MOVEL' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 'p:*' );

pernaB = FILTER hiveDataCorte BY b is not null;

filteredB = foreach pernaB generate myfuncs.getChaveVozMovel(data,b,'B',g,t,call_reference,$numRegions) as id,myfuncs.getChaveVozMovel(data,a,'A',g,t,call_reference,$numRegions) as i;

S3 = STORE filteredB INTO 'hbase://ns_oilegal:VOZ_MOVEL' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 'p:i' );

-- Atualiza tabela de controle

group_layout_dt = GROUP hive BY (l, dt);

group_layout_dt_counts = FOREACH group_layout_dt GENERATE group.dt as data,COUNT(hive) AS qt_regs,group.l as layout,'voz_movel' as interface;

S4 = STORE group_layout_dt_counts INTO 'dl_xdrs.tmp_tb_controle_volume_voz_movel' USING org.apache.hive.hcatalog.pig.HCatStorer();

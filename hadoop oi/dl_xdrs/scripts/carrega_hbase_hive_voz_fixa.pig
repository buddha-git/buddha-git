Register $dir_pig_incl/funcs.py using jython as myfuncs;


-- Grava registros em tabela hive

hive = LOAD 'hdfs:///$dir_processamento/voz_fixa/*' USING PigStorage('|') AS (l:chararray,f:chararray,data:chararray,a:chararray,b:chararray,c:chararray,d:chararray,bi:chararray,u:chararray,re:chararray,rs:chararray,t:chararray,cp:chararray,s:chararray,ct:chararray,cs:chararray,co:chararray,je:chararray,js:chararray,fp:chararray,af:chararray,bf:chararray,cdr:chararray,dt:chararray);

S1 = STORE hive INTO 'dl_xdrs.tmp_tb_voz_fixa' USING org.apache.hive.hcatalog.pig.HCatStorer();

-- Grava registros no hbase

hiveDataCorte = FILTER hive BY (dt > myfuncs.retorna_data_corte_hbase($tempoCorteHbase));

pernaA = foreach hiveDataCorte generate 
myfuncs.getChave($numRegions,SUBSTRING(data, 0, 8),a,'A',bi,myfuncs.getMd5(cdr)) as id,
myfuncs.aplicaSchema('l,f,a,b,c,d,bi,u,re,rs,t,cp,s,ct,cs,co,je,js,fp,af,bf,cdr',l,f,a,b,c,d,bi,u,re,rs,t,cp,s,ct,cs,co,je,js,fp,af,bf,cdr) as values;

S2 = STORE pernaA INTO 'hbase://ns_oilegal:VOZ_FIXA' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 'p:*' );

pernaB = FILTER hiveDataCorte BY b is not null;

filteredB = foreach pernaB generate myfuncs.getChave($numRegions,SUBSTRING(data, 0, 8),b,'B',bi,myfuncs.getMd5(cdr)) as id,myfuncs.getChave($numRegions,SUBSTRING(data, 0, 8),a,'A',bi,myfuncs.getMd5(cdr)) as i;

S3 = STORE filteredB INTO 'hbase://ns_oilegal:VOZ_FIXA' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 'p:i' );

pernaC = FILTER hiveDataCorte BY c is not null;

filteredC = foreach pernaC generate myfuncs.getChave($numRegions,SUBSTRING(data, 0, 8),c,'C',bi,myfuncs.getMd5(cdr)) as id,myfuncs.getChave($numRegions,SUBSTRING(data, 0, 8),a,'A',bi,myfuncs.getMd5(cdr));

S4 = STORE filteredC INTO 'hbase://ns_oilegal:VOZ_FIXA' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 'p:i' );

-- Atualiza tabela de controle

group_layout_dt = GROUP hive BY (l, dt);

group_layout_dt_counts = FOREACH group_layout_dt GENERATE group.dt as data,COUNT(hive) AS qt_regs,group.l as layout,'voz_fixa' as interface;

S4 = STORE group_layout_dt_counts INTO 'dl_xdrs.tmp_tb_controle_volume_voz_fixa' USING org.apache.hive.hcatalog.pig.HCatStorer();
Register $dir_pig_incl/funcs.py using jython as myfuncs;
Register $dir_pig_incl/Pig_Udf-0.0.1-SNAPSHOT.jar;

DEFINE toHexBin main.java.com.ibm.binarytest.Sample_Eval();

-- Grava registros em tabela hive

hive = LOAD 'hdfs:///$dir_processamento/dial/*' USING PigStorage('\u0001') AS (l:chararray,f:chararray,di:chararray,a:chararray,data:chararray,tl:chararray,ip:chararray,i6di:chararray,i6df:chararray,i6fi:chararray,i6ff:chararray,in:chararray,e:chararray,un:chararray,nu:chararray,sf:chararray,cp:chararray,s:chararray,id_sessao_md5:chararray,cdr:chararray,dt:chararray);

hive_gravar = foreach hive generate l,f,di,a,data,tl,ip,toHexBin(i6di) as i6di,toHexBin(i6df) as i6df,toHexBin(i6fi) as i6fi,toHexBin(i6ff) as i6ff,in,e,un,nu,sf,cp,s,id_sessao_md5,cdr,dt;

S1 = STORE hive_gravar INTO 'dl_xdrs.tmp_tb_dial' USING org.apache.hive.hcatalog.pig.HCatStorer();

-- Grava registros no hbase

hiveDataCorte = FILTER hive BY (dt > myfuncs.retorna_data_corte_hbase($tempoCorteHbase));

pernaA = foreach hiveDataCorte generate 
myfuncs.getChave($numRegions,id_sessao_md5,SUBSTRING(data, 0, 8),myfuncs.getMd5(cdr)) as id,
myfuncs.aplicaSchema('l,f,di,a,tl,ip,i6di,i6df,i6fi,i6ff,in,e,un,nu,sf,cp,s,cdr',l,f,di,a,tl,ip,i6di,i6df,i6fi,i6ff,in,e,un,nu,sf,cp,s,cdr) as values;

S2 = STORE pernaA INTO 'hbase://ns_oilegal:DIAL' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 'p:*' );

pernaB = FILTER hiveDataCorte BY ip is not null;

filteredB = foreach pernaB generate myfuncs.getChave($numRegions,'0',ip,SUBSTRING(data, 0, 8),myfuncs.getMd5(cdr)) as id,myfuncs.getChave($numRegions,id_sessao_md5,SUBSTRING(data, 0, 8),myfuncs.getMd5(cdr)) as i;

S3 = STORE filteredB INTO 'hbase://ns_oilegal:DIAL' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 
'p:i' );

pernaC = FILTER hiveDataCorte BY a is not null;

filteredC = foreach pernaC generate myfuncs.getChave($numRegions,'1',a,SUBSTRING(data, 0, 8),myfuncs.getMd5(cdr)) as id, myfuncs.getChave($numRegions,id_sessao_md5,SUBSTRING(data, 0, 8),myfuncs.getMd5(cdr)) as i;

S4 = STORE filteredC INTO 'hbase://ns_oilegal:DIAL' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 
'p:i' );

-- Atualiza tabela de controle

group_layout_dt = GROUP hive BY (l, dt);

group_layout_dt_counts = FOREACH group_layout_dt GENERATE group.dt as data,COUNT(hive) AS qt_regs,group.l as layout,'dial' as interface;

S5 = STORE group_layout_dt_counts INTO 'dl_xdrs.tmp_tb_controle_volume_dial' USING org.apache.hive.hcatalog.pig.HCatStorer();



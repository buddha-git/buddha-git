set hive.exec.max.dynamic.partitions=100000;

set hive.exec.max.dynamic.partitions.pernode=10000;

set hive.exec.dynamic.partition.mode=nonstrict;

insert into table dl_xdrs.tb_${hiveconf:interface} partition (dt,Id_Evento) select * from dl_xdrs.tmp_tb_${hiveconf:interface};

use dl_xdrs;

truncate table tmp_tb_${hiveconf:interface};

create table tb_controle_volume_tmp_${hiveconf:interface} like tb_controle_volume;

insert into table tb_controle_volume_tmp_${hiveconf:interface} partition(interface) select * from tb_controle_volume  where interface = '${hiveconf:interface}' union all select * from tmp_tb_controle_volume_${hiveconf:interface};

ALTER TABLE tb_controle_volume DROP IF EXISTS PARTITION(interface = '${hiveconf:interface}');

TRUNCATE TABLE tmp_tb_controle_volume_${hiveconf:interface};

insert into table tb_controle_volume partition (interface) select DATA,SUM(QT_REGS),LAYOUT,interface from tb_controle_volume_tmp_${hiveconf:interface} group by DATA,LAYOUT,interface;

drop table tb_controle_volume_tmp_${hiveconf:interface};



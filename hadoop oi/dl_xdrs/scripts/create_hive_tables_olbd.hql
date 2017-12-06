create table DL_XDRS.TMP_TB_CONTROLE_VOLUME_ADSl 
(
DATA STRING, 
QT_REGS BIGINT,
LAYOUT string,
INTERFACE string
);

create table DL_XDRS.TMP_TB_CONTROLE_VOLUME_VOZ_MOVEL 
(
DATA STRING, 
QT_REGS BIGINT,
LAYOUT string,
INTERFACE string
);

create table DL_XDRS.TMP_TB_CONTROLE_VOLUME_VOZ_FIXA
(
DATA STRING, 
QT_REGS BIGINT,
LAYOUT string,
INTERFACE string
);

create table DL_XDRS.TMP_TB_CONTROLE_VOLUME_DIAL
(
DATA STRING, 
QT_REGS BIGINT,
LAYOUT string,
INTERFACE string
);

create table DL_XDRS.TMP_TB_CONTROLE_VOLUME_FTTX
(
DATA STRING, 
QT_REGS BIGINT,
LAYOUT string,
INTERFACE string
);

create table DL_XDRS.TMP_TB_CONTROLE_VOLUME_GPRS
(
DATA STRING, 
QT_REGS BIGINT,
LAYOUT string,
INTERFACE string
);

create table DL_XDRS.TMP_TB_CONTROLE_VOLUME_MMS
(
DATA STRING, 
QT_REGS BIGINT,
LAYOUT string,
INTERFACE string
);

create table DL_XDRS.TMP_TB_CONTROLE_VOLUME_OCS
(
DATA STRING, 
QT_REGS BIGINT,
LAYOUT string,
INTERFACE string
);

create table DL_XDRS.TMP_TB_CONTROLE_VOLUME_SMS
(
DATA STRING, 
QT_REGS BIGINT,
LAYOUT string,
INTERFACE string
);

create table DL_XDRS.TMP_TB_CONTROLE_VOLUME_SYSLOG
(
DATA STRING, 
QT_REGS BIGINT,
LAYOUT string,
INTERFACE string
);

create table DL_XDRS.TB_CONTROLE_VOLUME 
(
DATA STRING, 
QT_REGS BIGINT,
LAYOUT string
)
PARTITIONED BY (interface STRING)
;

create table DL_XDRS.TMP_TB_CONTROLE_VOLUME 
(
DATA STRING, 
QT_REGS BIGINT,
LAYOUT string
)
PARTITIONED BY (interface STRING)
;

create table dl_xdrs.tmp_tb_voz_fixa
(
l string,
f string,
data string,
a string,
b string,
c string,
d string,
bi string,
u string,
re string,
rs string,
t string,
cp string,
s string,
ct string,
cs string,
co string,
je string,
js string,
fp string,
af string,
bf string,
cdr string,
dt string
);

create table dl_xdrs.tb_voz_fixa
(
layout string,
nome_arq_original string,
data string,
num_a string,
num_b string,
num_c string,
duracao string,
bilhetador string,
uf string,
rota_entrada string,
rota_saida string,
parte_tarifada string,
csp string,
fim_selecao string,
categoria string,
causa_saida string,
contador_parciais string,
juntor_entrada string,
juntor_saida string,
flag_portabilidade string,
num_fisico_a string,
num_fisico_b string,
cdr string
)
PARTITIONED BY (dt string)
CLUSTERED BY(num_a) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

create table dl_xdrs.tmp_tb_voz_movel
(
l string,
f string,
data string,
a string,
b string,
d string,
ie string,
im string,
ci string,
g string,
ct string,
r string,
t string,
la string,
cg string,
call_reference string,
cdr string,
dt string
)
STORED AS TEXTFILE;
   
create table dl_xdrs.tb_voz_movel
(
layout string,
nome_arq_original string,
data string,
num_a string,
num_b string,
duracao string,
imei string,
imsi string,
celula string,
gtt string,
causa_terminacao string,
rota string,
tipo_cdr string,
lac string,
cgi string,
call_reference string,
cdr string
)
PARTITIONED BY (dt string)
CLUSTERED BY(num_a) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 

create table dl_xdrs.tmp_tb_gprs
(
l string,
f string,
data string,
a string,
ip string,
i6 string,
ie string,
im string,
ci string,
v string,
tc string,
ct string,
ap string,
ta string,
cdr string,
dt string
);

create table dl_xdrs.tb_gprs
(
layout string,
nome_arq_original string,
data string,
num_a string,
IP string,
IPv6 string,
imei string,
imsi string,
celula string,
volume string,
tipo_Cdr string,
causa_terminacao string,
apn string,
tipo_acesso string,
cdr string
)
PARTITIONED BY (dt string)
CLUSTERED BY(IP) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 

create table dl_xdrs.tmp_tb_sms
(l string,
f string,
data string,
a string,
b string,
t string,
ca string,
ct string,
ea string,
eb string,
cdr string,
dt string
);


create table dl_xdrs.tb_sms
(layout string,
nome_arq_original string,
data string,
num_a string,
num_b string,
tipo_cdr string,
cnl_a string,
causa_terminacao string,
eot_a string,
eot_b string,
cdr string
)
PARTITIONED BY (dt string)
CLUSTERED BY(num_a) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 

create table dl_xdrs.tmp_tb_mms
(l string,
f string,
data string,
a string,
b string,
t string,
ca string,
ct string,
ea string,
eb string,
cdr string,
dt string);

create table dl_xdrs.tb_mms
(layout string,
nome_arq_original string,
data string,
num_a string,
num_b string,
tipo_cdr string,
cnl_a string,
causa_terminacao string,
eot_a string,
eot_b string,
cdr string)
PARTITIONED BY (dt string)
CLUSTERED BY(num_a) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 

create table dl_xdrs.tmp_tb_ocs
(
l string,
f string,
data string,
a string,
b string,
t string,
c string,
ct string,
ea string,
eb string,
ci string,
p string,
u string,
cdr string,
dt string
);

create table dl_xdrs.tb_ocs
(
layout string,
nome_arq_original string,
data string,
num_a string,
num_b string,
tipo_cdr string,
central string,
causa_terminacao string,
eot_a string,
eot_b string,
celula string,
parceiro string,
tipo_uso string,
cdr string
)
PARTITIONED BY (dt string)
CLUSTERED BY(num_a) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 


create table dl_xdrs.tmp_tb_dial
(
l string,
f string,
di string,
a string,
data string,
tl string,
ip string,
i6di binary,
i6df binary,
i6fi binary,
i6ff binary,
in string,
e string,
un string,
nu string,
sf string,
cp string,
s string,
id_sessao_md5 string,
cdr string,
dt string
);

create table dl_xdrs.tb_dial
(
layout string,
nome_arq_original string,
data_inicial string,
num_a string,
data_final string,
tipo_log string,
ip string,
delegated_ipv6_inicio binary,
delegated_ipv6_fim  binary,
framed_ipv6_inicio binary,
framed_ipv6_fim binary,
ip_nas string,
cod_estacao string,
uf_nas string,
nome_usuario string,
st_faturamento string,
cod_provedor string,
id_sessao string,
id_sessao_md5 string,
cdr string
)
PARTITIONED BY (dt string)
CLUSTERED BY(id_sessao_md5) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 

create table dl_xdrs.tmp_tb_adsl
(
l string,
f string,
di string,
a string,
data string,
tl string,
ip string,
nc string,
lc string,
i6di binary,
i6df binary,
i6fi binary,
i6ff binary,
in string,
e string,
un string,
nu string,
sf string,
cp string,
s string,
id_sessao_md5 string,
cdr string,
dt string
);

create table dl_xdrs.tb_adsl
(
layout string,
nome_arq_original string,
data_inicial string,
num_a string,
data_final string,
tipo_log string,
ip string,
num_circuito string,
local_circuito string,
delegated_ipv6_inicio binary,
delegated_ipv6_fim binary,
framed_ipv6_inicio binary,
framed_ipv6_fim binary,
ip_nas string,
cod_estacao string,
uf_nas string,
nome_usuario string,
st_faturamento string,
cod_provedor string,
id_sessao string,
id_sessao_md5 string,
cdr string
)
PARTITIONED BY (dt string)
CLUSTERED BY(id_sessao_md5) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 

create table dl_xdrs.tmp_tb_syslog
(
l string,
f string,
data string,
ipu string,
ipr string,
pi string,
pf string,
ipi binary,
ipf binary,
cdr string,
dt string,
ie string
);

create table dl_xdrs.tb_syslog
(
layout string,
nome_arq_original string,
data string,
IP_Publico string,
IP_Privado string,
Porta_Inicial string,
Porta_Final string,
IP_Pub_PollDown_Inicial binary,
IP_Pub_PollDown_Final binary,
cdr string
)
PARTITIONED BY (dt string,Id_Evento string)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 

create table dl_xdrs.tmp_tb_FTTX 
(
l string,
f string,
di string,
a string,
data string,
tl string,
ip string,
nc string,
lc string,
i6di binary,
i6df binary,
i6fi binary,
i6ff binary,
in string,
e string,
un string,
nu string,
sf string,
cp string,
s string,
id_sessao_md5 string,
cdr string,
dt string
);

create table dl_xdrs.tb_FTTX 
(
layout string,
nome_arq_original string,
data_inicial string,
num_a string,
data_final string,
tipo_log string,
ip string,
num_circuito string,
local_circuito string,
delegated_ipv6_inicio binary,
delegated_ipv6_fim binary,
framed_ipv6_inicio binary,
framed_ipv6_fim binary,
ip_nas string,
cod_estacao string,
uf_nas string,
nome_usuario string,
st_faturamento string,
cod_provedor string,
id_sessao string,
id_sessao_md5 string,
cdr string
)
PARTITIONED BY (dt string)
CLUSTERED BY(id_sessao_md5) INTO 16 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 

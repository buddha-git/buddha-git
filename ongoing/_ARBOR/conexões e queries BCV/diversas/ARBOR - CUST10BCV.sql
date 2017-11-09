  select --/*+ PARALLEL(4) */
/*+ INDEX */
  cdr.cdr_data_partition_key as key_cdr_data, 
  cdrb.cdr_data_partition_key as key_cdr_billed,
  cdr.rate_dt,
  cdr.trans_dt,
  cdrb.trans_dt,
  TO_CHAR(cdr.trans_dt, 'yyyy-mm-dd hh:mm:ss') 
from 
  cdr_data partition (CDR_DATA_P8) cdr,
  cdr_billed partition (cdr_billed_p8) cdrb
where
  cdr.msg_id = cdrb.msg_id and
  cdr.msg_id2 = cdrb.msg_id2 and
  cdr.msg_id_serv = cdrb.msg_id_serv and
  cdr.subscr_no = cdrb.subscr_no and
  cdr.subscr_no_resets = cdrb.subscr_no_resets and
  cdr.cdr_data_partition_key = cdrb.cdr_data_partition_key and
  cdr.trans_dt > '14/09/16' and
  cdr.trans_dt < '15/09/16' and
  --TO_CHAR(trans_dt, 'yyyy-mm-dd hh:mm:ss') > '2016-09-14 09:00:00' and
  TO_CHAR(cdr.trans_dt, 'yyyy-mm-dd hh:mm:ss') < '2016-09-14 12:00:00'
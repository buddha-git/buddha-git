set pagesize 0;
SET LINESIZE 600;
spool C:\ARBOR_BCV\bases\FAT_TRAF_14092016_CT02BCV.TXT
set trimspool on;

SELECT 
  bi.bill_ref_no || ';' ||
  TO_CHAR(cdr.trans_dt, 'yyyy-mm-dd hh:mm:ss') || ';' ||
  e.external_id || ';' ||
  cdr.rate_dt  || ';' ||
  cdr.type_id_usg  || ';' ||
  cdr.no_bill  || ';' ||
  (select d.description_text
  from descriptions d, usage_types up
  where d.description_code = up.description_code
  and cdr.type_id_usg = up.type_id_usg)  || ';' ||
  cdr.rated_units/10 || ';' ||
  cdrb.units_credited/10 || ';' ||
  cdr.point_origin || ';' ||
  cdr.point_target || ';' ||
  cdr.corridor_plan_id || ';' ||
  cdr.amount/100 || ';' ||
  cdrb.amount_credited/100 || ';' ||
  (cdr.amount-cdrb.amount_credited)/100 || ';' ||
  cdrb.unit_cr_id || ';' ||
  d.description_text || ';' ||
  cdr.open_item_id || ';' ||
  cdr.file_id || ';' ||
  cdr.provider_id || ';' ||
  cdr.annotation
FROM
  bill_invoice bi,
  cdr_data cdr,
  cdr_billed cdrb,
  unit_cr_definitions ucd,
  unit_cr_plans ucp,
  descriptions d ,
  customer_id_equip_map e,
  provider_class_values pcv  
where
  bi.bill_ref_no = cdrb.bill_ref_no
  AND bi.bill_ref_resets = cdrb.bill_ref_resets
  and cdr.subscr_no = e.subscr_no
  and e.external_id_type = 201
  and e.inactive_date is null
  AND cdrb.msg_id = cdr.msg_id
  AND cdrb.msg_id2 = cdr.msg_id2
  AND cdrb.msg_id_serv = cdr.msg_id_serv
  AND cdrb.split_row_num = cdr.split_row_num
  AND cdrb.cdr_data_partition_key = cdr.cdr_data_partition_key
  AND cdrb.unit_cr_id  = ucd.unit_cr_id (+)  
  and ucd.description_code = d.description_code (+) 
  and ucp.unit_cr_id (+) = ucd.unit_cr_id 
  and cdr.provider_id = pcv.provider_class (+)
 AND TO_CHAR(cdr.trans_dt, 'yyyy-mm-dd hh:mm:ss') BETWEEN '2016-09-14 09:00:00' AND  '2016-09-14 12:00:00';

spool off
quit;




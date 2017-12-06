use digital_stg;
 
truncate table stg_facebook_de_para_1;

insert into stg_facebook_de_para_1
Select
    fb.campaign_name as campanha,
    fb.`date` as dia,
	upper(fb.device) as dispositivo,
	fb.spend as custo,
	fb.reach as alcance, 
	fb.inline_link_clicks as clicks_links,
	0 as resultado,
	fb.impressions as impressoes,
    case
        when upper(fb.campaign_name) like '%FB%' then 'FACEBOOK'
        end as origem,
    case
        when upper(fb.campaign_name) like '%MOB%' then 'MOBILIDADE'
        when upper(fb.campaign_name) like '%RES%' then 'RESIDENCIAL'
        end as segmento,
    case
        when upper(fb.campaign_name) like '%CTL%' then 'CONTROLE'
        when upper(fb.campaign_name) like '%BANDA LARGA%' then 'BANDA LARGA'
        when upper(fb.campaign_name) like '%TV%' then 'TV'
        when upper(fb.campaign_name) like '%FIXO%' then 'FIXO'
        when upper(fb.campaign_name) like '%COMBO%' then 'COMBO'
        when upper(fb.campaign_name) like '%PÓS%' then 'POS-PAGO'
        when upper(fb.campaign_name) like '%INSTITUCIONAL%' then 'INSTITUCIONAL'
		end as produto,
    case
        when upper(fb.campaign_name) like '%LEADADS%' then 'DISPLAY'
        when upper(fb.campaign_name) like '%NEWSFEED%' then 'DISPLAY'
    end as midia,
    case
        when upper(fb.campaign_name) like '%LEADADS%' then 'LEADADS'
        when upper(fb.campaign_name) like '%NEWSFEED%' then 'NEWSFEED'
    end as submidia,
    from_unixtime(unix_timestamp()) as dt_carga
from 
	digital_stg.stg_facebook_platforms_report fb
where fb.fonte = 'VML';

select * from vin_populate_item_out
where cod_art = any ((select array(select item 
from tmd_items ti 
where is_updated = 1 
and UPPER(last_user) = COALESCE(:user, UPPER(last_user)) 
and :par = 2 
union 
select ti.item 
from tmd_item_sales tis 
inner join tmd_items ti on ti.id = tis.item_id 
where tis.is_updated = 1 
and UPPER(tis.last_user) = COALESCE(:user, UPPER(tis.last_user)) 
and :par = 2 
union 
select ti.item 
from tmd_logistic_units tlu 
inner join tmd_item_logistics til on til.id = tlu.item_logistic_id 
inner join tmd_items ti on ti.id = til.item_id 
where tlu.is_updated = 1 
and tlu.logistic_unit_pc = '41' 
and UPPER(tlu.last_user) = COALESCE(:user, UPPER(tlu.last_user)) 
and :par = 2 
union 
select ti.item 
from tmd_sale_codes_var tscv 
inner join tmd_item_sales tis on tis.id = tscv.item_sale_id 
inner join tmd_items ti on ti.id = tis.item_id 
inner join tpa_parameters tp on tp.parameter_code = tscv.code_type_pc and 
								tp.parameter_header_id = (select id from tpa_parameter_headers tph where tph.parameter_header = tscv.code_type_ph) and 
								tp.num_val_5 = 1 
where tscv.is_updated = 1 
and UPPER(tscv.last_user) = COALESCE(:user, UPPER(tscv.last_user)) 
and :par = 2 
and current_date between tscv.start_date and tscv.end_date 
union 
select ti.item 
from tmd_items ti 
inner join tmd_kit_components tkc on tkc.item_id=ti.id 
where :par = 2 
and tkc.is_updated=1 
and UPPER(tkc.last_user) = COALESCE(:user, UPPER(tkc.last_user)) 
union 
select ti.item 
from tmd_sale_prices_var tspv2 
inner join tmd_item_sales tis on tis.id = tspv2.item_sale_id 
inner join tmd_items ti on ti.id = tis.item_id 
inner join vpa_parameters vp on vp.parameter_header =tspv2.sale_price_type_ph and vp.is_default =1 and vp.parameter_code = tspv2.sale_price_type_pc and vp.num_val_1=1 
where :par = 2 
and tspv2.is_sent = 0 
and UPPER(tspv2.last_user) = COALESCE(:user, UPPER(tspv2.last_user)) 
and current_date between tspv2.start_date and tspv2.end_date 
union 
select ti.item 
from tmd_structure_item_links_var lnk 
inner join tmd_items ti on ti.id=lnk.item_id 
inner join tmd_structures ts on ts.id = lnk.structure_id 
inner join tmd_merchandise_structures tms on tms.id = ts.merchandise_structure_id and tms.is_default =1 
where :par = 2 
and lnk.is_updated=1 
and UPPER(lnk.last_user) = COALESCE(:user, UPPER(lnk.last_user)) 
and current_date between lnk.start_date and lnk.end_date 
union 
select ti.item 
from tmd_feature_item_links tfil 
inner join tmd_items ti on ti.id = tfil.item_id 
inner join tpa_specific_features tsf on tsf.id = tfil.specific_feature_id and tsf.used_scales = 1 and tsf.is_active = 1 
where :par = 2 
and tfil.is_updated = 1 
and UPPER(tfil.last_user) = COALESCE(:user, UPPER(tfil.last_user)) 
union 
select td.item 
from tin_data_item_in td 
inner join tmd_items ti on ti.item = td.item 
where :par = 1 
and processing_step3 = 1 
and processing_step4 = 0 
and td.transaction_code=#TRANSACTION# 
union 
select ti.item 
from tmd_sale_codes_var tscv inner join tmd_item_sales tis on tscv.item_sale_id = tis.id 
inner join tmd_items ti on ti.id = tis.item_id 
inner join tpa_parameters tp on tp.parameter_code = tscv.code_type_pc and 
								tp.parameter_header_id = (select id from tpa_parameter_headers tph where tph.parameter_header = tscv.code_type_ph) and 
								tp.num_val_5 = 1 
where tscv.is_updated = 1 
and tscv.last_user = 'user_batch' 
and :par = 1 
and current_date between tscv.start_date and tscv.end_date 
union 
select ti.item 
from tmd_sale_prices_var tspv2 
inner join tmd_item_sales tis on tis.id = tspv2.item_sale_id 
inner join tmd_items ti on ti.id = tis.item_id 
where :par = 1 
and tspv2.is_sent = 0 
and tspv2.last_user = 'user_batch' 
and current_date between tspv2.start_date and tspv2.end_date 
and exists (select 1 
								from vpa_parameters vp 
								where vp.parameter_header =tspv2.sale_price_type_ph 
								  and vp.is_default =1 
								  and vp.parameter_code = tspv2.sale_price_type_pc 
								  and vp.num_val_1=1 ) 
union 
select ti.item 
from tmd_structure_item_links_var lnk 
inner join tmd_items ti on ti.id = lnk.item_id 
inner join tmd_structures ts on ts.id = lnk.structure_id 
inner join tmd_merchandise_structures tms on tms.id = ts.merchandise_structure_id and tms.is_default =1 
where :par = 1 
and lnk.is_updated=1 
and lnk.last_user = 'user_batch' 
and current_date between lnk.start_date and lnk.end_date 
))::varchar[] )
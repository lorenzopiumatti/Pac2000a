--create table boom.save20250917_controlla_prezzi as 
--SELECT sz_code, dt_valid_from, dt_valid_until, n0_price, n0_price::numeric /100, n0_priority, bl_disable_promos, bl_with_loyalty
--FROM elvispos.article_price_list
--where current_date between dt_valid_from and  dt_valid_until


-- export da  dbeaver

-- import da dbeaver 

SELECT sp.* ,'#'x, ti.item,'#'x, tp.price , tp.promo_code
FROM save20250917_controlla_prezzi sp, 
	tmd_items ti , 
	tmd_item_Sales ts ,
	tmd_Sale_prices tp 
where ti.item = sp.sz_code 
and ts.item_id = ti.id
and tp.item_Sale_id = ts.id
and current_date between tp.start_date and tp.end_date
and tp.promo_code is  null
and sp.n0_price_eur <> tp.price 
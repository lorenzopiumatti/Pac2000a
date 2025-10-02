
select  * from tmd_orderable_assortments_var 
where  start_date = TO_DATE('15-09-2025', 'DD-MM-YYYY')
and item_id in 
(select a.item_id from(
select item_id,item_logistic_id,logistic_unit_id,operational_agreement_id, network_id ,count(*)
from tmd_orderable_assortments_var toav where 1=1 
and current_date between start_date and  end_date
group  by item_id,item_logistic_id,logistic_unit_id,operational_agreement_id, network_id 
having count(*)>1
) 
a )
order by item_id , start_date


select  * from tmd_orderable_assortments
where  start_date = TO_DATE('15-09-2025', 'DD-MM-YYYY')
and item_id in 
(select a.item_id from(
select item_id,item_logistic_id,logistic_unit_id,operational_agreement_id, network_id ,count(*)
from tmd_orderable_assortments toav where 1=1 
and current_date between start_date and  end_date
group  by item_id,item_logistic_id,logistic_unit_id,operational_agreement_id, network_id 
having count(*)>1
) 
a )
order by item_id , start_date



update tmd_orderable_assortments_var  set end_date = start_date
where  start_date = TO_DATE('15-09-2025', 'DD-MM-YYYY')
and item_id in 
(select a.item_id from(
select item_id,item_logistic_id,logistic_unit_id,operational_agreement_id, network_id ,count(*)
from tmd_orderable_assortments_var toav where 1=1 
and current_date between start_date and  end_date
group  by item_id,item_logistic_id,logistic_unit_id,operational_agreement_id, network_id 
having count(*)>1
) 
a )


update tmd_orderable_assortments  set end_date = start_date
where  start_date = TO_DATE('15-09-2025', 'DD-MM-YYYY')
and item_id in 
(select a.item_id from(
select item_id,item_logistic_id,logistic_unit_id,operational_agreement_id, network_id ,count(*)
from tmd_orderable_assortments toav where 1=1 
and current_date between start_date and  end_date
group  by item_id,item_logistic_id,logistic_unit_id,operational_agreement_id, network_id 
having count(*)>1
) 
a )
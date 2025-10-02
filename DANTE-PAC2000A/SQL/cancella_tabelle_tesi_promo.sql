---- /****** non cancellare ******/ SELECT n0_id, n0_company_id, n0_logo_id, n0_store_id, sz_description, bl_active, n0_msg_id, dt_date_start, dt_date_end, dt_created, sz_created_by, dt_updated, sz_updated_by, dt_deleted, sz_host_id 
--- /****** non cancellare ******/  FROM tesi_promodef.tesi_campaign; /****** non cancellare ******/ 

----/****** non cancellare ******/ SELECT n0_campaign_id, sz_customer_condition_type, sz_customer_condition_code, n0_status_ph, n0_status_pc, dt_created, sz_created_by, dt_updated, sz_updated_by 
----/****** non cancellare ******/ FROM tesi_promodef.campaign_attributes; 

--SELECT n0_id, n0_company_id, n0_logo_id, n0_store_group_id, n0_store_id, n0_campaign_id, sz_description, bl_active, dt_date_start, dt_date_end, n0_msg_id, dt_created, sz_created_by, dt_updated, sz_updated_by, dt_deleted, sz_host_id 
----FROM tesi_promodef.tesi_initiative;
---------------- truncate table tesi_promodef.tesi_initiative;

--SELECT n0_action_id, n0_status_ph, n0_status_pc, sz_grouping_code, dt_created, sz_created_by, dt_updated, sz_updated_by
----FROM tesi_promodef.action_attributes;
------------------ truncate table tesi_promodef.action_attributes;

--SELECT n0_surr_key, n0_initiative_id, sz_apply_on_code, n0_reward_value_tp, n0_price, n0_cost, n0_margin, n0_promo_cost, n0_promo_margin, sz_grouping_code, n0_status_ph, n0_status_pc, dt_created, sz_created_by, dt_updated, sz_updated_by 
-----FROM tesi_promodef.group_def_attributes;
------------------ truncate table tesi_promodef.group_def_attributes;

--SELECT n0_group_id, n0_status_ph, n0_status_pc, dt_created, sz_created_by, dt_updated, sz_updated_by 
------FROM tesi_promodef.group_descr_attributes;
------truncate table tesi_promodef.group_descr_attributes;

--SELECT n0_initiative_id, n0_reward_id, n0_reward_value, tm_happy_hour_start, tm_happy_hour_end, na_days_of_week, n0_trigger_value, n0_trigger_value_to, n0_status_ph, n0_status_pc, dt_created, sz_created_by, dt_updated, sz_updated_by, one_time_front_end, bl_one_time_front_end 
------FROM tesi_promodef.initiative_attributes;
----------------truncate table tesi_promodef.group_descr_attributes;

---SELECT n0_id, n0_company_id, n0_logo_id, n0_store_id, n0_store_group_id, n0_campaign_id, n0_initiative_id, sz_description, n0_priority, n0_islast, sz_host_id, n0_voucher_id, na_days_of_week, bl_active, n0_reward_id, n0_reward_value, sz_trigger_code_type, sz_trigger_code, sz_apply_on, sz_apply_on_code, sz_trigger_type, sz_trigger_operator, n0_trigger_value, n0_trigger_value_to, n0_apply_criterium, sz_customer_condition_type, sz_customer_condition_code, sz_exclude_condition_type, sz_exclude_condition_code, sz_promo_function, dt_date_start, dt_date_end, bl_cumulative, bl_is_retroactive, n0_msg_id, sz_created_by, sz_updated_by, n0_status_id, dt_created_when, dt_updated_when, dt_deleted_when, sz_channel, sz_promotional_code, n0_action_template_id, bl_is_level_promo, tm_happy_hour_start, tm_happy_hour_end 
------FROM tesi_promodef.tesi_action;
----------------truncate table tesi_promodef.tesi_action;


---SELECT n0_id, n0_type, sz_code, n0_qty_trigger, n0_amt_trigger, bl_get_discount_portion, sz_code_desc, n0_surr_key, to_delete, dt_created, sz_created_by, dt_updated, sz_updated_by, dt_deleted 
--------------FROM tesi_promodef.tesi_group_def;
----------------truncate table tesi_promodef.tesi_group_def;


---SELECT n0_id, sz_host_id, pdv, sz_operation_type, sz_table_trace, dt_operation_type, n0_operation_type, sz_created_by 
-------------FROM tesi_promodef.trace_promos_modified;
----------------truncate table tesi_promodef.trace_promos_modified;

truncate table tesi_promodef.tesi_initiative
--SELECT n0_action_id, n0_status_ph, n0_status_pc, sz_grouping_code, dt_created, sz_created_by, dt_updated, sz_updated_by
----FROM tesi_promodef.action_attributes;
truncate table tesi_promodef.action_attributes
--SELECT n0_surr_key, n0_initiative_id, sz_apply_on_code, n0_reward_value_tp, n0_price, n0_cost, n0_margin, n0_promo_cost, n0_promo_margin, sz_grouping_code, n0_status_ph, n0_status_pc, dt_created, sz_created_by, dt_updated, sz_updated_by 
-----FROM tesi_promodef.group_def_attributes;
truncate table tesi_promodef.group_def_attributes
--SELECT n0_group_id, n0_status_ph, n0_status_pc, dt_created, sz_created_by, dt_updated, sz_updated_by 
------FROM tesi_promodef.group_descr_attributes;
truncate table tesi_promodef.group_descr_attributes
--SELECT n0_initiative_id, n0_reward_id, n0_reward_value, tm_happy_hour_start, tm_happy_hour_end, na_days_of_week, n0_trigger_value, n0_trigger_value_to, n0_status_ph, n0_status_pc, dt_created, sz_created_by, dt_updated, sz_updated_by, one_time_front_end, bl_one_time_front_end 
------FROM tesi_promodef.initiative_attributes;
truncate table tesi_promodef.initiative_attributes
--SELECT n0_group_id, n0_status_ph, n0_status_pc, dt_created, sz_created_by, dt_updated, sz_updated_by 
------FROM tesi_promodef.group_descr_attributes;
truncate table tesi_promodef.group_descr_attributes
--SELECT n0_initiative_id, n0_reward_id, n0_reward_value, tm_happy_hour_start, tm_happy_hour_end, na_days_of_week, n0_trigger_value, n0_trigger_value_to, n0_status_ph, n0_status_pc, dt_created, sz_created_by, dt_updated, sz_updated_by, one_time_front_end, bl_one_time_front_end 
------FROM tesi_promodef.initiative_attributes;
truncate table tesi_promodef.group_descr_attributes
---SELECT n0_id, n0_company_id, n0_logo_id, n0_store_id, n0_store_group_id, n0_campaign_id, n0_initiative_id, sz_description, n0_priority, n0_islast, sz_host_id, n0_voucher_id, na_days_of_week, bl_active, n0_reward_id, n0_reward_value, sz_trigger_code_type, sz_trigger_code, sz_apply_on, sz_apply_on_code, sz_trigger_type, sz_trigger_operator, n0_trigger_value, n0_trigger_value_to, n0_apply_criterium, sz_customer_condition_type, sz_customer_condition_code, sz_exclude_condition_type, sz_exclude_condition_code, sz_promo_function, dt_date_start, dt_date_end, bl_cumulative, bl_is_retroactive, n0_msg_id, sz_created_by, sz_updated_by, n0_status_id, dt_created_when, dt_updated_when, dt_deleted_when, sz_channel, sz_promotional_code, n0_action_template_id, bl_is_level_promo, tm_happy_hour_start, tm_happy_hour_end 
------FROM tesi_promodef.tesi_action;
truncate table tesi_promodef.tesi_action
---SELECT n0_id, n0_type, sz_code, n0_qty_trigger, n0_amt_trigger, bl_get_discount_portion, sz_code_desc, n0_surr_key, to_delete, dt_created, sz_created_by, dt_updated, sz_updated_by, dt_deleted 
--------------FROM tesi_promodef.tesi_group_def;
truncate table tesi_promodef.tesi_group_def
---SELECT n0_id, sz_host_id, pdv, sz_operation_type, sz_table_trace, dt_operation_type, n0_operation_type, sz_created_by 
-------------FROM tesi_promodef.trace_promos_modified;
truncate table tesi_promodef.trace_promos_modified
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_control_post_dia2stg(stg_table_name character varying) 
	RETURNS character varying
	LANGUAGE plpgsql
	VOLATILE
as $body$
		
declare
	ret_var record;
	func text;
begin
	-- создать временную таблицу со скриптами проверок ККД
	-- пользовательские и агрегирующие проверки (agg_pct, user_sql) не обрабатываются!
	create temp table test_func(f_name) as (
	with r as (
		select 
			rule_type_cd as r_type, 
			rule_table_name as r_table,
			rule_field_name as r_field,
			rule_date_field_name as r_date_field,
			rule_sql as r_sql,
			rule_name as r_name
		from s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule 
		where rule_table_name = stg_table_name
		and rule_type_cd in ('max_date', 'nonnull_ratio')  --, 'agg_pct', 'user_sql')
		and rule_on = true
	),
	test_func_1 as(
	select 
	case 
		when r_type = 'max_date' then
			'select s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_max_date(current_date, '''||r_table||''', '''||r_field||''', '''||r_name||''', ''day'');'
		when r_type = 'nonnull_ratio' then
			'select s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_nonnull_ratio(null, '''||r_table||''', '''||r_field||''', '''||r_date_field||''', current_date, '''||r_name||''', ''year'', 10);'
--		when r_type = 'agg_pct' then
--			'select s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_aggregate('''||r_table||''', '''||r_date_field||''', '''||r_field||''', '''||r_sql||''', '''||r_name||''', ''day'', 7) into ret_var;'
--		when r_type = 'user_sql' then
--			'select s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_nonnull_ratio(null, '''||r_sql||''', '''||r_date_field||''', '''||r_name||''', null, null, null) into ret_var;'
		end	as func_1
	from r
	)
	select func_1 from test_func_1);
	for func in select f_name from test_func loop
		begin
			execute func;
		exception when others then
			return r_name;
		end;
	end loop;
	drop table test_func;
	return '0';
end; 

$body$
EXECUTE ON ANY;
	
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_control_pre_dia2stg(stg_table_name character varying) 
	RETURNS character varying
	LANGUAGE plpgsql
	VOLATILE
as $body$
	
declare
	ret_var varchar;
	dia_table varchar;
	func text;
	bench_date date; 
	tgt_date date; 	
	i integer;
begin
	drop table if exists r;
	create temp table r as (
		select 
			rule_type_cd as r_type, 
			rule_table_name as r_table,
			rule_field_name as r_field,
			rule_date_field_name as r_date_field,
			rule_name as r_name
		from s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule 
		where rule_table_name = stg_table_name
		and rule_type_cd in ('continuous stability', 'category stability', 'date stability', 'stab_cont', 'stab_categ', 'stab_date')
		and rule_on = true 
	);
	dia_table = overlay((select distinct r.r_table from r) placing 's_grnplm_vd_hr_edp_dia.' from 1 for 26);
	drop table if exists test_func;
	create temp table test_func(fi, f_name) as (
	select 0, 'select max('||r_date_field||')::date from '||r_table||';'
	from r
	union
	select 1, 'select max('||r_date_field||')::date from '||dia_table||';'
	from r
	union
	select 2,
	case 
		when r_type = 'continuous stability' or r_type = 'stab_cont' then
			format('select s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_stab_cont('''||r_table||''', $1, '''||r_field||''', $2, $3, '''||r_name||''', '''||r_date_field||''', null, '''||r_date_field||''', null, ''month'', 1, ''month'', 1);')
		when r_type = 'category stability' or r_type = 'stab_categ' then
			'select s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_stab_categ('''||r_table||''', $1, '''||r_field||''', $2, $3, '''||r_name||''', '''||r_date_field||''', '''||r_date_field||''', ''month'', 1, ''month'', 1);'
		when r_type = 'date stability' or r_type = 'stab_date' then
			'select s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_stab_date('''||r_table||''', $1, '''||r_field||''', $2, $3, '''||r_name||''', '''||r_date_field||''', '''||r_date_field||''', ''month'', 1, ''month'', 1);'
	end	
	from r);
	-- insert into public.func select * from test_func; -- для отладки
	for i, func in select * from test_func order by fi asc loop
		if i=0 then execute func into bench_date;
		elsif i=1 then execute func into tgt_date;
		else
			begin
				execute func using dia_table, bench_date, tgt_date;
			exception when others then
				return func;  -- в случае падения возвращает функцию ККД на которой упал
			end;
		end if;
	end loop;
	drop table r;
	drop table test_func;
	return '0';
end; 

$body$
EXECUTE ON ANY;
	
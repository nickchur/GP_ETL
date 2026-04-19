CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_nonnull_ratio(control_date date, table_name character varying, field_name character varying, date_field_name character varying, date_field_val date, rule_name character varying, date_interval_type character varying, date_interval_qty integer) 
	RETURNS double precision
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
	
	
	
	
	
	
declare
		
	DECLARE v_rule_sql_exp_final VARCHAR(2000);
	DECLARE v_nonnull_ratio float;
	DECLARE v_date_diff FLOAT;
	declare date_to date;
	declare date_from date;

BEGIN	
	--параметры для вызова
	--control_date - дата t (текущая)
	--table_name - таблица, для которой производится проверка
	--field_name - поле, содержащее проверяемую дату
	--rule_name - название правила (задается при реализации потока)
	--date_field_name - название поля, задающего период измерения показателя
	--date_field_val - название поля, задающего правую границу периода измерения показателя (окончание периода)
	--date_interval_type - тип интервала измерения разности между датами
	--date_interval_qty - не используется
	date_to = date_field_val;
	date_from  = s_grnplm_vd_hr_edp_srv_dq.fn_dq_calculate_interval_dt_from(date_field_val,date_interval_type,date_interval_qty);
	
	/*execute  'SELECT cast(COUNT(nullif('|| field_name ||','''')) as float)/nullif(cast(count(*) as int4),0)  as nratio FROM ' ||
					table_name ||' where '||date_field_name||' between $1 and $2' into v_nonnull_ratio using
			 date_from, date_to;*/
	execute  'SELECT cast(COUNT(nullif(cast('|| field_name ||' as varchar),'''')) as float)/nullif(cast(count(*) as int8),0)  as nratio FROM ' ||
					table_name ||' where '||date_field_name||' between $1 and $2' into v_nonnull_ratio using
			 date_from, date_to;
	
	insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_nonnull_ratio values (rule_name, table_name, field_name, date_field_val, v_nonnull_ratio, current_date);
	return v_nonnull_ratio;
	
end; 








$body$
EXECUTE ON ANY;
	
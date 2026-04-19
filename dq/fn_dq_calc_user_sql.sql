CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_user_sql(sql_expression character varying, date_field_name character varying, rule_name character varying, dt_val date, interval_type character varying, interval_qty integer) 
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
	
	
	
	
	
	
	
declare
	v_sql_expression VARCHAR(2000);
	calc_value numeric(20,5);
	v_date_from DATE;
	v_date_to DATE;

begin
			
	v_date_to = dt_val;	
	v_date_from = s_grnplm_vd_hr_edp_srv_dq.fn_dq_calculate_interval_dt_from(dt_val,interval_type,interval_qty);
	
	if dt_val is not null then 
		v_sql_expression = 
			sql_expression || ' WHERE '|| date_field_name  || '  between $1 and $2 ';
		execute v_sql_expression using  v_date_from, v_date_to  into calc_value;
		INSERT INTO s_grnplm_vd_hr_edp_srv_dq.tb_dq_user_sql
			(rule_name, sql_expression, load_dt, calc_value, calc_dt)
				 values (rule_name,sql_expression, cast(dt_val as date),calc_value, now());
	else 
		v_sql_expression = sql_expression;
		execute v_sql_expression into calc_value;
		INSERT INTO s_grnplm_vd_hr_edp_srv_dq.tb_dq_user_sql
			(rule_name, sql_expression, load_dt, calc_value, calc_dt)
				 values (rule_name,sql_expression, current_date,calc_value, now());
	
	end if;
	
	
end; 






$body$
EXECUTE ON ANY;
	
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_user_sql(character varying, character varying, character varying, date, character varying, integer) IS 'Выполняет произвольный SQL-запрос пользователя за указанный интервал и сохраняет результат в DQ-лог';

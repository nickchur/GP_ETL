CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_not_in_range(table_name character varying, field_name character varying, field_type character varying, min_value character varying, max_value character varying, rule_name character varying) 
	RETURNS integer
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
	cnt_not_in_range int;
	sql_expression VARCHAR(2000);
	calc_value numeric(20,5);
BEGIN	
	-- параметры для вызова
	-- table_name - таблица, для которой производится проверка
	-- field_name - проверяемое поле
	-- field_type - тип проверяемого поля (int, date, timestamp)
	-- min_value - нижняя граница диапазона (передается как varchar!)
	-- max_value - верхняя граница диапазона (передается как varchar!)
	-- rule_name - имя правила ККД 
	sql_expression = 'select count(*) from '|| table_name ||' where '|| field_name ||' not between ''' || min_value || '''::'|| field_type || ' and ''' || max_value || '''::'|| field_type || ';';
	execute sql_expression into calc_value;
	insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_user_sql (rule_name, sql_expression, calc_value, calc_dt) 
		values (rule_name, sql_expression, calc_value, now());
	return cnt_not_in_range;
end; 

$body$
EXECUTE ON ANY;
	
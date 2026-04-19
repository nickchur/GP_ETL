CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_max_date(control_date date, table_name character varying, field_name character varying, rule_name character varying, interval_type character varying) 
	RETURNS double precision
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
            
       DECLARE v_rule_sql_exp_final VARCHAR(2000);
       DECLARE v_max_date DATE;
       DECLARE v_date_diff FLOAT;
 
BEGIN
       --параметры для вызова
       --control_date - дата t (текущая)
       --table_name - таблица, для которой производится проверка
       --field_name - поле, содержащее проверяемую дату
       --rule_name - название правила (задается при реализации потока)
       --interval_type - тип интервала измерения разности между датами
       --interval_qty - не используется
       execute  'SELECT MAX(' || field_name || ')  as max_date FROM ' || table_name into v_max_date;
            
       CASE interval_type
             WHEN 'year' THEN
                    v_date_diff = age(control_date, v_max_date) /12;
             WHEN 'quarter' THEN
                    v_date_diff = age(control_date, v_max_date) /4;
             WHEN 'month' THEN
                     v_date_diff = extract(month from control_date)-extract(month from cast(v_max_date as DATE)) ;
             WHEN 'week' THEN
                     v_date_diff = Cast((control_date - v_max_date) AS FLOAT) / 7;
             WHEN 'day' THEN
                    v_date_diff = control_date - v_max_date;
       END CASE;   
       insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_max_date
             values (rule_name, table_name, field_name, control_date, v_date_diff, current_date);
       return v_date_diff;
      
end;
 
 
 
 
 
 
 

$body$
EXECUTE ON ANY;
	
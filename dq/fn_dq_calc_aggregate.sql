CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_aggregate(table_name character varying, date_field_name character varying, field_name character varying, sql_agg_expression character varying, dt_val date, rule_name character varying, interval_type character varying, interval_qty integer) 
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
	
declare
	v_date_from DATE;
	v_date_to DATE;
	v_sql_expression VARCHAR(2000);
	pct numeric(10,5);

begin
			
	v_date_to = dt_val;	
	v_date_from = s_grnplm_vd_hr_edp_srv_dq.fn_dq_calculate_interval_dt_from(dt_val,interval_type,interval_qty);
	
	v_sql_expression = 
		'WITH cte1 AS (SELECT ' || sql_agg_expression || ' AS res FROM ' || 
		 table_name || ' WHERE '|| date_field_name  || '= $1 ), cte2 AS (SELECT ' || sql_agg_expression || ' AS res FROM ' || 
		table_name || ' WHERE '|| date_field_name  || '  = $2 ) ' || 
		'SELECT CAST((coalesce(cte2.res,0) - cte1.res) AS FLOAT) / NULLIF(cte1.res,0) AS pct FROM cte1, cte2';
	execute v_sql_expression using v_date_to,v_date_from  into pct;

	insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_aggregate_comp values (rule_name, table_name, field_name,sql_agg_expression, cast(dt_val as date),pct, now());


	
end; 












$body$
EXECUTE ON ANY;
	
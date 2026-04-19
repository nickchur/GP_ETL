CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_scd2_validity(rule_name character varying, table_name character varying, pk_field_name character varying) 
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
	

DECLARE v_rule_sql_exp_final VARCHAR(2000);
		v_qty int4;
BEGIN
			execute 'SELECT COUNT(*) AS Res FROM (
			SELECT a.rule_name FROM ' || table_name || ' a
			join 
			(select b.*, coalesce(MAX(effective_to) OVER (PARTITION BY rule_name, rule_field_name ORDER BY effective_to  ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),effective_from) mx
			from ' || table_name || ' b) b2
			on a.' || pk_field_name || '=b2.' || pk_field_name || ' 
					 where 1=1
			and		 a.effective_from <> b2.mx and a.effective_from-1 <> b2.mx
			order by 1 desc
			) c' into v_qty;
	
		insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_scd2_validity values (rule_name, table_name, pk_field_name, v_qty, current_date);
END;




$body$
EXECUTE ON ANY;
	
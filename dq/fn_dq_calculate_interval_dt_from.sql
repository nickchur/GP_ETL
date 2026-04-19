CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calculate_interval_dt_from(p_on_dt date, p_rule_interval_type_cd character varying, p_rule_interval_qty integer) 
	RETURNS date
	LANGUAGE plpgsql
	VOLATILE
as $body$

begin
    return case p_rule_interval_type_cd
        when 'year' then s_grnplm_vd_hr_edp_srv_dq.add_month(p_on_dt, p_rule_interval_qty * -12) -- вычесть год (~365 дней)
        when 'quarter' then s_grnplm_vd_hr_edp_srv_dq.add_month(p_on_dt, p_rule_interval_qty * -4)  -- вычесть квартал (4 месяца)
        when 'month' then s_grnplm_vd_hr_edp_srv_dq.add_month(p_on_dt, p_rule_interval_qty * -1)  -- вычесть месяц (~30 дней)
        when 'month_ld' then s_grnplm_vd_hr_edp_srv_dq.last_day(s_grnplm_vd_hr_edp_srv_dq.add_month(p_on_dt, p_rule_interval_qty * -1))  -- вычесть месяц и взять последний день этого месяца
        when 'month_fd' then s_grnplm_vd_hr_edp_srv_dq.first_day(s_grnplm_vd_hr_edp_srv_dq.add_month(p_on_dt, p_rule_interval_qty * -1))  -- вычесть месяц и взять первый день этого месяца
        when 'week' then p_on_dt - p_rule_interval_qty * 7  -- вычесть неделю (7 дней)
        when 'day' then p_on_dt - p_rule_interval_qty  -- вычесть 1 день
    end;
end; 
$body$
EXECUTE ON ANY;
	
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calculate_interval_dt_from(date, character varying, integer) IS 'Вычисляет начальную дату интервала на основе контрольной даты, типа и количества интервалов';

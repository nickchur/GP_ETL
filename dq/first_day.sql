CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.first_day(start_date date) 
	RETURNS date
	LANGUAGE sql
	IMMUTABLE
as $body$

	select date_trunc('month', start_date)::date;

$body$
EXECUTE ON ANY;
	
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_dq.first_day(date) IS 'Возвращает первый день месяца для заданной даты';

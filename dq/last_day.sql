CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.last_day(start_date date) 
	RETURNS date
	LANGUAGE sql
	IMMUTABLE
as $body$

	SELECT (date_trunc('MONTH', start_date) + INTERVAL '1 MONTH - 1 day')::date;

$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_dq.last_day(date) IS 'Возвращает последний день месяца для заданной даты';

CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.last_day(anyelement) 
	RETURNS date
	LANGUAGE sql
	IMMUTABLE
as $body$

SELECT (date_trunc('MONTH', ($1)::date) + INTERVAL '1 MONTH - 1 day')::date;

$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.last_day(anyelement) IS 'Возвращает последний день месяца для заданной даты';

CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.last_day(start_date date) 
	RETURNS date
	LANGUAGE sql
	IMMUTABLE
as $body$

	select (s_grnplm_vd_hr_edp_srv_dq.first_day((start_date + interval '1 month')::date) - interval '1 day')::date;

$body$
EXECUTE ON ANY;
	
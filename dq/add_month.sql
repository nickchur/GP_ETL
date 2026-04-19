CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.add_month(start_date date, month_quantity integer) 
	RETURNS date
	LANGUAGE sql
	IMMUTABLE
as $body$

	select (start_date + (month_quantity || ' months')::interval)::date

$body$
EXECUTE ON ANY;
	
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_dq.add_month(date, integer) IS 'Прибавляет указанное количество месяцев к дате, возвращает результирующую дату';

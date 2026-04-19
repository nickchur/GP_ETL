CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_check_ctl(obj text, sch text, prm json DEFAULT NULL::json) 
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

begin
    return s_grnplm_vd_hr_edp_srv_wf.pr_check_etl(obj, sch, prm);
end;
$body$
EXECUTE ON ANY;
	
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_check_ctl(obj text, sch text, rdt text, ldt text DEFAULT NULL::text, kdt text DEFAULT NULL::text, etype boolean DEFAULT true, expire timestamp without time zone DEFAULT NULL::timestamp without time zone) 
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

begin
    return s_grnplm_vd_hr_edp_srv_wf.pr_check_etl(obj, sch, json_build_object('rdt', rdt, 'ldt', ldt, 'kdt', kdt, 'etype', etype, 'expire', expire));
end;
$body$
EXECUTE ON ANY;
	
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_check_ctl(text, text, json) IS 'Обёртка над pr_check_etl с etype=true по умолчанию для CTL-проверок';
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_check_ctl(text, text, text, text, text, boolean, timestamp without time zone) IS 'Обёртка над pr_check_etl с etype=true по умолчанию для CTL-проверок';

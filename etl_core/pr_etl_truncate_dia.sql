CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_etl_truncate_dia(wf text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

BEGIN
    return s_grnplm_vd_hr_edp_srv_wf.pr_etl_truncate_source(wf);
END;

$body$
EXECUTE ON ANY;
	
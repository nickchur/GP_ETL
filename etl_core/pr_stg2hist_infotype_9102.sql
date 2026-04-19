CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_stg2hist_infotype_9102() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

begin
    return s_grnplm_vd_hr_edp_srv_wf.pr_stg2hist('infotype_9102', 'actual_date', '{load_date}');
end;
$body$
EXECUTE ON ANY;
	
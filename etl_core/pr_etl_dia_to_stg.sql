CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_etl_dia_to_stg(wf text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

BEGIN
    return s_grnplm_vd_hr_edp_srv_wf.pr_etl_source_to_target(wf);
END;

$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_etl_dia_to_stg(text) IS 'Обёртка для переноса данных из DIA в STG через конфигурацию tb_etl_config';

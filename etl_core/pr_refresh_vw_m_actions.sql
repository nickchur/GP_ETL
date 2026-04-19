CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_refresh_vw_m_actions() 
	RETURNS boolean
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
begin 
	refresh materialized view s_grnplm_vd_hr_edp_vda.vw_m_actions;
	--refresh materialized view s_grnplm_vd_hr_edp_vda.vw_m_ref_action;
	return true;
end;


$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_refresh_vw_m_actions() IS 'Обновляет материализованное представление vw_m_actions';

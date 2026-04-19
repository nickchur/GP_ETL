CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_mapping_rebuild() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

begin
    return pr_update_mail_mapping();
end; 
$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_mapping_rebuild() IS 'Перестраивает маппинг почтовых адресов и отправляет уведомление о результате';

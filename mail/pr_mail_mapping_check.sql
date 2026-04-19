CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_mapping_check() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

begin
    return pr_check_mail_mapping();
end; 
$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_mapping_check() IS 'Отправляет письмо с результатами проверки маппинга почтовых адресов';

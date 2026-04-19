CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_super_workflow_tmt() 
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
	
	
	 	 	
		BEGIN
			perform pg_sleep(3);
		
			return;
		END;
	  




$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_super_workflow_tmt() IS 'Завершает зависшие workflow-процессы по таймауту';

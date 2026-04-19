CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_dummy() 
	RETURNS character varying
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
				 	 	
	begin
		RETURN 'dummy';
	end;



$body$
EXECUTE ON ANY;
	
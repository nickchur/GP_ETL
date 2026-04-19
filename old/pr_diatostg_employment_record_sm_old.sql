CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_diatostg_employment_record_sm_old() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
	
 
declare 
    log_id integer;
	m_txt text;
	e_detail text;
	e_hint text;
	e_context text;
	   
begin 
	log_id := s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('SMDTOGP_employment_record (pr_diatostg_employment_record_sm)');	
       
	begin
	    if (select count(*) from s_grnplm_vd_hr_edp_dia.tb_smd_employment_record_sm) = 0 then
	    m_txt = 'No data';
	    perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt, '', '', '') ; --ЛОГИРОВАНИЕ
	    return m_txt;
	    end if;
        
        truncate s_grnplm_vd_hr_edp_stg.tb_employment_record_sm;
        
        insert into s_grnplm_vd_hr_edp_stg.tb_employment_record_sm (
            actual_date,
            report_date,
            i_pernr,
            id_record,
            date_from,
            date_to,
            employer,
            division,
            pos,
            city,
            reason,
            fire_date,
            htseqnr,
            flag_general_experience,
            flag_experience_in_sber,
            flag_continuous_experience,
            flag_experience_in_pos,
            years_01,
            months_01,
            days_01,
            years_03,
            months_03,
            days_03,
            years_06,
            months_06,
            days_06,
            years_07,
            months_07,
            days_07,
            load_date
        )
        select
            hexportid as actual_date,
            hexpperio as report_date,
            employee as i_pernr,
            herbrownm as id_record,
            hdatefrom as date_from,
            hdateto as date_to,
            hemployer as employer,
            hdivision as division,
            hposition as pos,
            hcityou as city,
            hfreason as reason,
            hdatyvol as fire_date,
            htseqnr as htseqnr,
            httls01 as flag_general_experience,
            httls03 as flag_experience_in_sber,
            httls06 as flag_continuous_experience,
            httls07 as flag_experience_in_pos,
            htduy01 as years_01,
            htdum01 as months_01,
            htdud01 as days_01,
            htduy03 as years_03,
            htdum03 as months_03,
            htdud03 as days_03,
            htduy06 as years_06,
            htdum06 as months_06,
            htdud06 as days_06,
            htduy07 as years_07,
            htdum07 as months_07,
            htdud07 as days_07,
            load_date
        from s_grnplm_vd_hr_edp_dia.tb_smd_employment_record_sm;

	    perform s_grnplm_vd_hr_edp_srv_wf.pr_log_skew('s_grnplm_vd_hr_edp_stg.tb_employment_record_sm');
		
		perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id,'s_grnplm_vd_hr_edp_stg.tb_employment_record_sm','report_date','report_date', 'actual_date'); --ЛОГИРОВАНИЕ
	        RETURN 'OK';
	
	    exception when OTHERS then
	        get stacked diagnostics m_txt = MESSAGE_TEXT;
	        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
	     	get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
	      	get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
	        
	        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt,e_detail,e_hint,e_context) ; --ЛОГИРОВАНИЕ
	        return m_txt;
       
	end;
end;





$body$
EXECUTE ON ANY;
	
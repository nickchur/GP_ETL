CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_smdtodia_employment_record_sm_old() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
	
	
 
	declare 
		max_prev_hexportid timestamp := (select max(hexportid) from s_grnplm_vd_hr_edp_dia.tb_smd_employment_record_sm);
	    log_id integer;
		m_txt text;
		e_detail text;
		e_hint text;
		e_context text;
		   
	begin 
		log_id := s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('SMDTOGP_employment_record (pr_smdtodia_employment_record_sm)');

		begin   
			
			if (select 1 from s_grnplm_vd_hr_edp_dia.pxf_smd_employment_record_sm limit 1) is null then
	            m_txt = 'No data (проверь pxf или данные в СМД)';
	            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt, '', '', '') ;
	            return m_txt;
	        end if;
								
		    truncate s_grnplm_vd_hr_edp_dia.tb_smd_employment_record_sm;
		   
		    insert into s_grnplm_vd_hr_edp_dia.tb_smd_employment_record_sm (
		        hexportid,
		        hexpperio,
		        employee,
		        herbrownm,
		        hdatefrom,
		        hdateto,
		        hemployer,
		        hdivision,
		        hposition,
		        hcityou,
		        hfreason,
		        hdatyvol,
		        htseqnr,
		        httls01,
		        httls03,
		        httls06,
		        httls07,
		        htduy01,
		        htdum01,
		        htdud01,
		        htduy03,
		        htdum03,
		        htdud03,
		        htduy06,
		        htdum06,
		        htdud06,
		        htduy07,
		        htdum07,
		        htdud07,
		        load_date
		    )
		    select
		        to_timestamp(nullif(nullif(left(hexportid, 14), ' '), ''), 'yyyymmddhh24miss'),
		        to_date(nullif(nullif(left(hexpperio, 8), ' '), ''), 'yyyymmdd'),
		        nullif(nullif(employee, ' '), '')::integer,
		        nullif(nullif(herbrownm, ' '), '')::integer,
		        to_date(nullif(nullif(left(hdatefrom, 10), ' '), ''), 'yyyy-mm-dd'),
		        to_date(nullif(nullif(left(hdateto, 10), ' '), ''), 'yyyy-mm-dd'),
		        nullif(nullif(hemployer, ''), ' '),
		        nullif(nullif(hdivision, ''), ' '),
		        nullif(nullif(hposition, ''), ' '),
		        nullif(nullif(hcityou, ''), ' '),
		        nullif(nullif(hfreason, ''), ' '),
		        to_date(nullif(nullif(left(hdatyvol, 10), ' '), ''), 'yyyy-mm-dd'),
		        nullif(nullif(htseqnr, ''), ' ')::integer,
		        nullif(nullif(httls01, ''), ' '),
		        nullif(nullif(httls03, ''), ' '),
		        nullif(nullif(httls06, ''), ' '),
		        nullif(nullif(httls07, ''), ' '),
		        nullif(nullif(htduy01, ''), ' ')::integer,
		        nullif(nullif(htdum01, ''), ' ')::integer,
		        nullif(nullif(htdud01, ''), ' ')::integer,
		        nullif(nullif(htduy03, ''), ' ')::integer,
		        nullif(nullif(htdum03, ''), ' ')::integer,
		        nullif(nullif(htdud03, ''), ' ')::integer,
		        nullif(nullif(htduy06, ''), ' ')::integer,
		        nullif(nullif(htdum06, ''), ' ')::integer,
		        nullif(nullif(htdud06, ''), ' ')::integer,
		        nullif(nullif(htduy07, ''), ' ')::integer,
		        nullif(nullif(htdum07, ''), ' ')::integer,
		        nullif(nullif(htdud07, ''), ' ')::integer,
		        current_date as load_date
		    from s_grnplm_vd_hr_edp_dia.pxf_smd_employment_record_sm;
		
			if max_prev_hexportid is not null and (select 1 from s_grnplm_vd_hr_edp_dia.tb_smd_employment_record_sm where hexportid > max_prev_hexportid limit 1) is null then
	            m_txt = 'No new actual_date';
	            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt, '', '', '') ;
	            return m_txt;
	        end if;
			      
			perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id,'s_grnplm_vd_hr_edp_dia.tb_smd_employment_record_sm',null,'hexpperio', 'hexportid'); --ЛОГИРОВАНИЕ
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
	
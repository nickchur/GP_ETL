CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_event(wf text DEFAULT ''::text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    log_id int;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_Log_start('EVENT_'||wf);
    begin
        log_id = pr_Log_end(log_id);
        return 'OK'; 
    
    exception when OTHERS then
        declare 
            e_txt text;
            e_detail text;
            e_hint text;
            e_context text;
        begin    
            get stacked diagnostics e_txt = MESSAGE_TEXT;
            get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
            get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
            get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
            
            log_id =  pr_Log_error(log_id, e_txt, e_detail, e_hint, e_context); 
            return 'Error: '||e_txt;
       end;
   end;
end; 
$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_event(text) IS 'Регистрирует разовое событие в лог workflow (start + end), возвращает статус OK или Error';

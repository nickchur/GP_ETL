CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_work(_log_id integer, _action text, _msg text, _res text DEFAULT NULL::text) 
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    _app text;
begin
    execute 'show application_name' into _app;
    raise info '% % % % % %', clock_timestamp(), _app, _log_id, _action, _msg, _res;
    INSERT INTO s_grnplm_vd_hr_edp_srv_wf.tb_log_work values (clock_timestamp(), _app, _log_id, _action, _msg, _res);  

    return; 

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

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(0, e_txt, e_detail, e_hint, e_context); 
        return;
    end;
end; 
$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_work(integer, text, text, text) IS 'Записывает отладочное событие в tb_log_work с временной меткой и именем приложения';

CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_informatica(v_message text, v_action text DEFAULT 'log_inf'::text, log_id integer DEFAULT NULL::integer) 
	RETURNS integer
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    sql text;
    id int4;
begin
    sql = format('set application_name = %L', v_message);
    execute sql;
    
    set lock_timeout = 100000;
    id = s_grnplm_vd_hr_edp_srv_wf.pr_log_action(v_action, v_message, log_id);
    set lock_timeout = 0;

    return id;

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
    
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id, e_txt, e_detail, e_hint, e_context);
        set lock_timeout = 0;
        return 0;
    end;
end; 
$body$
EXECUTE ON ANY;
	
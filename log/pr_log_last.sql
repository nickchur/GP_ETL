CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_last(v_workflow text, v_action text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

begin

    return (
        select distinct on (workflow) message msg
        from s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow
        where true
            and workflow = v_workflow
            and end_action = v_action 
        order by workflow, start_id desc
    );
        
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
        return null;
    end;
end; 
$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_last(text, text) IS 'Возвращает последнее сообщение workflow с указанным действием завершения';

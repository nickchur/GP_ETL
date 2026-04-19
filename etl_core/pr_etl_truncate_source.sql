CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_etl_truncate_source(wf text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    log_id int;
    m_txt text;
    e_detail text;
    e_hint text;
    e_context text;
    etl record;
    rw int8;
BEGIN
    SET search_path = s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_Log_start(wf);   --ЛОГИРОВАНИЕ
    begin
        etl =  pr_etl_get_config(wf);
        if etl.workflow is null then
            return wf||' not found';
        end if;
        -- raise info '%',etl;

        if (etl.source_tuncate is true) then
            execute 'TRUNCATE '||etl.source_table;
            RETURN 'Ok '||etl.source_table||' is now empty';
        else
            RETURN 'Ok';
        end if;

    exception when OTHERS then
        get stacked diagnostics m_txt = MESSAGE_TEXT;
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
        get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
        get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt,e_detail,e_hint,e_context) ; --ЛОГИРОВАНИЕ
        return m_txt;
    end;
END;

$body$
EXECUTE ON ANY;
	
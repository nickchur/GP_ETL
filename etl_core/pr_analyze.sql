CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_analyze(tb_name text, fld_names text DEFAULT NULL::text) 
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

declare
    tt timestamp := clock_timestamp();
begin
    execute('analyze '||tb_name||coalesce(' ('||fld_names||')', ''));
    return format('Ok analyze %s', clock_timestamp() - tt);
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

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(null, e_txt, e_detail, e_hint, e_context);
        return 'Error: '||e_txt;
    end;
end; 
$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_analyze(text, text) IS 'Выполняет ANALYZE для таблицы или указанных колонок, возвращает время выполнения';

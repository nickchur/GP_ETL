CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_skew(tb_name text) 
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

declare
    tb_size int8;
    txt text;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    
    tb_size = pr_table_size(tb_name);
    
    if coalesce(tb_size, 0) = 0 then
        return 'Error: not a table';
    end if;

    txt = pr_analyze(tb_name);
    
    return concat(txt, '\n', pr_log_skew_hist(tb_name, tb_size));

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

        perform pr_Log_error(0, e_txt, e_detail, e_hint, e_context);
        return e_txt;
    end;
end;
$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_skew(text) IS 'Запускает ANALYZE таблицы и сохраняет гистограмму перекоса данных по сегментам в tb_log_skew';

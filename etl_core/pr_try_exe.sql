CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_try_exe(exe text, sleep integer DEFAULT 60, retry integer DEFAULT 1) 
	RETURNS bigint
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    rc int8;
    e_code text;
    e_txt text;
    e_hint text;
    e_det text;
    app text;
begin 
    execute 'show application_name' into app;
    for k in 1..retry loop
        execute format('set application_name = %L', format('%s (%s)', app, k));
        
        begin
            raise info '% %', k, exe;
            execute exe ;
            get diagnostics rc = ROW_COUNT;
            return rc;

        exception when OTHERS then
            e_code = sqlstate;
            get stacked diagnostics e_txt = MESSAGE_TEXT, e_hint = PG_EXCEPTION_HINT;
            get stacked diagnostics e_det = PG_EXCEPTION_DETAIL;
            get stacked diagnostics e_det = COLUMN_NAME;
            raise info '% %', e_code, e_txt;
            
            if k < retry then
                perform pg_sleep(sleep * k);
            end if;
        end;
    
    end loop;
    execute format('set application_name = %L', app);
    raise exception using ERRCODE = e_code, MESSAGE = format('%s (%s)', e_txt, e_det), DETAIL = exe, HINT = e_hint;
end; 
$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_try_exe(text, integer, integer) IS 'Выполняет SQL-запрос с повторными попытками при ошибке с задержкой sleep секунд';

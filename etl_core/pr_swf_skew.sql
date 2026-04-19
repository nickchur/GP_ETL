CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_skew(lschema text, ltable text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    rec record;
    txt text;
--  lschema text default 's_grnplm_vd_hr_edp_%';
--  ltable text default '%';
    k int default 0;
    s int default 0;
    b int default 0;
begin
    perform pg_sleep(5);

    for rec in(
            select c.relstorage, a.* from pg_stat_all_tables a 
            inner join pg_tables p on p.schemaname = a.schemaname and a.relname = p.tablename and p.tableowner=current_user 
            left join pg_class c on format('%I.%I', p.schemaname, p.tablename)::regclass::oid=c.oid and c.relstorage::text not in('x')
            where a.schemaname like lschema and a.relname like ltable
                and a.relname not like '%_1_prt_%'
                and (a.last_analyze < now()::date - '3 day'::interval or a.last_analyze is null) 
                and lower(left(a.relname,3)) not in('pxf','ext')
                -- and a.relname not in (select distinct substring(relname from '(\w+)_\d_prt') from pg_stat_all_tables a where a.schemaname like lschema and a.relname similar to '%_\d_prt%')
            order by a.last_analyze
        ) loop

        k = k + 1; 
        -- txt = rec.schemaname||'.'||rec.relname;
        txt = format('%I.%I', rec.schemaname, rec.relname);
        raise info '% %', rec, txt;
        if s_grnplm_vd_hr_edp_srv_wf.try_cast2regclass(txt) is not null
            and clock_timestamp()-now() < '4 hour'::interval then
                begin 
                    set lock_timeout = 1000;
                    txt = s_grnplm_vd_hr_edp_srv_wf.pr_log_skew(txt::text);
                    s = s + 1;
                    set lock_timeout = 0;
                exception when lock_not_available then
                    set lock_timeout = 0;
                    b = b + 1;
                    txt = 'BUSY (lock). '||txt;
                end;
        else 
            txt = 'skip '||txt;
        end if;
        raise info '% (%)/% %' , s, b, k, txt;
    end loop;
    return 'OK '||s||' ('||b||')/'||k;
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
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_skew(text, text) IS 'Записывает метрики перекоса таблицы в лог SWF';

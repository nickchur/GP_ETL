CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_soft_truncate(srs text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    exe text;
begin 
    begin 
        exe = format($exe$
            lock table %1$s in ACCESS EXCLUSIVE mode NOWAIT;
            truncate %1$s;
        $exe$, srs);
        -- select pg_sleep(1);
        execute exe;
        return 'truncate';
    exception when lock_not_available then
        exe = format($exe$
            delete from %s where 1=1;
        $exe$, srs);
        execute exe;
        return 'delete';
    end;
exception 
    -- when syntax_error_or_access_rule_violation then
    --     raise exception using ERRCODE = sqlstate, MESSAGE = sqlerrm, DETAIL = exe, HINT = sqlstate;
    when OTHERS then
        get stacked diagnostics exe = PG_EXCEPTION_CONTEXT;
        raise exception using ERRCODE = sqlstate, MESSAGE = concat(sqlerrm,' (',sqlstate,')')
            , DETAIL = split_part(exe, 'statement\n', 1) --, HINT = pr_get_func(exe)
        ;
end;

$body$
EXECUTE ON ANY;
	
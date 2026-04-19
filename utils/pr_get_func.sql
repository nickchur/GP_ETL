CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_get_func(fn text, ln integer DEFAULT NULL::integer) 
	RETURNS text
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

declare
    exe text;
    src text;
    fid oid;
begin 
    if fn ~ 'PL/pgSQL' then
        -- ln = coalesce(ln, (select substring(split_part(fn, 'statement\n', 1) from ' line (\d+) at ')::int));
        -- fn = (select substring(split_part(fn, 'statement\n', 1) from 'PL/pgSQL function *?(.+) line '));
        -- ln = coalesce(ln, (select substring(split_part(fn, 'PL/pgSQL ', 2) from ' line (\d+) at ')::int));
        -- fn = (select substring(split_part(fn, 'PL/pgSQL ', 2) from ' ??function (.+) line '));
        ln = coalesce(ln, (select substring(fn from ' line (\d+) at ')::int));
        fn = (select substring(fn from 'function ??(.+) line '));
    end if;
    
    begin
        fid = fn::regproc::oid;
    exception when OTHERS then
        fid = null;
    end;

    src = (
        select a.src from (
            select a.oid, b.nspname sch, proname func
            , (select concat('(', string_agg(t::oid::regtype::text, ',' order by n), ')') 
                from unnest(string_to_array(proargtypes::text, ' ')) with ordinality b(t,n)) as param
            , a.prosrc src
            from pg_proc a 
            join pg_namespace b on b.oid = a.pronamespace::oid
            where b.nspname like 's_grnplm_vd_hr_edp_%'
        ) a
        where concat(a.sch, '.', a.func, a.param) = fn
            or concat(a.func, a.param) = fn
            or a.oid = fid
            -- or a.func = fn
        order by param desc
        limit 1
    );
    
    if ln is null then
        return (select string_agg(concat(right('    '||n,4), ' ', str), chr(10) order by n) from regexp_split_to_table(src, '\n') with ordinality code(str,n));
    else 
        return (select concat(fn, ' (', ln, ') ', coalesce(split_part(src, chr(10), ln), '')));
    end if;
exception when OTHERS then
    get stacked diagnostics exe = PG_EXCEPTION_CONTEXT;
    raise exception using ERRCODE = sqlstate, MESSAGE = concat(sqlerrm,' (',sqlstate,')')
        , DETAIL = coalesce(split_part(exe, 'statement\n', 1),'');
end;

$body$
EXECUTE ON ANY;
	
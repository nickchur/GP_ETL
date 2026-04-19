CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_wf_group_replace(new_wf text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    rc int8;
    ret text;
    old_wf text[];
    rel_wf text[];
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf, information_schema, public;
    new_wf := substring(new_wf from 'pr\_(\w+)');
    old_wf := (select substring(pg_get_functiondef(('s_grnplm_vd_hr_edp_srv_wf.pr_'||new_wf||'()')::regprocedure) from $ss$.+pr_swf_wf_group\(.*\'({.+})\'.*,$ss$)::text[]);  --'
    rel_wf := (select substring(pg_get_functiondef(('s_grnplm_vd_hr_edp_srv_wf.pr_'||new_wf||'()')::regprocedure) from $ss$.+pr_swf_wf_group\(.*,.*\'({.+})\'$ss$)::text[]);  --'

    perform ('s_grnplm_vd_hr_edp_srv_wf.pr_'||unnest||'()')::regprocedure from unnest(old_wf);

    if array_length(old_wf,1)<>array_length(rel_wf,1) then
        return 'Error: length old_wf '||array_length(old_wf,1)::text||' <> rel_wf '||array_length(rel_wf,1)::text;
    end if;

    
    insert into tb_swf_hist
    select now(), * from tb_swf;

    ret = 'Ok '||new_wf||' '||old_wf::text||'['||array_length(old_wf,1)::text||']';

--    insert into tb_swf (wf_name, wf_exec, wf_beg, wf_interval, wf_expire, wf_last)
--    select new_wf wf_name
--    , 'pr_'||new_wf||'()' wf_exec
--    , min(wf_last::date)  + 1 + min(wf_beg::time) wf_beg
--    , min(wf_interval) wf_interval                           
--    , min(wf_expire) wf_expire
--    , max(wf_last) wf_last
--    from tb_swf a
--    where wf_name in (select unnest(old_wf))
--    group by 1;

    with wait as (
        select wf_name, unnest(wf_waits) wf_wait, wf_waits
        from tb_swf a
    ), rltn as (
        select wf_name, unnest(wf_relations) wf_relation, wf_relations
        from tb_swf a
    )
    insert into tb_swf (wf_name, wf_exec, wf_beg, wf_interval, wf_expire, wf_last, wf_waits)
    select new_wf wf_name
    , 'pr_'||new_wf||'()' wf_exec
    , min(a.wf_last::date)  + 1 + min(a.wf_beg::time) wf_beg
    , min(a.wf_interval) wf_interval                           
    , min(a.wf_expire) wf_expire
    , max(a.wf_last) wf_last
    , nullif(array_agg(distinct b.wf_wait), '{NULL}'::text[]) new_waits
    from tb_swf a 
    left join (
        select * from wait
        where wf_name in (select unnest(old_wf) )
            and wf_wait not in (select unnest(old_wf) )
        union all
        select * from rltn
        where wf_name in (select unnest(old_wf) )
            and wf_relation not in (select unnest(old_wf) )
    ) b on true
    where a.wf_name in (select unnest(old_wf))
    group by 1;

    get diagnostics rc = ROW_COUNT;
    ret = ret||' add +'||rc;
                                

    update tb_swf
    set wf_end = now()
    where wf_name in (select unnest(old_wf));

    get diagnostics rc = ROW_COUNT;
    ret = ret||' Old -'||rc;
                                
    with old as(
        select unnest(old_wf) f
    ), wait as (
        select wf_name, unnest(wf_waits) wf_wait, wf_waits
        from tb_swf a
    ), rltn as (
        select wf_name, unnest(wf_relations) wf_relation, wf_relations
        from tb_swf a
    ), task as (
        select distinct wf_name 
        from wait
        where wf_name not in (select unnest(old_wf))
            and wf_wait in (select unnest(old_wf))
        union 
        select distinct wf_name 
        from rltn
        where wf_name not in (select unnest(old_wf))
            and wf_relation in (select unnest(old_wf))
    )
    update tb_swf
    set wf_waits = a.new_waits, wf_relations = a.new_relations
    from (
        select wf_name wf
            , nullif(array_agg(distinct wait), '{NULL}'::text[]) new_waits
            , max(wf_waits) old_waits
            , nullif(array_agg(distinct relation), '{NULL}'::text[]) new_relations
            , max(wf_relations) old_relations
        from (                            
            select t.wf_name
            , case when fw.f is not null then new_wf else w.wf_wait end wait, wf_waits
            , case when fr.f is not null then new_wf else r.wf_relation end relation, wf_relations
            from task t
            left join wait w on w.wf_name = t.wf_name
            left join old fw on fw.f = w.wf_wait
            left join rltn r on r.wf_name = t.wf_name
            left join old fr on fr.f = r.wf_relation
        ) a
        group by 1
    ) a
    where wf_name = a.wf;

    get diagnostics rc = ROW_COUNT;
    ret = ret||' rel -'||rc;
    return ret;

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

        perform pr_Log_error(null, e_txt,e_detail,e_hint,e_context) ;
        return 'Error: '||e_txt;
    end;

end; 
$body$
EXECUTE ON ANY;
	
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_get_last_pxf_key(log_id integer, dia text, tbl text, fld text DEFAULT 'ctl_loading'::text) 
	RETURNS bigint
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    lid int8;
    exe text;
    log record;
--    pxf record;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    exe = format(' lock table s_grnplm_vd_hr_edp_%s in ACCESS EXCLUSIVE mode ', dia);
    raise info 'LCK %', exe;
    execute exe;

    select a.start_id, a.start_ts
--        , a.start_action
        , a.workflow
--        , a.end_id, a.end_ts
        , a.end_action
--        , a.duration
        , (a.end_action = 'error' and a.message like 'Ztest error%') ztest_err
        , a.message
--        , try_cast('json',b.detail)::json ztest
    into log
    from vw_log_workflow a
    left join tb_log_workflow_err b on a.start_id = b.log_id
    where coalesce(try_cast('json', b.detail)::json->>'object', a.message) =  's_grnplm_vd_hr_edp_'||tbl
        and end_action = 'error'
        and a.message like 'Ztest error%'
        -- and a.start_ts < '2024-08-03'
    order by 1 desc
    limit 1
    ;

    raise info 'LOG %', log;

    if log.ztest_err then
--        pxf = ( select * from tb_swf_pxf_log where wf_action = tbl order by id desc limit 1 ); 
--        lid = (pxf.wf_message->>'lid')::int8 + 1;
        exe = format(' select coalesce( min(%s), 0) from s_grnplm_vd_hr_edp_%s limit 1 ', fld, dia);
        raise info 'DIA %', exe;
        execute exe into lid;
        raise info 'DIA %', lid;
    end if;
    
    if lid is null then    
        exe = format(' select coalesce( max(%s), 0) from s_grnplm_vd_hr_edp_%s limit 1 ', fld, tbl);
        raise info 'STG %', exe;
        execute exe into lid;
        raise info 'STG %', lid;
    end if;

    perform pr_swf_log_action(tbl, 'pxf', json_build_object(
        'lid', lid,
        'fld', fld,
        -- 'oper', oper,
        'log_id', log_id,
--        'pxf', pxf, 
        'log', log
    ));

    return lid;

--exception when OTHERS then
--    raise info 'Error %', sqlerrm;
--    return null::int8;
end;
$body$
EXECUTE ON ANY;
	
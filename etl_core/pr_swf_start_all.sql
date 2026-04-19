CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_all(lwf text DEFAULT NULL::text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    m_jsn json default null;
    swf_rec record;
    log_id int4;
    sql text;
    swf text;
    e_txt text;
    e_detail text;
    e_hint text;
    e_context text;
    reselt int;
    act_rec record;
    act_res bool;
    txt text;
    chk_ts record;
    -- wait_int interval default '1 minute'::interval;
    wait_int interval default '45 seconds'::interval;
    nn int;
    mn int;
    app text;
    sub text;
begin --transaction isolation level repeatable read;
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    app = 'swf_start_'||coalesce(lwf,'');
    sql = format('set application_name = %L', app);
    execute sql;

    -- for act_rec in (select  (now()-xact_start) duration, * from  pg_stat_activity 
    --                 where lower(query) like '% s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_all(%' 
    --                     and usename = user and state = 'active' order by 1) loop

    --     if (now() - act_rec.xact_start) >= '6 hours'::interval then
    --         log_id = pr_swf_log_action(now()::timestamp::text, null, json_build_object('xact', act_rec.xact_start, 'pid', act_rec.pid, 'state', act_rec.state, 'lwf', lwf));
    --         act_res = pg_cancel_backend(act_rec.pid);
    --         log_id = pr_swf_log_action('cancel', null, json_build_object('reselt', act_res::int, 'duration', (now() - act_rec.xact_start),'msg', act_rec.query), log_id);
    --     end if;
    -- end loop;

    mn = (select count(1) from tb_swf_status);
    nn = coalesce(substring(lwf from '(\d+)')::int, 0);

    -- sub = 'wait';
    -- execute format('set application_name = %L', app||'/'||sub);
    perform pg_sleep(mn);
    perform pg_sleep(mn - extract(second from clock_timestamp())::int%mn + nn);

    for swf_rec in (select *
                    , max(last_end) over() max_end 
                    , max(check_end) over() max_chk_end 
                    , (min((chk or (last_end < now() - '5 min'::interval and check_end < now() - '5 min'::interval))::int) over())::bool min_chk
                    --, min((last_end - check_end)) over() min_int
                    --, max((last_end - check_end)) over() max_int
                    from vw_swf_status order by rn_td) loop
        raise info '%', swf_rec;

        m_jsn = json_build_object('swf', swf_rec.swf_name, 'ready', swf_rec.ready, 'td', swf_rec.rn_td --, 'rn', swf_rec.next_rn
                                , 'wf', swf_rec.next_name, 'lwf', lwf, 'chk', swf_rec.min_chk);
        log_id = pr_swf_log_action(now()::timestamp::text, null, m_jsn);

        if (swf_rec.min_chk is false) and ( (now() - swf_rec.max_end) < wait_int or (now() - swf_rec.max_chk_end) < wait_int) then 
        -- if (swf_rec.min_chk is false) and (clock_timestamp() - swf_rec.max_end) between -wait_int and wait_int then 
            sub = 'wait_chk';
            execute format('set application_name = %L', app||'/'||sub);
            -- perform pg_sleep(extract(epoch from (wait_int - (clock_timestamp() - swf_rec.max_end))));
            -- perform pg_sleep(mn);
            perform pg_sleep(mn - extract(second from clock_timestamp())::int%mn + nn);
            m_jsn = json_build_object('reselt', 0,'swf', swf_rec.swf_name, 'wf', swf_rec.next_name, 'msg', 'WAIT (chk)', 'end', swf_rec.max_end, 'chk_end', swf_rec.max_chk_end);
            log_id = pr_swf_log_action('wait',null, m_jsn, log_id);
            return m_jsn::text;
        end if;

        if swf_rec.next_name is null then 
            sub = 'nothing';
            execute format('set application_name = %L', app||'/'||sub);
            -- perform pg_sleep(mn);
            perform pg_sleep(mn - extract(second from clock_timestamp())::int%mn + nn);
            m_jsn = json_build_object('reselt', 0, 'swf', swf_rec.swf_name, 'td', swf_rec.rn_td, 'msg', 'Nothing to do');
            log_id = pr_swf_log_action('stop',null, m_jsn, log_id);
            return m_jsn::text;
        end if;

       sub = 'lock_swf';
       execute format('set application_name = %L', app||'/'||sub);
       begin 
            set lock_timeout = 10;
            execute format('select * from tb_swf_%s_log where id=0', lower(swf_rec.swf_name)) into chk_ts;
            -- execute format('update tb_swf_%s_log set ts = now() where id=0', lower(swf_rec.swf_name));
            execute format('truncate tb_swf_%s_log', lower(swf_rec.swf_name));
            execute format('insert into tb_swf_%s_log (id, ts, wf_action) values(0, clock_timestamp(), now())', lower(swf_rec.swf_name));
            set lock_timeout = 0;

        exception when lock_not_available then
            set lock_timeout = 0;
            m_jsn = json_build_object('reselt', 0,'swf', swf_rec.swf_name, 'wf', swf_rec.next_name, 'msg', 'SWF BUSY (lock)');
            log_id = pr_swf_log_action('skip',null, m_jsn, log_id);
            continue;
        when OTHERS then
            set lock_timeout = 0;
            get stacked diagnostics e_txt = MESSAGE_TEXT;
            m_jsn = json_build_object('reselt', 0,'swf', swf_rec.swf_name, 'wf', swf_rec.next_name, 'msg', e_txt);
            log_id = pr_swf_log_action('error',null, m_jsn, log_id);
            return m_jsn::text;
        end;

        if true and (chk_ts.wf_action::timestamp <> swf_rec.last_beg)
                and ((now() - swf_rec.last_beg) < wait_int or (now() - chk_ts.ts) < wait_int) then 
            sub = 'wait_log';
            execute format('set application_name = %L', app||'/'||sub);
            -- perform pg_sleep(extract(epoch from (wait_int -(now() - swf_rec.max_end))));
            -- perform pg_sleep(extract(epoch from (wait_int -(now() - chk_ts.ts))));
            -- perform pg_sleep(mn);
            perform pg_sleep(mn - extract(second from clock_timestamp())::int%mn + nn);
            
            m_jsn = json_build_object('reselt', 0,'swf', swf_rec.swf_name, 'wf', swf_rec.next_name, 'msg', 'WAIT (log)', 'last', swf_rec.last_beg, 'ts', chk_ts.ts);
            -- m_jsn = json_build_object('reselt', 0,'swf', swf_rec.swf_name, 'wf', swf_rec.next_name, 'msg', 'WAIT (log)');
                                      -- , 'dif', (swf_rec.last_beg - chk_ts.wf_action::timestamp), 'res', swf_rec.last_message->>'reselt');
            log_id = pr_swf_log_action('wait',null, m_jsn, log_id);
            execute format('update tb_swf_%s_log set ts = %s, wf_action = %s where id=0'
                    , lower(swf_rec.swf_name), quote_nullable(chk_ts.ts::text), quote_nullable(chk_ts.wf_action::text));
            return m_jsn::text;
        end if;

        -- insert into s_grnplm_vd_hr_edp_srv_wf.tb_swf_log_wf select now(), *  from s_grnplm_vd_hr_edp_srv_wf.vw_swf; 

        -- START
        sub = 'start';
        execute format('set application_name = %L', app||'/'||sub);
        m_jsn = pr_swf_start(swf_rec.swf_name, swf_rec.next_name)::json;     
        m_jsn = (select json_object_agg(key, value)
                    from (
                        select * from json_each(m_jsn) 
                        union all
                        select * from json_each(json_build_object('chk', (chk_ts.wf_action::timestamp = swf_rec.last_beg) ))
                        ) a );
                          -- , 'dif', (swf_rec.last_beg - chk_ts.wf_action::timestamp), 'res', swf_rec.last_message->>'reselt'))
        log_id = pr_swf_log_action('do',null, m_jsn, log_id);
        raise info '%', m_jsn;

        -- reselt = (m_jsn->>'reselt')::int;
        -- if  reselt = -10 then
        -- end if;

        execute format('update tb_swf_%s_log set wf_action = now(), ts = clock_timestamp() where id=0', lower(swf_rec.swf_name));
        update s_grnplm_vd_hr_edp_srv_wf.tb_swf_status set last_beg = now(), last_end = clock_timestamp(), last_message = m_jsn where swf_name = swf_rec.swf_name;
        return m_jsn::text;
    end loop;
    m_jsn = json_build_object('reselt', 0, 'msg', 'All SWF are busy');
    return m_jsn::text;

exception when OTHERS then
    get stacked diagnostics e_txt = MESSAGE_TEXT;
    get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
    get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
    get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

    m_jsn = json_build_object('reselt', -3, 'swf', 'All', 'msg', translate(e_txt,'"',''''));
    raise log '%', m_jsn;

    perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(0, e_txt, e_detail, e_hint, e_context) ; 
    return m_jsn::text;
end; 
$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_all(text) IS 'Запускает все активные super-workflow или конкретный по маске имени';

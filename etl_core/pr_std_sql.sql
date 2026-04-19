CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_std_sql(action text, prm text DEFAULT ''::text, whr text DEFAULT 'true'::text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    sql text;
begin
    prm = lower(prm);
    
    sql = (select 
    case lower(action)
    when '#sdf' then
        $_$
        select n.nspname ||'.'|| c.relname as name,  pg_get_userbyid(c.relowner) owner 
        from pg_class c join pg_namespace n on c.relnamespace=n.oid 
        where c.relname like '%_hr_edp_sdf_view' 
        -- where pg_get_userbyid(c.relowner) ~ 'sdf'
            and c.relname ~ '%1$s' and %2$s order by 1
        $_$
    when '#sdf2' then
        $_$
        select a.*
            --, c.reloptions, c.relstorage --, to_char(c.reltuples, 'FM999,999,999,999,999,999') reltuples, c.relpages
            --, pg_get_table_distributedby(format('%I.%I', schemaname, tablename)::regclass::oid) distributedby
            --, pg_get_partition_def(format('%I.%I', schemaname, tablename)::regclass::oid) partition_def
            --, pg_size_pretty(s_grnplm_vd_hr_edp_srv_wf.pr_table_size(table_schema||'.'||table_name)) table_size
        from pg_views a
        --join pg_class c on format('%I.%I', schemaname, tablename)::regclass::oid=c.oid
        where viewowner ~ 'hr_edp_sdf'
            -- and schemaname like '%_hr_edp_%' and tablename not like '%_1_prt_%' 
            -- and lower(viewname) ~ lower('%1$s') and %2$s 
        order by schemaname, viewname
        $_$
    when '#temp' then
        $_$
            select n.nspname ||'.'|| c.relname as name 
            from pg_class c join pg_namespace n on c.relnamespace=n.oid 
            where c.relname like 'pg_temp%' and c.relname ~ '%1$s' and %2$s order by 1
        $_$
    when '#busy' then
        $_$
        select count(distinct datid) datids, count(distinct pid) pids, count(distinct sess_id) sess_ids, count(distinct usesysid) usesysids, count(1) from pg_stat_activity
        $_$
    when '#cancel' then
        $_$
        select left(query_start::text,16) ts, left((now()-query_start)::text,8) dif, pid, state, left(query,200) sql --, pg_cancel_backend(pid) cnl, pg_sleep(1) 
        from pg_stat_activity 
        where state <> 'idle' 
            and query not like 'select s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_all(%' and xact_start < now() - '15 minutes'::interval
            and %2$s 
        order by ts
        $_$
    when '#activity' then
        $_$
        select left(query_start::text,16) ts
            --, left(xact_start::text,16) tr, usename
            , left((clock_timestamp()-query_start)::text,8) dur
            , pid, state
            , coalesce(nullif(application_name,''),'...') app_name
            , coalesce(nullif(waiting_reason,''),'.') waiting
            , left(query,300) sql
            , sess_id
            , (clock_timestamp()-backend_start)::interval(0)::text start_dur
        from pg_stat_activity a
        where query_start is not Null and state <> 'idle' 
            and query ~ '%1$s' and %2$s
        order by query_start,xact_start
        $_$
        -- group by query_start,xact_start,usename,state,query
    when '#activity all' then
        $_$
        select sess_id 
            , query_start::timestamp(0)::text ts
            , xact_start::timestamp(0)::text tr
            --, usename
            , left((clock_timestamp()-query_start)::text,8) dur
            , pid, state
            , coalesce(nullif(application_name,''),'...') app_name
            , coalesce(nullif(waiting_reason,''),'.') waiting
            , left(query,300) sql
            --, sess_id
        from pg_stat_activity a
        where usename = current_user
            -- and query_start is not Null and state <> 'idle' 
            and query ~ '%1$s' and %2$s
        order by sess_id,query_start,xact_start
        $_$
        -- group by query_start,xact_start,usename,state,query
    when '#ctl' then
        $_$
            select ts, id, obj, url
--                , (select string_agg(j, ', ') keys from json_object_keys((msg)::json) j) keys
                , length(msg::text) len
                , msg
            from s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl
            where url ~ '%1$s' and %2$s
            order by ts desc
        $_$
    when '#rep' then
        $_$
            select * from (
                select a.id
                    , left(a.ts::text, 16) ts
                    , nullif(a.parent, 0) parent
                    , a.wf_action
                    , row_number() over(partition by coalesce(a.parent, a.id) order by a.ts) rn
                    , nullif(a.wf_message->>'html', '') is not Null is_html
--                    , left((b.ts - a.ts)::text, 9) as end
                    , (b.ts is not null) as is_end
                    , (c.ts is not null) as is_send
                    , left((c.ts - a.ts)::text, 8) as send
                    , to_char(length(a.wf_message->>'html')::int4, 'FM999,999,999,999,999,999') len
                    , left(translate(a.wf_message::text, '<>', '[]'), 500) html 
                from tb_swf_mail_log a
                left join tb_swf_mail_log b on coalesce(a.parent, a.id) = b.parent and  b.wf_action = 'end'
                left join tb_swf_mail_log c on coalesce(a.parent, a.id) = c.parent and  c.wf_action = 'send'
                where  lower(a.wf_action) ~ lower('%1$s')
            ) a
            where %2$s
--                and a.end is not null 
--                and a.parent is null -- head
--                and send is null 
            order by coalesce(a.parent, a.id) desc, rn desc
        $_$
    when '#work' then
        $_$
        select a.rn, a.is_swf, a.ts, a.tr, a.dur, a.pid, a.state, b.next, b.ready, b.start, b.wf, b.duration
            , b.wait wait_swf, b.duration::interval - a.dur::interval wait_gp, sess_id, a.sql, b.swf_name, a.application_name
        from (
            select row_number() over(partition by (query like 'select s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_all(%') order by xact_start) as rn
                , (query like 'select s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_all(%') is_swf 
                , left(query_start::text,16) ts, left(xact_start::text,16) tr, left((clock_timestamp()-query_start)::text,8) dur, pid, state, left(query,200) sql
                , sess_id, waiting_reason, coalesce(nullif(application_name,''),'...') application_name
            from pg_stat_activity
            where state <> 'idle' 
            ) a
        left join (
            SELECT rn_td n, left(next_td::text,16) next, left(ready::text,16) ready
                , left(greatest(last_end, ready)::text,16) start, next_name wf
                , left((clock_timestamp() - greatest(last_end, ready))::text,8) duration
                , left((greatest(last_end, ready) - ready)::text,8) wait
                , swf_name
            FROM vw_swf_status
            where next_name is not null
            order by rn_td
        ) b on b.wf = substring(a.application_name from '(\w+)/?')
        where coalesce(wf,'') ~ '%1$s' and %2$s
        order by tr, 1
        $_$
    when '#todo' then
        $_$
        select b.n rn, b.todo
            , b.next, b.ready, b.start, b.wf, b.duration
            , b.wait wait_swf, b.duration::interval - a.dur::interval wait_gp, a.sql
            , a.is_swf, a.ts, a.tr, a.dur, a.pid, a.state
        from (
            SELECT rn_td n, left(wf_next::text,16) next, left(ready::text,16) ready
                , left(greatest(wf_last_end, ready)::text,16) start, wf_name wf
                , left((clock_timestamp() - greatest(wf_last_end, ready))::text,8) duration
                , left((greatest(wf_last_end, ready) - ready)::text,8) wait
                , todo
            FROM vw_swf
            where active and (todo or not done)
            order by rn_td
            ) b
        left join (
            select row_number() over(partition by (query like 'select s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_all(%') order by xact_start) as rn
                , (query like 'select s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_all(%') is_swf 
                , left(query_start::text,16) ts, left(xact_start::text,16) tr, left((clock_timestamp()-query_start)::text,8) dur, pid, state, left(query,200) sql 
            from pg_stat_activity
            where state <> 'idle' 
        ) a on a.rn = b.n and a.is_swf and b.todo
        order by tr, b.todo desc, 1
        $_$
        
    when '#vacuum' then
        sql =$_$
        select row_number() over(order by coalesce(last_vacuum, '1900-01-01') ,  coalesce(last_analyze, '1900-01-01') , schemaname desc) n
--            , round(n_dead_tup::numeric/nullif(n_live_tup,0), 2)::text dl
            , schemaname ,relname, n_dead_tup, n_live_tup
            , last_analyze::text, analyze_count, last_vacuum::text, vacuum_count
            , n_dead_tup::float / nullif(n_live_tup,0) as rate
            , b.table_type, b.is_insertable_into 
--            , a.*
            --, c.reloptions, c.relstorage
        from pg_stat_all_tables a 
        inner join tables b on a.schemaname = b.table_schema and a.relname = b.table_name -- and b.is_insertable_into::text = 'YES'
        where a.schemaname like '%_hr_edp_%' and a.relname ~ '%1$s' and %2$s
            -- and a.relname not like 'tb_swf_%_log'
            and a.relname not like '%_1_prt_%'
            -- and n_dead_tup > 0 
            -- and lower(left(a.relname,3)) not in('pxf','ext') 
            -- and last_analyze is not null
        order by 1
        $_$
    when '#swf chk' then
        -- if whr == 'true': whr = "7"
        sql =$_$
        select a.beg_id
        , a.wf, a.swf, a.td
        , left(a.ready::text, 16) ready
        , left(a.beg_ts::text, 16) beg_ts
        , left(a.end_ts::text, 16) end_ts
        , left(a.duration::text, 8) duration
        , a.reselt
        , a.beg_msg->>'chk' beg_chk, a.end_msg->>'chk' end_chk
        , a.msg, a.beg_msg, a.end_msg 
        from vw_swf__log a
        where beg_id in 
            (select distinct a.beg_id 
            from vw_swf__log a
            inner join vw_swf__log b on a.wf = b.wf and a.ready = b.ready and a.beg_id <> b.beg_id
            where a.end_action = 'do' and b.end_action = 'do' 
                -- and (a.end_msg->>'wf_id') is not null and (b.end_msg->>'wf_id') is not null
                and a.ready >= current_date - %2$s and b.ready >=  current_date - case when %2$s = 'true' then 7 else %2$s end
                and a.wf ~ '%1$s'
                )
        and a.ready >= current_date - %2$s and a.end_action = 'do' and a.wf ~ '%1$s'
        order by 1 desc
        $_$
    when '#rel chk' then
        $_$
        with swf as (
            select * from tb_swf where coalesce(wf_end, now()) >= now() or ctl_wf_id is not null
        )
        select 'wf_waits' as rel_type, a.*
        from (SELECT distinct wf_id, wf_name, unnest(wf_waits) AS wf, wf_waits wf_rel from swf) a
        LEFT JOIN ( SELECT wf_name FROM swf) b ON a.wf = b.wf_name
        where b.wf_name is null
        union  all
        select 'wf_relations' as rel_type, a.*
        from (SELECT distinct wf_id, wf_name, unnest(wf_relations) AS wf, wf_relations wf_rel FROM swf) a
        LEFT JOIN ( SELECT wf_name FROM swf) b ON a.wf = b.wf_name
        where b.wf_name is null
        order by 3,4
        $_$
    when '#lock' then
        $_$
        select l.locktype, l.database, l.relation, l.virtualtransaction, l.pid, l.mode
        , a.usename
        , r.relname
        , c.relname class
        , o.relname obj
        , l.granted
--        , pg_blocking_pids(l.pid) 
        , left(a.query_start::text,16) ts, left((clock_timestamp()-a.query_start)::text,8) dur, a.state, left(a.query,200) sql 
        , l.fastpath, l.mppsessionid, l.mppiswriter, l.gp_segment_id
        , l.page, l.tuple, l.virtualxid, l.transactionid, l.classid, l.objid, l.objsubid
        from pg_locks l
        left join pg_class c on c.oid = l.classid
        left join pg_class o on o.oid = l.objid
        left join pg_class r on r.oid = l.relation
        join pg_stat_activity a on a.pid = l.pid
        where l.database > 0 
--            and a.usename like 'u_%_hr_%'
            and coalesce(r.relname,'') ~ '%1$s' and %2$s
--            and not l.granted
            and l.relation in (select distinct relation from pg_locks where not granted)
        order by l.relation, l.granted desc, l.pid
        $_$
        -- wf_name ~ '%1$s' and
    when '#lock tbl' then
        $_$
        select l.locktype, l.database, l.relation, l.virtualtransaction, l.pid, l.mode
        , a.usename
        , r.relname
        , c.relname class
        , o.relname obj
        , l.granted
--        , pg_blocking_pids(l.pid) 
        , left(a.query_start::text,16) ts, left((clock_timestamp()-a.query_start)::text,8) dur, a.state, left(a.query,200) sql 
        , l.fastpath, l.mppsessionid, l.mppiswriter, l.gp_segment_id
        , l.page, l.tuple, l.virtualxid, l.transactionid, l.classid, l.objid, l.objsubid
        , r.*
        from pg_locks l
        left join pg_class c on c.oid = l.classid
        left join pg_class o on o.oid = l.objid
        left join pg_class r on r.oid = l.relation
        join pg_stat_activity a on a.pid = l.pid
        where l.database > 0 
--            and a.usename like 'u_%_hr_%'
            and r.relacl::text like '%u_sklgrnplm_s_vd_hr_edp_ppl%'
            and coalesce(r.relname,'') ~ '%1$s' and %2$s
--            and not l.granted
--            and l.relation in (select distinct relation from pg_locks where not granted)
        order by l.relation, l.granted desc, l.pid
        $_$
        -- wf_name ~ '%1$s' and
    when '#lock det' then
        $_$
          SELECT b.relname obj 
                 , blocked_locks.pid         AS blocked_pid
                 , blocked_locks.locktype AS blocked_locktype
                 , string_agg(distinct blocked_locks.mode, ',') AS blocked_mode
                 , left(blocked_activity.query_start::text,16) blocked_start
                 , blocked_activity.usename  AS blocked_user
                 , blocked_activity.application_name    AS blocked_app
                 , blocked_activity.state    AS blocked_state
                 , string_agg(distinct blocking_locks.pid::text, ', ') AS blocking_pid
                 , blocking_locks.granted blocking_granted
                 , string_agg(distinct blocking_locks.mode, ', ') AS blocking_mode
                 , left(blocking_activity.query_start::text,16) blocking_start
                 , blocking_activity.usename AS blocking_user
                 , blocking_activity.application_name   AS blocking_app
                 , blocking_activity.state   AS blocking_state
                 , blocked_activity.query    AS blocked_query
                 , blocking_activity.query   AS blocking_query
           FROM pg_locks         blocked_locks
           JOIN pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
           JOIN pg_locks         blocking_locks 
                ON blocking_locks.locktype = blocked_locks.locktype
                AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                AND blocking_locks.pid != blocked_locks.pid
           JOIN pg_stat_activity  blocking_activity ON blocking_activity.pid = blocking_locks.pid
           left join pg_class b on b.oid = blocked_locks.relation
           -- left join pg_class c on c.oid = blocking_locks.relation
           WHERE true and NOT blocked_locks.granted
                and (blocked_activity.usename like 'u_%_hr_%' or blocking_activity.usename like 'u_%_hr_%')
                and coalesce(b.relname,'') ~ '%1$s' and %2$s
            group by obj, blocked_pid, blocked_locktype, blocked_start, blocked_user, blocked_query, blocked_app, blocked_state
                --, blocking_pid
                , blocking_start, blocking_user, blocking_query, blocking_app, blocking_state, blocking_granted
            order by obj, blocking_granted desc, blocked_pid
        $_$
       -- 
    when '#swf' then
        $_$select * from vw_swf where lower(wf_name) ~ lower('%1$s') and %2$s order by 1$_$

    when '#swf tb' or sql.lower() == '#swf tbl' then
        $_$
            select left(wf_beg::time::text, 8) wf_time
                , left(s_grnplm_vd_hr_edp_srv_wf.pr_swf_get_next(a.wf_beg, a.wf_interval, a.wf_last + a.wf_duration, a.wf_end)::text, 16) wf_next
                , ctl_wf_id
                , wf_id, wf_order, wf_name, wf_beg, wf_interval::text, wf_end, wf_expire::text
                , wf_exec, wf_relations::text, wf_waits::text, wf_last, wf_duration, wf_swf, wf_reselt 
            from tb_swf a where lower(wf_name) ~ lower('%1$s') and %2$s order by 1,2,wf_order,wf_id
        $_$
    when '#status' then
        $_$select * from vw_swf_status where  swf_name ~ '%1$s' and %2$s order by 1$_$

    when '#status tb' or sql.lower() == '#status tbl' then
        $_$select * from tb_swf_status where  swf_name ~ '%1$s' and %2$s order by 1$_$

    when '#log last' then
        $_$select * from (select distinct on (workflow) * from vw_log_workflow where lower(workflow) ~ lower('%1$s') and %2$s order by workflow, start_id desc) a order by coalesce(end_ts,start_ts) desc$_$
    when '#log' then
        $_$
            select start_id::text
            , left(start_ts::text, 16) start_ts
            , start_action::text, workflow::text, end_id::text, end_action::text
            , left(end_ts::text, 16) end_ts
            , left(duration::text, 8) duration
            , message::text
            , to_char(rows_count, 'FM999,999,999,999,999,999') rows_count
            , period_name::text, period_from::text, period_to::text, load_name::text, load_min::text, load_max::text, key_name::text, key_min::text, key_max 
            from vw_log_workflow where lower(workflow) ~ lower('%1$s') and %2$s 
        $_$ || case when whr ~ 'order by' then '' else 'order by coalesce(end_ts,start_ts) desc' end
           -- if whr.find("order by") else "order by coalesce(end_ts,start_ts) desc"
    when '#log tb' then
        $_$
            select a.* , b.*
            from tb_log_workflow a
            left join tb_log_workflow_stat b on a.id = b.log_id
            where lower(wf_message) ~ lower('%1$s') and %2$s order by id desc
        $_$

    when '#log stat' then
        $_$
            select b.*
            from tb_log_workflow_stat b 
            where lower(wf_obj) ~ lower('%1$s') and %2$s 
            order by ts desc nulls last,1 desc
        $_$

    when '#log swf' then
        $_$select * from tb_swf_%1$s_log where %2$s order by 1 desc$_$
         
    when '#skew last' then
        $_$
            select ts, replace(tbl, 's_grnplm_vd_hr_edp_', '') tbl, skew, segments
            , pg_size_pretty(tbl_size) size
            , 1.0 * tbl_size / nullif(data_size, 0) as ratio
            , (100.0 * min/max)::int::text || ' %' min_max
            , sum
            , min
            , max
            , avg
            , std 
            --, data_size
            , duration
            , storage
            , replace(distributedby, 'DISTRIBUTED ', '') distributedby
            --, tbl_size
            , options
            from (select distinct on (tbl) * from tb_log_skew where tbl not like '%_1_prt_%' and tbl ~ '%1$s' and %2$s order by tbl, 1 desc) a
            order by 1 desc
        $_$
    when '#skew hist' then
        $_$
            select ts, replace(tbl, 's_grnplm_vd_hr_edp_', '') tbl, skew, segments
            , pg_size_pretty(tbl_size) size, 1.0 * tbl_size / nullif(data_size, 0) as ratio
            , sum, min, max, avg, std
            --, data_size
            , duration
            , storage
            , replace(distributedby, 'DISTRIBUTED ', '') distributedby
            --, tbl_size
            , options
            from tb_log_skew 
            where tbl not like '%_1_prt_%' 
                and tbl ~ '%1$s' and %2$s 
            order by 1 desc
        $_$
    when '#skew' then
        $_$
            with seg as ( 
                select '%1$s' tbl, gp_segment_id, count(1) , sum(pg_column_size(a.*)) size
                from %1$s a 
                group by 1, 2 
            )
            select a.tbl
                , to_char(a.count, 'FM999,999,999,999,999,999') count
                , to_char(a.min, 'FM999,999,999,999,999,999') min
                , to_char(a.avg, 'FM999,999,999,999,999,999') avg
                , to_char(a.max, 'FM999,999,999,999,999,999') max
                , to_char(a.stddev, 'FM999,999,999,999,999,999') stddev
                , a.min_rate, a.max_rate, a.stddev_rate, a.min_max, a.size
--                , b.gp_segment_id as min_gp
--                , pg_size_pretty(b.size) as min_size
--                , c.gp_segment_id as max_gp
--                , pg_size_pretty(c.size) as max_size
                , pg_size_pretty(t.tsize) tbl_size
                , pg_size_pretty(a.size) data_size
                , 1.0 * t.tsize / nullif(a.size, 0) as ratio
                , pg_get_table_distributedby('%1$s'::regclass::oid) distributedby
            from (
                select tbl
                    , count(a.count), min(a.count), avg(a.count)::int8, max(a.count) , stddev(a.count)::int8
                    , 1.0 - (min(a.count) / avg(a.count)) as min_rate
                    , max(a.count) / avg(a.count) - 1 as max_rate
                    , stddev(a.count) / avg(a.count) as stddev_rate
                    , 1.0 - (min(a.count)::numeric / max(a.count)) as min_max
                    , sum(size) size
                from seg a
                group by 1
            ) a
--            join (select distinct on (tbl) * from seg order by tbl, count) b on true
--            join (select distinct on (tbl) * from seg order by tbl, count desc) c on true
            join (select pr_table_size('%1$s') tsize) t on true

        $_$

    when '#err' then
        --, pr_get_func(split_part(context, 'statement\n', 1)) context_1
        $_$
            select * 
                , pr_get_func(context) context_0
                , split_part(context, 'PL/pgSQL ', 2) context_1
                , split_part(context, 'PL/pgSQL ', 3) context_2
                , split_part(context, 'PL/pgSQL ', 4) context_3
            from vw_log_workflow_err where coalesce(last_call,'') <> 'pr_log_error' and coalesce(last_call,'') ~ '%1$s' and %2$s order by 1 desc
            $_$
        -- "select * from tb_log_workflow_err where context ~ '%1$s' and %2$s order by 1 desc"
        
    when '#err all' then
        $_$select * from vw_log_workflow_err where  coalesce(last_call, '') ~ '%1$s' and %2$s order by 1 desc$_$
    when '#err tb' then
        $_$select * from tb_log_workflow_err where message ~ '%1$s' and %2$s order by 1 desc$_$
        
    when '#wf' then
        $_$
        select beg_action, beg_id, beg_ts, swf, wf, td, ready, wait, beg_msg, duration, end_id, end_ts, end_action, reselt, end_msg, msg 
        from vw_swf__log where end_action='do' and wf ~ '%1$s' and %2$s order by 1 desc
        $_$
    when '#wf all' then 
        $_$
        select beg_action, beg_id, beg_ts, swf, wf, td, ready, wait, beg_msg, duration, end_id, end_ts, end_action, reselt, end_msg, msg 
        from vw_swf__log where wf ~ '%1$s' and %2$s order by 1 desc$_$
    when '#wf cancel' then 
        $_$select * from vw_swf__log where end_action='cancel' and wf ~ '%1$s' and %2$s order by 1 desc$_$
    when '#wf last' then
        $_$select * from (select distinct on (wf) * from vw_swf__log where end_action='do' and wf ~ '%1$s' and %2$s order by wf, 1 desc) a order by 1 desc$_$
    when '#wf swf' then
        $_$select * from (select distinct on (swf) * from vw_swf__log where wf ~ '%1$s' and %2$s order by swf, 1 desc) a order by 1 desc$_$
    
    when '#cwf' then
        $_$
        select left(beg_msg->>'sdt', 16) event_ts 
        --, beg_id, beg_action
        , left(beg_ts::text, 16) beg_ts
        , wf
        , loading_id lid
        --, end_id
        , left(end_ts::text, 16) end_ts
        , left(duration::text, 8) duration
        , end_action
        , res, wf_id, msg
        , (beg_msg->>'cwf') wid
        , (beg_msg->>'rtr') rtr
        , (beg_msg->>'wfp') wfp
        -- , beg_msg, end_msg
        from vw_swf_ctl_log where wf ~ '%1$s' and %2$s 
        order by beg_id desc
        $_$
    when '#cwf tb' then
        $_$select * from tb_swf_ctl_log  where wf_message->>'wf'~ '%1$s' and %2$s order by ts desc$_$
    when '#cwf tb' then
        $_$select * from (select distinct on (wf) * from vw_swf_ctl_log where end_action='do' and wf ~ '%1$s' and %2$s order by wf, 1 desc) a order by 1 desc$_$
    when '#cwf err' then
        $_$
        select  * from tb_swf_ctl_log where lower(wf_action) = 'error' and %2$s order by ts desc
        $_$
    
    when '#obj' then
        $_$select oid,* from pg_class where relacl::text ~ 'edp_ppl' and relname ~ '%1$s' and %2$s$_$
        
    when '#table' then
        $_$
        select a.*
            , c.reloptions, c.relstorage --, to_char(c.reltuples, 'FM999,999,999,999,999,999') reltuples, c.relpages
            , pg_get_table_distributedby(format('%I.%I', schemaname, tablename)::regclass::oid) distributedby
            -- , pg_get_partition_def(format('%I.%I', schemaname, tablename)::regclass::oid) partition_def
            -- , pg_size_pretty(s_grnplm_vd_hr_edp_srv_wf.pr_table_size(table_schema||'.'||table_name)) table_size
        from pg_tables a
        left join pg_class c on format('%I.%I', schemaname, tablename)::regclass::oid=c.oid
        where schemaname like '%_hr_edp_%' and tablename not like '%_1_prt_%' 
            and lower(tablename) ~ lower('%1$s') and %2$s 
            and not tablename ~ 'ext_gpload'
        order by schemaname, tablename
        $_$
    when '#table det' then
        $_$
        select * from (
            select a.table_schema, a.table_name
                , case 
                    when d.partitionlevel is not null then 'PARTITION'
                    when c.relstorage = 'x' then 'PXF'
                    when a.table_type = 'BASE TABLE' then 'TABLE'
                    else a.table_type end as table_type
    --            , is_insertable_into
    --            , d.partitionlevel
    --            , d.partitionrank
    --            , d.partitiontype
                , pg_size_pretty(coalesce(b.size, pg_table_size(c.oid))) as table_size
                , c.relstorage 
                , c.reloptions
    --            , c.reltuples, c.relpages
                , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' 
                    then pg_get_table_distributedby(format('%I.%I', table_schema, table_name)::regclass::oid) 
                    else null end distributedby 
                , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' 
                    then substring(pg_get_partition_def(format('%I.%I', table_schema, table_name)::regclass::oid) from '(PARTITION BY \w+\([\w""]+\))')
                    else null end partition_def
                , prt_end
                , coalesce(b.n_live_tup, '') n_live_tup
                , coalesce(b.n_dead_tup, '') n_dead_tup
                , coalesce(b.prt_cnt, 0) prt_cnt
                , coalesce(b.use_cnt, 0) use_cnt
    --            , coalesce(b.use_prc, 0) use_prc
                , coalesce(b.prt_analyze, 0) prt_analyze
    --            , coalesce(b.prt_prc, 0) prt_prc
                , b.last_analyze, b.last_vacuum
                , e.tableowner, e.hasindexes, e.hasrules, e.hastriggers
            from tables a
            left join pg_class c on format('%I.%I', table_schema, table_name)::regclass::oid=c.oid
            left join (
                select a.schemaname,  coalesce(b.tablename, a.relname) tablename
                    , to_char(sum(n_live_tup), 'FM999,999,999,999,999,999') n_live_tup
                    , to_char(sum(n_dead_tup), 'FM999,999,999,999,999,999') n_dead_tup
                    , coalesce(nullif(count(1) - 1, 0), 1) prt_cnt
                    , count(nullif(n_live_tup, 0)) use_cnt
                    , round(count(nullif(n_live_tup, 0))::numeric / coalesce(nullif(count(1) - 1, 0), 1)*100, 0)::int2 use_prc
                    , count(last_analyze) prt_analyze 
                    , round(count(last_analyze)::numeric / coalesce(nullif(count(1) - 1, 0), 1)*100, 0)::int2 prt_prc
                    , left(max(last_analyze)::text, 16) last_analyze 
                --    , count(last_vacuum) fist_vacuum 
                    , left(max(last_vacuum)::text, 16) last_vacuum 
                    , sum(pg_table_size(a.relid)) size
                --    , min(partitionrangestart) prt_min
                    , max(partitionrangeend) prt_end
                from pg_stat_user_tables a
                left join pg_partitions b on a.schemaname = b.partitionschemaname and a.relname = b.partitiontablename
                where a.schemaname like 's_grnplm_vd_hr_edp_%'
                    and a.relname ~ lower('%1$s')
                    and not a.relname like 'ext_gpload%'
                group by 1,2
            )  b on a.table_schema = b.schemaname and a.table_name = tablename
            left join pg_partitions d on a.table_schema = d.partitionschemaname and a.table_name = d.partitiontablename
            left join pg_tables e on a.table_schema = e.schemaname and a.table_name = e.tablename
            where table_schema like '%_hr_edp_%' and partitionlevel is null
        ) a
        where lower(table_name) ~ lower('%1$s') and %2$s 
        order by 1,2
        $_$
    when '#table std' then
        $_$
        select table_schema, table_name, table_type, is_insertable_into 
            , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' then pg_get_table_distributedby((table_schema||'.'||table_name)::regclass::oid) else null end distributedby 
            , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' then pg_get_partition_def((table_schema||'.'||table_name)::regclass::oid) else null end partition_def 
        from tables where true
            and (table_schema like 'pg_%' or table_schema in ('public', 'information_schema','diskquota'))
            and table_name not like '%_1_prt_%' and table_name ~ '%1$s' and %2$s 
            and not table_name ~ 'ext_gpload'
        order by 1,2
        $_$
    when '#table all' then
        $_$
        select table_schema, table_name, table_type, is_insertable_into 
--            , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' then pg_get_table_distributedby((table_schema||'.'||table_name)::regclass::oid) else null end distributedby 
--            , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' then pg_get_partition_def((table_schema||'.'||table_name)::regclass::oid) else null end partition_def 
        from tables where true
--            and (table_schema like 'pg_%' or table_schema in ('public', 'information_schema','diskquota'))
            and table_name not like '%_1_prt_%' and table_name ~ '%1$s' and %2$s 
            and not table_name ~ 'ext_gpload'
        order by 1,2
        $_$
    when '#table det_old' then
        $_$
        select table_schema, table_name, table_type, is_insertable_into 
            --, c.reloptions, case c.relstorage when 'a' then 'append' when 'c' then 'column' when 'h' then 'heap' when 'v' then 'virtual' when 'x' then 'external' else c.relstorage end relstorage
            , c.reloptions, c.relstorage --, c.reltuples, c.relpages
--            , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' then pg_size_pretty(s_grnplm_vd_hr_edp_srv_wf.pr_table_size(table_schema||'.'||table_name)) else null end table_size
            , pg_size_pretty(b.size) as table_size
            , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' then pg_get_table_distributedby((table_schema||'.'||table_name)::regclass::oid) else null end distributedby 
            , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' 
                then substring(pg_get_partition_def(format('%I.%I', table_schema, table_name)::regclass::oid) from '(PARTITION BY \w+\([\w""]+\))')
                else null end partition_def 
            , b.n_live_tup, b.n_dead_tup, b.prt_cnt, b.use_cnt --, b.use_prc
            , b.prt_analyze --, b.prt_prc
            , b.last_analyze, b.last_vacuum
        from tables t
        left join pg_class c on (table_schema||'.'||table_name)::regclass::oid=c.oid
        left join (
            select a.schemaname,  coalesce(b.tablename, a.relname) tablename
                , to_char(sum(n_live_tup), 'FM999,999,999,999,999,999') n_live_tup
                , to_char(sum(n_dead_tup), 'FM999,999,999,999,999,999') n_dead_tup
                , coalesce(nullif(count(1) - 1, 0), 1) prt_cnt
                , count(nullif(n_live_tup, 0)) use_cnt
                , round(count(nullif(n_live_tup, 0))::numeric / coalesce(nullif(count(1) - 1, 0), 1)*100, 0)::int2 use_prc
                , count(last_analyze) prt_analyze 
                , round(count(last_analyze)::numeric / coalesce(nullif(count(1) - 1, 0), 1)*100, 0)::int2 prt_prc
                , left(max(last_analyze)::text, 16) last_analyze 
            --    , count(last_vacuum) fist_vacuum 
                , max(last_vacuum) last_vacuum 
                , sum(pg_table_size(a.relid)) as size
            from pg_stat_user_tables a
            left join pg_partitions b on a.schemaname = b.partitionschemaname and a.relname = b.partitiontablename
            where a.schemaname like 's_grnplm_vd_hr_edp_%'
                and a.relname ~ lower('%1$s')
                and not a.relname like 'ext_gpload%'
            group by 1,2
        )  b on t.table_schema = b.schemaname and t.table_name = b.tablename
        where table_schema like '%_hr_edp_%' and table_name ~ '%1$s' and %2$s order by 1,2
        $_$
    
    when '#table size' then
        $_$
        with tb_size as (
            select a.schemaname,  coalesce(b.tablename, a.relname) tablename
                , to_char(sum(n_live_tup), 'FM999,999,999,999,999,999') n_live_tup
                , to_char(sum(n_dead_tup), 'FM999,999,999,999,999,999') n_dead_tup
                , coalesce(nullif(count(1) - 1, 0), 1) prt_cnt
                , count(nullif(n_live_tup, 0)) use_cnt
                , round(count(nullif(n_live_tup, 0))::numeric / coalesce(nullif(count(1) - 1, 0), 1)*100, 0)::int2 use_prc
                , count(last_analyze) prt_analyze 
                , round(count(last_analyze)::numeric / coalesce(nullif(count(1) - 1, 0), 1)*100, 0)::int2 prt_prc
                , left(max(last_analyze)::text, 16) last_analyze 
            --    , count(last_vacuum) fist_vacuum 
                , max(last_vacuum) last_vacuum 
                , sum(pg_table_size(a.relid)) as size
            from pg_stat_user_tables a
            left join pg_partitions b on a.schemaname = b.partitionschemaname and a.relname = b.partitiontablename
            where a.schemaname like 's_grnplm_vd_hr_edp_%'
                and a.relname ~ lower('%1$s')
                and not a.relname like 'ext_gpload%'
            group by 1,2
        )
        select *
        from (
            select table_schema, table_name, table_type, is_insertable_into 
                , c.reloptions, case c.relstorage when 'a' then 'append' when 'c' then 'column' when 'h' then 'heap' when 'v' then 'virtual' when 'x' then 'external' else c.relstorage end relstorage
    --            , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' then pg_size_pretty(s_grnplm_vd_hr_edp_srv_wf.pr_table_size(try_cast2regclass(table_schema||'.'||table_name)::text)) else null end table_size
                , pg_size_pretty(b.size) as table_size
                , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' then pg_get_table_distributedby(try_cast2regclass(table_schema||'.'||table_name)::oid) else null end distributedby 
                , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' 
                    then substring(pg_get_partition_def(format('%I.%I', table_schema, table_name)::regclass::oid) from '(PARTITION BY \w+\([\w""]+\))')
                    else null end partition_def 
                , b.size
            from tables t
            join pg_class c on try_cast2regclass(table_schema||'.'||table_name)::oid=c.oid
            join tb_size  b on t.table_schema = b.schemaname and t.table_name = b.tablename
        ) a
        where table_type = 'BASE TABLE' and table_schema like '%_hr_edp_%' and table_name not like '%_1_prt_%' and table_name ~ '%1$s' and %2$s 
        order by size desc
        $_$
    when '#table sum_old' then
        $_$
        with tbl as (
            select table_schema, table_name, table_type, is_insertable_into 
                , c.reloptions, c.relstorage
                , pg_table_size(format('%I.%I',table_schema, table_name)) table_size
                , (pg_get_table_distributedby(format('%I.%I',table_schema, table_name)::regclass::oid) = 'DISTRIBUTED REPLICATED') is_repl 
                , (table_name like '%_1_prt_%') is_prt
                , (pg_get_partition_def(format('%I.%I',table_schema, table_name)::regclass::oid) is not null) is_prt_def
            from tables t
            left join pg_class c on format('%I.%I',table_schema, table_name)::regclass::oid=c.oid
            where table_schema like '%_hr_edp_%' 
                and table_name ~ '%1$s' and %2$s 
        )
        select table_schema, table_type, relstorage, is_insertable_into
            , is_prt_def, is_prt, is_repl
            , count(1) as table_cnt
            , pg_size_pretty(sum(table_size)) as table_sum
            , pg_size_pretty(max(table_size)) as table_max
        from tbl a
        group by 1,2,3,4,5,6,7
        union all
        select table_schema, null::text, null::text, null::text
            , null::boolean, null::boolean, null::boolean
            , count(1) as table_cnt
            , pg_size_pretty(sum(table_size)) as table_sum
            , pg_size_pretty(max(table_size)) as table_max
        from tbl a
        group by 1,2,3,4,5,6,7
        order by 1,2,3,4,5,6,7
        $_$
    when '#table sum' then
        sql=$_$
        with size as (
            select a.schemaname, c.relstorage   
                , a.relname
                , c.reloptions
                , (pg_get_table_distributedby(format('%I.%I',a.schemaname, a.relname)::regclass::oid) = 'DISTRIBUTED REPLICATED') is_repl 
                , (a.relname like '%_1_prt_%') is_prt
                , (pg_get_partition_def(format('%I.%I',a.schemaname, a.relname)::regclass::oid) is not null) is_prt_def
                , pg_table_size(format('%I.%I',a.schemaname, a.relname)) table_size
            from pg_stat_user_tables a
            join pg_class c on format('%I.%I',a.schemaname, a.relname)::regclass::oid=c.oid
            where a.schemaname like 's_grnplm_vd_hr_edp_d%' 
                and a.relname ~ '%1$s' and %2$s 
        )
            select schemaname, relstorage, is_repl
                , sum(is_prt_def::int) sum_prt_def
                , sum(is_prt::int) sum_prt
                , pg_size_pretty(sum(table_size)) table_size
                , count (1)
            from size a
            group by 1,2,3
        union all
            select schemaname, '=sum' as relstorage, null::bool is_repl
                , sum(is_prt_def::int) sum_prt_def
                , sum(is_prt::int) sum_prt
                , pg_size_pretty(sum(table_size)) table_size
                , count (1)
            from size a
            group by 1,2,3
        order by 1,2,3
        $_$
    when '#part' then
        $_$
            select * from pg_partitions 
            where schemaname like '%_hr_edp_%'and tablename ~ '%1$s' and %2$s 
            order by schemaname, tablename, partitionlevel, partitionrank nulls first
        $_$
    when '#part det' then
        $_$
            select pg_size_pretty(pg_total_relation_size((partitionschemaname||'.'||partitiontablename)::regclass::oid)) size, *
            from pg_partitions where schemaname like '%_hr_edp_%' and tablename ~ '%1$s' and %2$s 
            order by schemaname, tablename, partitionlevel, partitionrank nulls first
        $_$
    when '#part cnt' then
        $_$
        select schemaname, tablename, count(1) cnt, count(distinct partitionlevel) partitionlevels
        from pg_partitions 
        where schemaname like '%_hr_edp_%' and tablename ~ '%1$s' and %2$s 
        group by schemaname, tablename
        order by schemaname, tablename
        $_$
    when '#col' then
        $_$
        select t.table_schema, t.table_name, pa.attnum num, pa.attname fld
            --, format_type(pa.atttypid, pa.atttypmod) ftype
            , tp.typname, pa.atttypmod
            , pa.attstorage, pa.attalign, pa.attbyval, table_type
            , col_description(format('%I.%I', t.table_schema, t.table_name)::regclass::oid, pa.attnum)
        from pg_attribute pa 
        join tables t on (t.table_schema||'.'||t.table_name)::regclass::oid = pa.attrelid
        left join (select typname::regtype::oid typid, typname from pg_type where typrelid=0 and typnamespace=11 and typarray>0) tp on pa.atttypid = tp.typid
        where table_schema like '%_hr_edp_%' and table_name not like '%_1_prt_%' and table_name ~ '%1$s' and %2$s and pa.attnum > 0 
        order by 1, 2, pa.attnum
        $_$
    when '#col sht' then
        $_$
        select ordinal_position, table_schema, table_name, column_name, udt_name
            , col_description(format('%I.%I', table_schema, table_name)::regclass::oid, ordinal_position)
        from columns 
        where table_schema like '%_hr_edp_%' and table_name ~ '%1$s' and table_name not like '%_1_prt_%' and %2$s order by table_schema,table_name,ordinal_position
        $_$
    when '#col det' then
        $_$
        select * 
            , col_description(format('%I.%I', table_schema, table_name)::regclass::oid, ordinal_position)
        from columns 
        where table_schema like '%_hr_edp_%' and table_name ~ '%1$s' and table_name not like '%_1_prt_%' and %2$s order by table_schema,table_name,ordinal_position
        $_$
    when '#col list' then
        $_$
            select format('%I.%I',table_schema, table_name) tbl
                , string_agg(column_name, ', ' order by ordinal_position) fld
                , string_agg(format('%s::%s', column_name, udt_name), ', ' order by ordinal_position) ftp
                , string_agg(udt_name, ', ' order by ordinal_position) tps
            from (
                select table_catalog, table_schema, table_name, column_name, udt_name, ordinal_position 
                from columns 
                where table_schema like '%_hr_edp_%' and table_name not like '%_1_prt_%' and table_name ~ '%1$s' and %2$s order by table_schema,table_name,ordinal_position
            ) a
            group by 1
            order by 1
        $_$

    when '#col def' then
        $_$
        select string_agg(
            case 
            when part.part like '%COLUMN % ENCODING %' then ','
            when part.part like $$%START % END % orientation='column'%$$ then part.part||','
            else part end
            , '\n' order by tbl_def, n) tbl_def 
        from (
            select d.tbl_def||coalesce(part.part::text||'\n', '')||';\n' tbl_def
            from (
                select 'create table if not exists '||tbl_name
                || ' (\n'||string_agg(fld_def,'\n' order by fld_num)||'\n)\n'
                || coalesce('with '||translate(c.reloptions::text, '{}','()')||'\n', '')
                || coalesce(pg_catalog.pg_get_table_distributedby(tbl_name::regclass::oid) ||'\n', '')
                as tbl_def
                , tbl_name
                , c.reloptions
                $_$+$_$
                from (
                    select '    '||case when fld_num=1 then '' else ', 'end||fld_name||' '||fld_type 
                        ||case when is_nullable='NO' then ' not NULL' else '' end 
                        ||coalesce(' default '||column_default, '') as fld_def, *
                    from (
                        select format('%I.%I',table_schema, table_name) tbl_name
                            , row_number() over(partition by table_schema, table_name order by ordinal_position) fld_num
                            --, ordinal_position fld_num
                            , column_name fld_name
                            --, data_type fld_type
                            , udt_name fld_type
                            , is_nullable
                            , column_default
                        from columns 
                        where table_schema like '%_hr_edp_%' and table_name ~ '%1$s' and %2$s
                             and table_name not like '%_1_prt_%'
                        ) a
                    order by  tbl_name, fld_num
                ) b
                left join pg_class c on tbl_name::regclass::oid=c.oid
                group by tbl_name, c.oid, c.reloptions
            ) d            
            , pg_get_partition_def(tbl_name::regclass, true) part
            order by tbl_name
      ) a, regexp_split_to_table(tbl_def, '\n') with ordinality part(part,n)
        where part.part not like '%COLUMN % ENCODING %' 
            -- or right(trim(part.part), 1) = ','
        $_$
    when '#view' then
        $_$select * from pg_views where schemaname like 's_grnplm_vd_hr_edp_%' and viewname ~ '%1$s' and %2$s$_$
    when '#view all' then
        $_$select * from pg_views where viewname ~ '%1$s' and %2$s$_$
    when '#view std' then
        $_$select * from pg_views where (schemaname like 'pg_%' or schemaname in ('public', 'information_schema','diskquota')) and viewname ~ '%1$s' and %2$s$_$
    when '#view def' then
        $_$select format('\ncreate or replace view %I.%I as\n%s', schemaname, viewname, trim(definition)) from pg_views where schemaname ~ 's_grnplm_vd_hr_edp_' and viewname ~ '%1$s' and %2$s$_$
        
    when '#func' then
        $_$
        select func 
            , substring (txt from '\((.+)\).+RETURNS') def
            --, own::regclass
            , rol.rolname, rsg.rsgname
            --, txt
        from (
            select *, pg_get_functiondef(func::regprocedure) txt
            from (
                SELECT format('%I.%I(%s)', ns.nspname, p.proname, oidvectortypes(p.proargtypes)) func, proowner::regclass own
                FROM pg_proc p 
                INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
                WHERE ns.nspname like '%_hr_edp_%' 
                --  and lower(p.proname) ~ lower('%1$s') 
            ) a
            where lower(func) like lower('%%1$s%')
        ) a
        left join pg_roles rol on a.own = rol.oid
        left join pg_resgroup rsg on rol.rolresgroup = rsg.oid
        where %2$s
        order by 1
        $_$
    when '#func def' then
        $_$
        select txt
        from (
            select func, pg_get_functiondef(func::regprocedure)||';' txt 
            from (
                SELECT format('%I.%I(%s)', ns.nspname, p.proname, oidvectortypes(p.proargtypes)) func
                FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
                WHERE ns.nspname like 's_grnplm_vd_hr_edp_%' 
            ) a
            where lower(func) like lower('%%1$s%')
        ) a
        where %2$s
        order by 1
        $_$
    when '#func std' then
        $_$
            SELECT format('%I.%I(%s)', ns.nspname, p.proname, oidvectortypes(p.proargtypes)) func
            FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
            WHERE (ns.nspname like 'pg_%' or ns.nspname in ('public', 'information_schema','diskquota'))
                and lower(p.proname) ~ lower('%1$s') and  %2$s order by 1
        $_$
    when '#func std_def' then
        $_$
        select pg_get_functiondef(f::regprocedure)||';' f from (
            SELECT format('%I.%I(%s)', ns.nspname, p.proname, oidvectortypes(p.proargtypes)) f
            FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
            WHERE (ns.nspname like 'pg_%' or ns.nspname in ('public', 'information_schema','diskquota'))
                and lower(p.proname) ~ lower('%1$s') and %2$s order by 1
        ) a
        order by 1
        $_$
    when '#fld_stat' then
        $_$
        select ts::timestamp(0)
            , replace(tbl_name, 's_grnplm_vd_hr_edp_', '') tbl_name
            , fld_num
            , fld_name
            , data->>'fld_type' fld_type
            , data->>'null_cnt' null_cnt
            , (data->>'is_not_null')::bool not_null
            , coalesce(data->>'fld_stat', '...') fld_stat
            , data->>'dist_cnt' dist_cnt
            , (data->>'is_uniq')::bool is_uniq
            , data->>'min_len' min_len
            , data->>'max_len' max_len
            , coalesce((data->>'std_len')::int, 0) std_len
            , left(data->>'min', 50) min
            , left(data->>'max', 50) max
            , data->>'cnt' cnt
        from tb_log_fld_stat a
        -- left join (select typname::regtype::oid typid, typname from pg_type where typrelid=0 and typnamespace=11 and typarray>0) tp on a.fld_type::regtype::oid = tp.typid
        where lower(tbl_name) ~ lower('%1$s') and %2$s
        order by ts desc, tbl_name, fld_num
        $_$
    when '#fld_stat last' then
        $_$
        select ts::timestamp(0)
            , replace(tbl_name, 's_grnplm_vd_hr_edp_', '') tbl_name
            , fld_num
            , fld_name
            , data->>'fld_type' fld_type
            , data->>'null_cnt' null_cnt
            , (data->>'is_not_null')::bool not_null
            , coalesce(data->>'fld_stat', '...') fld_stat
            , data->>'dist_cnt' dist_cnt
            , (data->>'is_uniq')::bool is_uniq
            , data->>'min_len' min_len
            , data->>'max_len' max_len
            , coalesce((data->>'std_len')::int, 0) std_len
            , left(data->>'min', 50) min
            , left(data->>'max', 50) max
            , data->>'cnt' cnt
        from (select distinct on (tbl_name, fld_num) * from  tb_log_fld_stat where lower(tbl_name) ~ lower('%1$s') and %2$s order by tbl_name, fld_num, ts desc ) a
        -- left join (select typname::regtype::oid typid, typname from pg_type where typrelid=0 and typnamespace=11 and typarray>0) tp on (a.data->'fld_type')::regtype::oid = tp.typid
        order by ts desc, tbl_name, fld_num
        $_$
    when '#fld_stat fld' then
        $_$
        select fld_name
            , data->>'fld_type' fld_type
        from (select distinct on (tbl_name, fld_num) * from  tb_log_fld_stat where lower(tbl_name) ~ lower('%1$s') and %2$s order by tbl_name, fld_num, ts desc ) a
        order by ts desc, tbl_name, fld_num
        $_$
    when '#fld_stat def' then
        -- , case when table_type = 'BASE TABLE' and is_insertable_into = 'YES' then pg_get_partition_def((table_schema||'.'||table_name)::regclass::oid) else null end partition_def 
        -- -- || coalesce(pg_catalog.pg_get_partition_def(tbl_name::regclass::oid, true)::text ||'\n' ,'')
        -- select d.tbl_def, part.part, '\n;\n' tbl_def
        $_$
        select d.tbl_def||coalesce(part.part::text||'\n', '')||';\n' tbl_def
        from (
            select 'create table if not exists '||tbl_name
            || '(\n'||string_agg(fld_def,'\n' order by fld_num)||'\n)\n'
            || coalesce('with '||translate(c.reloptions::text, '{}','()')||'\n', '')
            || coalesce(pg_catalog.pg_get_table_distributedby(tbl_name::regclass::oid) ||'\n', '')
            as tbl_def
            , ts, tbl_name
            $_$+$_$
            from (
                select '    '||case when fld_num=1 then '' else ', 'end||fld_name||' '||fld_type as fld_def, *
                from (select distinct on (tbl_name, fld_num) * from  tb_log_fld_stat where lower(tbl_name) ~ lower('%1$s') and %2$s order by tbl_name, fld_num, ts desc ) a
                order by ts desc, tbl_name, fld_num
            ) b
            left join pg_class c on tbl_name::regclass::oid=c.oid
            group by ts, tbl_name, c.oid, c.reloptions
        ) d            
        , pg_get_partition_def(tbl_name::regclass, true) part
        order by ts desc, tbl_name
        $_$
    when '#vda tb' then
        $_$
            select vda_name, actual, main::text, workflows::text, sub_wfs::text, z_except::text 
            from tb_vda 
            where lower(vda_name) ~ lower('%1$s') and %2$s
            order by 1
        $_$
    when '#vda' then
        $_$
            select wf_status, data_mart, log_date, cnt_wf, cnt_start, cnt_ok, cnt_err
            , sum_duration::text, beg_ts, end_ts, duration::text, rows_count, period_name, period_from, period_to, load_name, load_min, load_max, key_name, key_min, key_max
            , replace(wf_error_msg,'\\n','<br>') wf_error_msg
            , replace(wf_msg,'\\n','<br>') wf_msg
            , ztest_check, ztest_error
            from s_grnplm_vd_hr_edp_vda.vw_workflow 
            where lower(data_mart) ~ lower('%1$s') and %2$s
            order by end_ts desc
        $_$
    when '#ztest' then
        sql=$_$
        --select distinct on (n, key_date) *
        select *
        --    n, nn, ts, object, ztest_ok, msg, is_except, is_error, confidence, zscore, key_date, key_diff, stable, rows_count, value, avg, std, cnt, min, max, log_id, notes
        from (
            select b.n 
                , row_number() over(partition by last order by key_date desc, ts desc) nn
                , left(z.ts::text, 16) ts
                --, z.object
                , replace(z.object, 's_grnplm_vd_hr_edp_', '') object
                , z.ztest_ok
                , (z.notes->>'msg') msg
                , z.is_except, z.is_error
--                , z.confidence
                , (100 - abs(round(z.zscore::numeric, 0)))::int as confidence
                , round(z.zscore::numeric, 2) zscore
                , round(z.stable::numeric, 2) stable
                , to_char(z.rows_count, 'FM999,999,999,999,999,999') rows_count
                , z.key_date
                , '+ ' || to_char(z.key_diff, 'FM999,999,999,999,999,999') key_diff
                , to_char(z.value, 'SGFM999,999,999,999,999,999') value
                , '(' || to_char(z.cnt,   'FM999,999,999,999,999,999') || ')' cnt
                , to_char(z.avg,   'FM999,999,999,999,999,999') avg
                , to_char(z.std,   'FM999,999,999,999,999,999') std
                , to_char(z.min,   'FM999,999,999,999,999,999') min
                , to_char(z.max,   'FM999,999,999,999,999,999') max
                , z.log_id
                , b.cnt_last
                , (z.notes->>'cfg') cfg
            from s_grnplm_vd_hr_edp_srv_dq.vw_ztest z
            inner join (
                select 
                     row_number() over(order by last desc) n, z.*
                from (
                    select object, max(ts) last, max(cnt) cnt_last
                    from s_grnplm_vd_hr_edp_srv_dq.vw_ztest z
                    where true
        --                and ts >= (now() - '9 hours'::interval)::date
        --                and (error is not null or split_part(z.object, '.', 1) ~ 's_grnplm_vd_hr_edp_')
        --                and not ztest_ok
                        and split_part(z.object, '.', 1) ~ 's_grnplm_vd_hr_edp_'
                        and split_part(z.object, '.', 2) ~ '%1$s' and %2$s
                    group by 1
                ) z
            ) b on z.object = b.object
        ) a
        --where not (ztest_ok or coalesce(is_except, false)) or coalesce(is_error, false)
        --where nn <= cnt_last and nn <= 30
        --order by n, key_date desc, ts desc, ztest_ok::text desc
        order by ts desc
        $_$
    else null end);
    
    sql = format(sql, prm, whr);
    
    return sql;

end; 
$body$
EXECUTE ON ANY;
	
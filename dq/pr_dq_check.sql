CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.pr_dq_check(_metric text DEFAULT '%'::text, _tbl text DEFAULT '%'::text, report json DEFAULT NULL::json) 
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

declare
    exe text;
    rec record;
    jsn json;
    -- res int4 = 1;
    html text;
    style json;
    txt text;
    lid int4;
    
    off int = 0;
    rc int8;
    app text;
    
    ts timestamp = clock_timestamp();
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    execute 'show application_name' into app;
    lid = pr_log_start( format('CHECK_%s_%s', _metric, _tbl));
    begin
        report = coalesce(report, json_build_object('rep', True, 'only_err', False, 'gr_ok', True, 'max_err', 1, 'max_no', 10, 'max_ok', 10), '{}'::json);
        loop
            execute format('set application_name = %L', app||'>loop_'||off::text);
            exe = null::text;
            
            for rec in (
                select * 
                from s_grnplm_vd_hr_edp_srv_dq.tb_dq_checks a
                where a.metric like _metric and a.tbl like _tbl
                    and a.active
                order by a.metric, a.tbl, a.params::text
                limit 450 offset off
            ) loop

                exe = exe || ' union all ';
                exe = concat(exe, format($sql$
                     select %1$L as metric, %2$L as tbl, %3$L::json as params
                         , s_grnplm_vd_hr_edp_srv_dq.pr_dq_%1$s(%2$L, %3$L::json) as msg
                $sql$, rec.metric, rec.tbl, rec.params)); 

            end loop;

            if off = 0 then
                exe = $sql$
                    create temp table tmp_vw
                    WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=3)
                    on commit drop as 
                $sql$ || exe;
            else
                exe = $sql$
                    insert into tmp_vw
                $sql$ || exe;
            end if;

            execute exe;
            get diagnostics rc = ROW_COUNT;
            
            exit when rc < 450 ;
            off = off + rc;
        end loop;

        execute format('set application_name = %L', app||'>rep');
        -- ("True 438, False 26, Error 176",-1,640,23,640,3,3)

        select string_agg(format('%s %s', case when res > 0 then 'True' when res = 0 then 'False' else 'Error' end, cnt), ', ' order by res desc) msg
            , min(a.res) res
            , sum(a.cnt) cnt
            , (select count(distinct tbl) from tmp_vw) tbl_cnt
            , (select count(distinct params->>'fn') from tmp_vw) fn_cnt
            , (select count(distinct msg->>'dt') from tmp_vw) dt_cnt
            , (select count(distinct metric) from tmp_vw) mt_cnt
            , report as report
        into rec
        from (
            select (a.msg->>'res')::int res, count(1) cnt
            from tmp_vw a
            group by 1
        ) a;
        
        txt = case when rec.res < 0 then 'Error' when rec.res > 0 then 'True' else 'False' end;

        if coalesce((report->>'rep')::bool, True) then
        
            exe = format($sql$
                create temp table tmp_rep
                WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=3)
                on commit drop as 
                with vw as (
                    select a.*
                        , (a.msg->>'res')::int as res
                        , (a.msg->>'dt') as dt
                        , (a.params->>'fn') as fn
                    from tmp_vw a
                )
                select row_number() over(order by a.res::int, a.dt, a.metric, a.tbl) as rn
                     -- , a.res
                     -- , (a.res > 0) verdict
                     %1$s --, a.metric
                     %2$s --, a.tbl
                     --, a.fn feature %3$s
                     --, a.dt %4$s 
                     --, a.params
                     , a.msg
                     , case 
                       when a.res < 0 and a.cnt > %6$s then concat('Count: ', a.cnt) 
                       when a.res = 0 and a.cnt > %7$s then concat('Count: ', a.cnt) 
                       when a.res > 0 and a.cnt > %8$s then concat('Count: ', a.cnt) 
                       else a.fn end as feature
                from (
                    -- select a.metric
                    --     , a.tbl
                    --     --, a.params
                    --     , a.msg
                    --     , a.res
                    --     , a.dt
                    --     , a.fn
                    --     , 1 as cnt
                    -- from vw a
                    -- where a.res >= 999
                    -- union all
                    select 
                        case
                        when res > 0 and %9$s then json_build_object('res', 1, 'dt', a.dt)::text
                        else a.msg::text end as msg
                        -- , a.msg::text::json
                        , a.metric
                        , a.tbl
                        -- , null::json as params
                        , a.res
                        , a.dt
                        , string_agg(distinct a.fn, ', ')::text as fn
                        , count(distinct a.fn) cnt
                    from vw a
                    -- where a.res < 999
                    group by a.metric, a.tbl, a.res, a.dt, 1 -- a.msg::text --, a.msg->>'error'
                ) a
                where true  %5$s
                order by rn
            $sql$
                , case when rec.mt_cnt  > 1 then ', a.metric'       else '' end  -- 1
                , case when rec.tbl_cnt > 1 then ', a.tbl'          else '' end  -- 2
                , case when rec.fn_cnt  > 1 then ', a.fn feature'   else '' end  -- 3
                , case when rec.dt_cnt  > 1 then ', a.dt'           else '' end  -- 4
                , case when coalesce((report->>'only_err')::bool, False) then 'and a.res <= 0' else '' end  -- 5
                , coalesce((report->>'max_err')::int, 1) -- 6
                , coalesce((report->>'max_no')::int, 100) -- 7
                , coalesce((report->>'max_ok')::int, 1) -- 8
                , coalesce((report->>'gr_ok')::bool, True)::text -- 9
            );
            execute exe;
        
            style = json_build_object(
                'td', $css$ 
                    select 'id' , 'td'
                    union
                    select 'style' , 'white-space:'||
                        case
                        when %key% in ('feature') then 'pre-warp'
                        when %key% in ('msg') then 'nowarp'
                        else null end
                    union
                    select 'style' , 'word-warp:'|| 
                        case
                        when %key% in ('feature') then 'break-word'
                        else null end
                    union
                    select 'style' , 'text-align:'|| 
                        case 
                        when %type% = 'number' then 'right' 
                        when %key% = 'operation' then 'center' 
                        else null end
                    union
                    select 'style' , 'color:' || 
                        case
                        when %type% = 'boolean' and %value% = 'true' then 'green' 
                        when %type% = 'boolean' and %value% = 'false' then 'red' 
                        else null end
                    union
                    select 'style' , 'font-weight:' || 
                        case
                        when %type% = 'boolean' and %value% = 'true' then '600' 
                        when %type% = 'boolean' and %value% = 'false' then '600' 
                        else null end
                    union
                    select 'style' , 'background:' ||
                        case
                        when %key% in ('res', 'rn') then 
                            case
                            when (((%row%->>'msg')::json)->>'res')::int =  1 then 'palegreen'    -- Ok
                            when (((%row%->>'msg')::json)->>'res')::int =  0 then 'LemonChiffon' -- No
                            when (((%row%->>'msg')::json)->>'res')::int = -1 then 'pink'         -- Empty
                            when (((%row%->>'msg')::json)->>'res')::int = -99 then 'salmon'      -- Error
                            else null end
                        when %key% in ('dt', '_msg') then 
                            case
                            when now() - (((%row%->>'msg')::json)->>'dt')::date < '1 month'::interval then null --'palegreen'
                            when now() - (((%row%->>'msg')::json)->>'dt')::date < '2 month'::interval then 'LemonChiffon'
                            when now() - (((%row%->>'msg')::json)->>'dt')::date < '3 month'::interval then 'pink'
                            else 'salmon' end
                        else null end
                $css$
            );

            html = '';
            html = html || '<style>#table,#th,#tr,#td {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; text-align: left}</style>';
            html = html || '<style>#np {font-size:0px}</style>';
            html = html || '<style>#num {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; font-weight:500; text-align: right}</style>';
            
            html = concat(html, format('<div style="color:%1$s"><h2> Verdict %2$s </h2><h3> %3$s (%6$s) </h3><h4> All %4$s: %5$s </h4></div>'
                , case when rec.res < 0 then 'red' when rec.res > 0 then 'green' else 'orange' end -- 1
                , txt::text  -- 2
                , now()::timestamp(0)::text -- 3
                , rec.cnt -- 4
                , rec.msg::text  -- 5
                , (clock_timestamp() - ts)::interval(0)::text -- 6
                ));
            html = concat(html, pr_tbl2html('tmp_rep', replace(format('CHECK %s %s', _metric, _tbl), '%', ''), 'order by rn', style), format('<h4> %1$s </h4>', row_to_json(rec)::text));
            
            txt = json_build_object(
                'res', case txt when 'True' then 1 else -1 end
                , 'verdict', txt
                , 'html', array[html]
            )::text;
        
        else 
            
            txt = json_build_object(
                'res', case txt when 'True' then 1 else -1 end
                , 'verdict', txt
            )::text;
        end if;
        
        lid = pr_log_end(lid);
        execute format('set application_name = %L', app);
        return txt;

    exception when OTHERS then
        declare e_detail text; e_hint text; e_context text;
        begin
            get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL, e_hint = PG_EXCEPTION_HINT, e_context = PG_EXCEPTION_CONTEXT;

            lid = pr_log_error(0, sqlerrm, e_detail, exe, e_context); 
            execute format('set application_name = %L', app);
            return format('Error: %s (%s)', sqlerrm, sqlstate);
        end;
    end;
end;

$body$
EXECUTE ON ANY;
	
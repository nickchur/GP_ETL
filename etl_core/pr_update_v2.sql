CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_update_v2(log_id integer, wf text, pst text, query text, ds text[], step text, max_mod timestamp without time zone DEFAULT NULL::timestamp without time zone) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    -- max_mod timestamp ;
    log_id integer;
    m_txt text;
    rc int8;
    ri int8 = 0;
    rd int8 = 0;
    rm int8 = 0;
    rt int8 = 0;
    sql text;
    sds text;
    son text;
    rec record;
    kk int4 = 0;
    dd date;
    dd_ar date[];
    app text;
    min_rpt date;
    max_rpt date;
    dst text;
    fr_tbl text;
    to_tbl text;
    ztest text;
begin 
    execute 'show application_name' into app;
    -- log_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start(format('GPUPDATE_%1$s_cdm (pr_update_%1$s_cdm)', wf));

    begin
        execute format($$select substring(pg_get_partition_def('s_grnplm_vd_hr_edp_stg.tb_%s_cdm'::regclass::oid) from 'PARTITION BY RANGE\((\w+)\)' )$$, wf) into dst;
        
        -- execute format('truncate s_grnplm_vd_hr_edp_dia.tmp_%1$s%2$s', wf, pst);
        execute format('truncate s_grnplm_vd_hr_edp_dia.tmp_%s_cdm', wf);
        
        if max_mod is null then
            execute format('select max(mod_date) from s_grnplm_vd_hr_edp_stg.tb_%s_cdm', wf) into max_mod;
            max_mod = coalesce(max_mod, '1900-01-01'::date);
        end if;
        
        execute format('select min(%4$s)::date, max(%4$s)::date from s_grnplm_vd_hr_edp_stg.tb_%1$s%2$s where mod_date > %3$L', wf, pst, max_mod, dst) into min_rpt, max_rpt;

        if min_rpt is null then
            m_txt = 'No new data '||wf;
            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id, m_txt);
            return m_txt;
        end if;


        sql = '';
        for rec in (select a.attnum num, a.attname fld, format_type(a.atttypid, a.atttypmod) flf
                    from pg_catalog.pg_attribute a where a.attnum > 0
                    and a.attrelid = format('s_grnplm_vd_hr_edp_stg.tb_%s_cdm', wf)::regclass::oid  order by a.attnum) loop

            if rec.num <> 1 then
                sql = sql||', ';
            end if;

            if rec.fld = 'mod_date' then
                sql = sql||format($$ %s $$, 'b.min_tmp');
            else
                sql = sql||format($$ a.%s $$, rec.fld);
            end if;
        end loop;
        
        sds = (select string_agg(fl, ', ') from unnest(ds) fl);
        son = (select string_agg(format('a.%1$s = b.%1$s', fl), ' and ') from unnest(ds) fl);


        for rec in (select partitionposition ord, substring (partitiontablename from tablename||'_(\w+)') prt
                    from pg_partitions a where schemaname = 's_grnplm_vd_hr_edp_stg' and tablename = format('tb_%s_cdm', wf)
                        and (( substring(partitionrangeend from $s$'(\d{4}-\d{2}-\d{2})'::date$s$)::date > min_rpt
                                and substring(partitionrangestart from $s$'(\d{4}-\d{2}-\d{2})'::date$s$)::date <= max_rpt)
                            or partitionisdefault = true)
                    order by 1) loop

            execute format('set application_name = %L', app||'>'||rec.prt);

            -- execute format('truncate s_grnplm_vd_hr_edp_dia.tmp_%1$s%2$s', wf, pst);
            execute format('truncate s_grnplm_vd_hr_edp_dia.tmp_%s_cdm', wf);
            
            -- execute format('insert into s_grnplm_vd_hr_edp_dia.tmp_%1$s%2$s select * from s_grnplm_vd_hr_edp_stg.tb_%1$s%2$s_%3$s where mod_date > %4$L', wf, pst, rec.prt, max_mod);
            -- get diagnostics rc = ROW_COUNT;
            -- continue when rc = 0 ;
            -- rm = rm + rc;
            rt = 0;
            fr_tbl = format('s_grnplm_vd_hr_edp_stg.tb_%s%s_%s', wf, pst, rec.prt);
            to_tbl = format('s_grnplm_vd_hr_edp_dia.tmp_%s_cdm', wf);
            if step = 'day' then
                -- execute format($$select array_agg(distinct %3$s order by %3$s) from s_grnplm_vd_hr_edp_dia.tmp_%1$s%2$s $$, wf, pst, dst) into dd_ar;
                execute format($$select array_agg(distinct %2$s order by %2$s) from %1$s where mod_date > %3$L $$, fr_tbl, dst, max_mod) into dd_ar;
                for dd in (select a::date from unnest(dd_ar) a) loop
                    execute format('set application_name = %L', app||'>'||rec.prt||'>'||dd::text);
                
                    execute format(query, fr_tbl, to_tbl, max_mod, dd);
                    get diagnostics rc = ROW_COUNT;
                    rt = rt + rc;
                end loop;
            else 
                -- execute format(query, wf, pst, rec.prt, max_mod);
                execute format(query, fr_tbl, to_tbl, max_mod);
                get diagnostics rc = ROW_COUNT;
                rt = rt + rc;
            end if;
            
            continue when rt = 0 ;
            rm = rm + rt;

            execute format($sql$
                insert into s_grnplm_vd_hr_edp_dia.tmp_%1$s_cdm
                select * from s_grnplm_vd_hr_edp_stg.tb_%1$s_cdm_%2$s a
            $sql$, wf , rec.prt);

            get diagnostics rc = ROW_COUNT;
            rd = rd + rc;

            execute format('truncate s_grnplm_vd_hr_edp_stg.tb_%1$s_cdm_%2$s', wf, rec.prt);

            execute format($sql$
                insert into s_grnplm_vd_hr_edp_stg.tb_%1$s_cdm
                select %2$s
                from (
                    select distinct on (%3$s) *
                    from s_grnplm_vd_hr_edp_dia.tmp_%1$s_cdm a
                    order by %3$s, actual_date desc
                ) a
                left join (
                    select %3$s, hash, min(mod_date) min_tmp
                    from s_grnplm_vd_hr_edp_dia.tmp_%1$s_cdm b
                    group by 1, 2, 3
                ) b on %4$s and a.hash = b.hash
            $sql$, wf, sql, sds, son);

            get diagnostics rc = ROW_COUNT;
            ri = ri + rc;
            kk = kk + 1;
        end loop;

        execute format('set application_name = %L', app||'>ztest');
        -- ztest = s_grnplm_vd_hr_edp_srv_dq.fn_dq_ztest(
        --     format('s_grnplm_vd_hr_edp_stg.tb_%s_cdm', wf)
        --     'count(*)', dst, min_rpt, max_rpt
        --     -- (current_date - interval '14 month')::date,
        --     -- (current_date + interval '2 month')::date
        --     --(select min(report_date) from s_grnplm_vd_hr_edp_stg.tb_oss_cdm),
        --     --(select max(report_date) from s_grnplm_vd_hr_edp_stg.tb_oss_cdm)
        -- );

        execute format('set application_name = %L', app||'>log');

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id, format('s_grnplm_vd_hr_edp_stg.tb_%s_cdm', wf), dst, 'load_date','actual_date'); --ЛОГИРОВАНИЕ
        RETURN format('Ok %s %s %s (> %s) %s', wf, to_char(rm, 'FM999,999,999,999,999,999'), to_char(ri - rd, 'SGFM999,999,999,999,999,999'), max_mod, kk);

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

            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id, e_txt, e_detail, e_hint, e_context) ; 
            return 'Error: '||e_txt;
         end;
    end;
end;
$body$
EXECUTE ON ANY;
	
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_update_v2(log_id integer, wf text, pst text, query text, ds text[], jno text, step text, max_mod timestamp without time zone DEFAULT NULL::timestamp without time zone) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    -- max_mod timestamp ;
    -- log_id integer;
    m_txt text;
    rc int8;
    ri int8 = 0;
    rd int8 = 0;
    rn int8 = 0;
    ro int8 = 0;
    rt int8 = 0;
    sql text;
    sds text;
    son text;
    rec record;
    kk int4 = 0;
    dd date;
    dd_ar date[];
    app text;
    min_rpt date;
    max_rpt date;
    dst text;
    fr_tbl text;
    to_tbl text;
    flt text;
    exe text;
    whr text;
begin 
    execute 'show application_name' into app;
    -- log_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start(format('GPUPDATE_%1$s_cdm (pr_update_%1$s_cdm)', wf));
    begin
        
        exe = format($$select substring(pg_get_partition_def('s_grnplm_vd_hr_edp_stg.tb_%s_cdm'::regclass::oid) from 'PARTITION BY RANGE\((\w+)\)' )$$, wf);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe into dst;

        
        exe = format('truncate s_grnplm_vd_hr_edp_dia.tmp_%s_cdm', wf);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe;
        
        if max_mod is null then
            -- execute format('select max(mod_date) from s_grnplm_vd_hr_edp_stg.tb_%s_cdm', wf) into max_mod;
            exe = format('select max(actual_date) from s_grnplm_vd_hr_edp_stg.tb_%s_cdm', wf);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe into max_mod;
            max_mod = coalesce(max_mod, '1900-01-01'::date);
        end if;
        
        -- exe = format('select min(%4$s)::date, max(%4$s)::date from s_grnplm_vd_hr_edp_stg.tb_%1$s%2$s where actual_date > %3$L', wf, pst, max_mod, dst);
        exe = format('select min(%4$s)::date, max(%4$s)::date from s_grnplm_vd_hr_edp_stg.tb_%1$s%2$s where actual_date >= %3$L', wf, pst, max_mod, dst);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe into min_rpt, max_rpt;

        if min_rpt is null then
            m_txt = 'No new data '||wf;
            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id, m_txt);
            execute format('set application_name = %L', app);
            return m_txt;
        end if;


        sql = '';
        for rec in (select a.attnum num, a.attname fld, format_type(a.atttypid, a.atttypmod) flf
                    from pg_catalog.pg_attribute a where a.attnum > 0
                    and a.attrelid = format('s_grnplm_vd_hr_edp_stg.tb_%s_cdm', wf)::regclass::oid  order by a.attnum) loop

            if rec.num <> 1 then
                sql = sql||', ';
            end if;

            if rec.fld = 'mod_date' then
                sql = sql||format($$ %s $$, 'b.min_tmp');
                -- sql = sql||format($$ %s $$, 'null::timestamp');
            else
                sql = sql||format($$ a.%s $$, rec.fld);
            end if;
        end loop;
        

        sds = (select string_agg(fl, ', ') from unnest(ds) fl);
        son = (select string_agg(format('a.%1$s is not distinct from b.%1$s', fl), ' and ') from unnest(ds) fl);

        -- for rec in (
        --         select partitionposition ord, substring (partitiontablename from tablename||'(_\w+)') prt
        --             , partitionrangestart pr_beg, partitionrangeend pr_end, partitionisdefault pr_def
        --         from pg_partitions a where partitionlevel = 0 and step <> 'all'
        --                 and schemaname = 's_grnplm_vd_hr_edp_stg' and tablename = format('tb_%s%s', wf, pst) 
        --                 and (( substring(partitionrangeend from $s$'(\d{4}-\d{2}-\d{2}).*'::$s$)::date > min_rpt::date
        --                     and substring(partitionrangestart from $s$'(\d{4}-\d{2}-\d{2}).*'::$s$)::date <= max_rpt::date)
        --                     or partitionisdefault = true)
        --         union
        --         select 0 ord, '' prt, null pr_beg, null pr_end, true pr_def where step = 'all'
        for rec in (select * from s_grnplm_vd_hr_edp_srv_wf.pr_get_part('s_grnplm_vd_hr_edp_stg',format('tb_%s%s', wf, pst)
                                , case when step = 'all' then -1 else 0 end, min_rpt::date, max_rpt::date)
                order by 1) loop

            -- execute format('set application_name = %L', app||'<'||rec.prt);

            -- execute format('truncate s_grnplm_vd_hr_edp_dia.tmp_%s_cdm', wf);
            
            if rec.pr_def is not true then
                flt = format(' and a.%1$s >= %2$s and a.%1$s < %3$s ', dst, rec.pr_beg, rec.pr_end);
            else
                flt = '';
            end if;
            
            fr_tbl = format('s_grnplm_vd_hr_edp_stg.tb_%s%s%s', wf, pst, rec.prt);
            to_tbl = format('s_grnplm_vd_hr_edp_dia.tmp_%s_cdm', wf);
            
            if step = 'day' then
                -- exe = format($$select array_agg(distinct %2$s order by %2$s) from %1$s where actual_date > %3$L $$, fr_tbl, dst, max_mod);
                exe = format($$select array_agg(distinct %2$s order by %2$s) from %1$s where actual_date >= %3$L $$, fr_tbl, dst, max_mod);
                raise info '%', clock_timestamp();
                raise info '%', exe;
                execute exe into dd_ar;
                
                rt = 0;
                for dd in (select a::date from unnest(dd_ar) a) loop
                    execute format('set application_name = %L', app||'>'||rec.prt||'>'||dd::text);

                    flt = format(' and a.%1$s = %2$L ', dst, dd);
                
                    exe = format(query, fr_tbl, to_tbl, max_mod, flt);
                    raise info '%', clock_timestamp();
                    raise info '%', exe;
                    execute exe;
                    get diagnostics rc = ROW_COUNT;
                    rt = rt + rc;
                end loop;
            else 
                -- execute format(query, wf, pst, rec.prt, max_mod);
                execute format('set application_name = %L', app||'>new'||rec.prt);
                exe = format(query, fr_tbl, to_tbl, max_mod, flt);
                raise info '%', clock_timestamp();
                raise info '%', exe;
                execute exe;
                get diagnostics rc = ROW_COUNT;
                rt = rc;
            end if;
            
            continue when rt = 0 ;
            rn = rn + rt;

        end loop;

        -- execute format('set application_name = %L', app||'>anlz');
        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_analyze(format('s_grnplm_vd_hr_edp_dia.tmp_%1$s_cdm', wf));

        -- for rec in (select partitionposition ord, substring (partitiontablename from tablename||'_(\w+)') prt
        -- for rec in (select partitionposition ord, substring (partitiontablename from tablename||'(_\w+)') prt
        --                 , partitionrangestart pr_beg, partitionrangeend pr_end
        --             from pg_partitions a where partitionlevel = 0 
        --                 and schemaname = 's_grnplm_vd_hr_edp_stg' and tablename = format('tb_%s_cdm', wf)
        --                 and (( substring(partitionrangeend from $s$'(\d{4}-\d{2}-\d{2}).*'::$s$)::date > min_rpt::date
        --                         and substring(partitionrangestart from $s$'(\d{4}-\d{2}-\d{2}).*'::$s$)::date <= max_rpt::date)
        --                     or partitionisdefault = true)
        for rec in (select * from s_grnplm_vd_hr_edp_srv_wf.pr_get_part('s_grnplm_vd_hr_edp_stg', format('tb_%s_cdm', wf), 0, min_rpt::date, max_rpt::date)
                    order by 1) loop

            
            exe = format('select 1 from s_grnplm_vd_hr_edp_dia.tmp_%s_cdm%s limit 1', wf, rec.prt);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe into rc;
            continue when coalesce(rc, 0) = 0;
            
            execute format('set application_name = %L', app||'>old'||rec.prt);
            exe = format($sql$
                insert into s_grnplm_vd_hr_edp_dia.tmp_%1$s_cdm
                select a.* from s_grnplm_vd_hr_edp_stg.tb_%1$s_cdm%2$s a
                inner join (select distinct %3$s from s_grnplm_vd_hr_edp_dia.tmp_%1$s_cdm%2$s) b on %4$s
            $sql$, wf , rec.prt, sds, son);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe;

            get diagnostics rc = ROW_COUNT;
            ro = ro + rc;

            execute format('set application_name = %L', app||'>del'||rec.prt);
            -- execute format('truncate s_grnplm_vd_hr_edp_stg.tb_%1$s_cdm%2$s', wf, rec.prt);
            -- execute format('delete from s_grnplm_vd_hr_edp_stg.tb_%1$s_cdm%2$s where true', wf, rec.prt);
            -- execute format('delete from s_grnplm_vd_hr_edp_stg.tb_%1$s_cdm%2$s where %3$s in (select distinct %3$s from s_grnplm_vd_hr_edp_dia.tmp_%1$s_cdm%2$s)', wf, rec.prt, dst);
            exe = format('delete from s_grnplm_vd_hr_edp_stg.tb_%1$s_cdm%2$s a
                            using (select distinct %3$s from s_grnplm_vd_hr_edp_dia.tmp_%1$s_cdm%2$s) b
                            where a.%3$s = b.%3$s', wf, rec.prt, dst);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe;
            get diagnostics rc = ROW_COUNT;
            rd = rd + rc;
            
            if jno = '' then
                whr = format($sql$
                    inner join (
                        select report_date , max(actual_date) actual_date
                        from s_grnplm_vd_hr_edp_dia.tmp_%1$s_cdm%2$s
                        group by 1
                    ) c on a.report_date = c.report_date and a.actual_date = c.actual_date
                $sql$, wf, rec.prt); 
            else 
                whr = jno;
            end if;

            execute format('set application_name = %L', app||'>ins'||rec.prt);
            exe = format($sql$
                insert into s_grnplm_vd_hr_edp_stg.tb_%1$s_cdm
                select %2$s
                from (
                    select distinct on (%3$s) *
                    from s_grnplm_vd_hr_edp_dia.tmp_%1$s_cdm%6$s a
                    order by %3$s, actual_date desc, load_date desc
                ) a
                inner join (
                    select %3$s, hash, min(mod_date) min_tmp
                    from s_grnplm_vd_hr_edp_dia.tmp_%1$s_cdm%6$s b
                    group by %3$s, hash
                ) b on %4$s and a.hash = b.hash
                %5$s
            $sql$, wf, sql, sds, son, whr, rec.prt);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe;

            get diagnostics rc = ROW_COUNT;
            ri = ri + rc;
            kk = kk + 1;
            
            -- execute format('set application_name = %L', app||'>anlz'||rec.prt);
            -- perform s_grnplm_vd_hr_edp_srv_wf.pr_analyze(format('s_grnplm_vd_hr_edp_stg.tb_%1$s_cdm%2$s', wf, rec.prt));
        end loop;


        -- execute format('set application_name = %L', app||'>skew');
        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_log_skew(format('s_grnplm_vd_hr_edp_stg.tb_%s_cdm', wf)); 
        -- execute format('set application_name = %L', app||'>analyze');
        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_analyze(format('s_grnplm_vd_hr_edp_stg.tb_%s_cdm', wf));

        execute format('set application_name = %L', app||'>log');
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id, format('s_grnplm_vd_hr_edp_stg.tb_%s_cdm', wf), dst, 'load_date','actual_date'); --ЛОГИРОВАНИЕ
        
        execute format('set application_name = %L', app||'>ztest');
        perform s_grnplm_vd_hr_edp_srv_dq.fn_dq_ztest_v2(format('s_grnplm_vd_hr_edp_stg.tb_%s_cdm', wf), 'count(*)', dst, min_rpt, max_rpt, 0);
        
        execute format('set application_name = %L', app);
        RETURN format('Ok %s_cdm %s %s (> %s) %s', wf, to_char(rn, 'FM999,999,999,999,999,999'), to_char(ri - rd, 'SGFM999,999,999,999,999,999'), max_mod, kk);

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

            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id, e_txt, coalesce(nullif(e_detail,''),exe), e_hint, e_context) ; 
            execute format('set application_name = %L', app);
            return 'Error: '||e_txt;
         end;
    end;
end;
$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_update_v2(integer, text, text, text, text[], text, timestamp without time zone) IS 'Обновляет STG-таблицу из DIA-источника по шагу партиции и ключам распределения (v2)';

CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_diatostg_v2(log_id integer, wf text, fld text, hsh text, mod text, ds text[], jno text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    -- log_id integer;
    m_txt text;
    rc int8;
    rn int8 = 0;
    ri int8 = 0;
    rd int8 = 0;
    ro int8 = 0;
    -- max_id int8;
    -- max_ts timestamp default null;
    max_dia timestamp ;
    min_dia timestamp ;
    sql text;
    sds text;
    son text;
    rec record;
    k int4 = 0;
    dd date;
    app text;
    dst text;
    dss text;
    exe text;
    whr text;
begin 
    execute 'show application_name' into app;
    raise info '.';
    -- log_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start(format('SMDTOGP_%1s$s (pr_diatostg_%1$s)', wf));
    begin
        exe = format('truncate s_grnplm_vd_hr_edp_dia.tmp_%s', wf);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe;
        
        exe = format($$select substring(pg_get_partition_def('s_grnplm_vd_hr_edp_stg.tb_%s'::regclass::oid) from 'PARTITION BY RANGE\((\w+)\)' )$$, wf);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe into dst;

        exe = format($$select substring(pg_get_partition_def('s_grnplm_vd_hr_edp_dia.dia_%s'::regclass::oid) from 'PARTITION BY RANGE\((\w+)\)' )$$, wf);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe into dss;

        -- execute format('select min(ctl_validfrom), max(ctl_validfrom) from s_grnplm_vd_hr_edp_dia.dia_%s', wf) into min_dia, max_dia;
        exe = format('select min(%2$s), max(%2$s) from s_grnplm_vd_hr_edp_dia.dia_%1$s', wf, coalesce(dss, dst));
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe into min_dia, max_dia;

        if max_dia is null then
            m_txt = 'No new data in dia';
            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id, m_txt);
            execute format('set application_name = %L', app);
            return m_txt;
        end if;

        sql = '';
        for rec in (select a.attnum num, a.attname fld, format_type(a.atttypid, a.atttypmod) flf
                    from pg_catalog.pg_attribute a where a.attnum > 0
                    and a.attrelid = format('s_grnplm_vd_hr_edp_stg.tb_%s', wf)::regclass::oid  order by a.attnum) loop

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
        son = (select string_agg(format('a.%1$s is not distinct from b.%1$s', fl), ' and ') from unnest(ds) fl);

        -- for rec in (select partitionposition ord, substring (partitiontablename from tablename||'(_\w+)') prt
       --                 , partitionrangestart pr_beg, partitionrangeend pr_end, partitionisdefault pr_def
        --             from pg_partitions a where partitionlevel = 0 
        --                 and schemaname = 's_grnplm_vd_hr_edp_dia' and tablename = 'dia_'||wf
        --                 and ((substring(partitionrangeend from $s$'(\d{4}-\d{2}-\d{2}).*'::$s$)::date > min_dia::date
        --                     and substring(partitionrangestart from $s$'(\d{4}-\d{2}-\d{2}).*'::$s$)::date <= max_dia::date)
        --                     or partitionisdefault = true)
        --             order by 1) loop
        for rec in (select * from s_grnplm_vd_hr_edp_srv_wf.pr_get_part('s_grnplm_vd_hr_edp_dia',format('dia_%s', wf), 0, min_dia::date, max_dia::date)
                    order by 1) loop

            execute format('set application_name = %L', app||'>new'||rec.prt);

            --execute format('truncate s_grnplm_vd_hr_edp_dia.tmp_%s%s', wf, rec.prt);

            exe = format($sql$
                insert into s_grnplm_vd_hr_edp_dia.tmp_%1$s
                select %2$s 
                    , %3$s  as hash
                    , %4$s  as mod_date
                    , now() as load_date
                from s_grnplm_vd_hr_edp_dia.dia_%1$s%5$s a
            $sql$, wf, fld, hsh, mod, rec.prt);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe;

            get diagnostics rc = ROW_COUNT;
            continue when rc = 0 ;
            rn = rn + rc;
        end loop;

        -- execute format('set application_name = %L', app||'>anlz');
        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_analyze(format('s_grnplm_vd_hr_edp_dia.tmp_%1$s', wf));

        -- for rec in (select partitionposition ord, substring (partitiontablename from tablename||'(_\w+)') prt
        --                 , partitionrangestart pr_beg, partitionrangeend pr_end, partitionisdefault pr_def
        --             from pg_partitions a where partitionlevel = 0 
        --                 and schemaname = 's_grnplm_vd_hr_edp_stg' and tablename = 'tb_'||wf
        --                     and (( substring(partitionrangeend from $s$'(\d{4}-\d{2}-\d{2}).*'::$s$)::date > min_dia::date
        --                         and substring(partitionrangestart from $s$'(\d{4}-\d{2}-\d{2}).*'::$s$)::date <= max_dia)
        --                         or partitionisdefault = true)
        for rec in (select * from s_grnplm_vd_hr_edp_srv_wf.pr_get_part('s_grnplm_vd_hr_edp_stg',format('tb_%s', wf), 0, min_dia::date, max_dia::date)
                    order by 1) loop
            exe = format('select 1 from s_grnplm_vd_hr_edp_dia.tmp_%1$s%2$s limit 1', wf, rec.prt);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe into rc;
            continue when coalesce(rc, 0) = 0;

            execute format('set application_name = %L', app||'>old'||rec.prt);
            exe = format($sql$
                insert into s_grnplm_vd_hr_edp_dia.tmp_%1$s
                select a.* from s_grnplm_vd_hr_edp_stg.tb_%1$s%2$s a
                inner join (select distinct %3$s from s_grnplm_vd_hr_edp_dia.tmp_%1$s%2$s) b on %4$s 
            $sql$, wf , rec.prt, sds, son);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe;

            get diagnostics rc = ROW_COUNT;
            ro = ro + rc;

            execute format('set application_name = %L', app||'>del'||rec.prt);
            -- execute format('truncate s_grnplm_vd_hr_edp_stg.tb_%1$s%2$s', wf, rec.prt);
            -- execute format('delete from s_grnplm_vd_hr_edp_stg.tb_%1$s%2$s where true', wf, rec.prt);
            -- execute format('delete from s_grnplm_vd_hr_edp_stg.tb_%1$s%2$s where %3$s in (select distinct %3$s from s_grnplm_vd_hr_edp_dia.tmp_%1$s%2$s)', wf, rec.prt, dst);
            exe = format('delete from s_grnplm_vd_hr_edp_stg.tb_%1$s%2$s a
                            using (select distinct %3$s from s_grnplm_vd_hr_edp_dia.tmp_%1$s%2$s) b
                            where a.%3$s = b.%3$s', wf, rec.prt, dst);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe;
            get diagnostics rc = ROW_COUNT;
            rd = rd + rc;
            
            if jno = '' then
                whr = format($sql$
                inner join (
                    select report_date, max(actual_date) actual_date
                    from s_grnplm_vd_hr_edp_dia.tmp_%1$s%2$s
                    group by 1
                ) c on a.report_date = c.report_date and a.actual_date = c.actual_date
                $sql$, wf, rec.prt);
            else 
                whr = jno;
            end if ;

            execute format('set application_name = %L', app||'>ins'||rec.prt);
            exe = format($sql$
                insert into s_grnplm_vd_hr_edp_stg.tb_%1$s
                select %2$s
                from (
                    select distinct on (%3$s) *
                    from s_grnplm_vd_hr_edp_dia.tmp_%1$s%6$s a
                    order by %3$s, actual_date desc, ctl_loading desc, load_date desc
                ) a
                inner join (
                    select %3$s, hash, min(mod_date) min_tmp
                    from s_grnplm_vd_hr_edp_dia.tmp_%1$s%6$s b
                    group by %3$s, hash
                ) b on %4$s and a.hash = b.hash
                %5$s
            $sql$, wf, sql, sds, son, whr, rec.prt);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            -- if wf='oss_sep' then return exe; end if;
            execute exe;

            get diagnostics rc = ROW_COUNT;
            ri = ri + rc;
            k = k + 1;
            -- execute format('set application_name = %L', app||'>anlz'||rec.prt);
            -- perform s_grnplm_vd_hr_edp_srv_wf.pr_analyze(format('s_grnplm_vd_hr_edp_stg.tb_%1$s%2$s', wf, rec.prt));
           
        end loop;

        -- execute format('set application_name = %L', app||'>skew');
        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_log_skew('s_grnplm_vd_hr_edp_stg.tb_'||wf); 
        -- execute format('set application_name = %L', app||'>analyze');
        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_analyze('s_grnplm_vd_hr_edp_stg.tb_'||wf);
        
        execute format('set application_name = %L', app||'>log');
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id,'s_grnplm_vd_hr_edp_stg.tb_'||wf, dst, 'load_date','actual_date'); 
        
        execute format('set application_name = %L', app);
        RETURN format('Ok %s %s (%s - %s) %s', wf, to_char(ri - rd, 'SGFM999,999,999,999,999,999'), min_dia, max_dia, k);

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
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_diatostg_v2(integer, text, text, text, text, text[], text) IS 'Партиционированная загрузка из DIA в STG с дедупликацией по хэшу и ключам распределения';

CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_stg2hist(wf text, key_date text DEFAULT NULL::text, not_hsh text[] DEFAULT '{}'::text[]) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    m_txt text;
    rn int8 = 0;
    ri int8 = 0;
    ro int8 = 0;
    sql text;
    hsh text;
    ord text;
    rec record;
    app text;
    exe text;
    log_id int4;
    db_key text;
    db_whr text;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    execute 'show application_name' into app;
    log_id = pr_Log_start(format('STG2HIST_%1s$s (pr_stg2hist_%1$s)', wf));
    begin
        key_date = coalesce(key_date, 'now()');
    
--        exe = format('truncate s_grnplm_vd_hr_edp_dia.tmp_%s_hist', wf);
        exe = format('lock s_grnplm_vd_hr_edp_stg.tb_%s_hist in EXCLUSIVE mode', wf);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe;

        db_key = pg_get_table_distributedby(format('s_grnplm_vd_hr_edp_stg.tb_%s', wf)::regclass::oid);
        db_key = substring(db_key  from 'DISTRIBUTED BY \((\w+)\)');
        db_whr = (select string_agg(format('a.%1$s is not distinct from b.%1$s', fl), ' and ') from  regexp_split_to_table(db_key, ',') fl);
    
        sql = '';
        hsh = $s$''$s$;
    
        for rec in (select a.attnum num, a.attname fld, format_type(a.atttypid, a.atttypmod) flf
                    from pg_catalog.pg_attribute a where a.attnum > 0 and not a.attisdropped 
                    and a.attrelid = format('s_grnplm_vd_hr_edp_stg.tb_%s', wf)::regclass::oid  order by a.attnum) loop

            continue when rec.fld in ('hash', 'mod_date');

--            if rec.fld not in ('load_date', 'actual_date', 'ctl_validfrom', 'ctl_loading', 'ctl_action') then
            if not (rec.fld = any(not_hsh) or rec.fld = key_date) then
                hsh = hsh || format($s$ || coalesce(%s::text, '') $s$, rec.fld);
            end if;

            sql = sql||', '||format($s$ a.%1$I $s$, rec.fld);
        
        end loop;

        hsh = format('md5(%s)::uuid', hsh);
    
    
        execute format('set application_name = %L', app||'>new');
        exe = format($sql$
            drop table if exists tmp;
            create temp table tmp 
            WITH (appendonly=true, orientation=row, compresstype=zstd,compresslevel=3)
            on commit drop as
                select %4$s as mod_date, %3$s as hash
                    %2$s 
                from s_grnplm_vd_hr_edp_stg.tb_%1$s a
            distributed by (%5$s)
        $sql$, wf, sql, hsh, key_date, db_key);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe;

        get diagnostics rn = ROW_COUNT;

        execute format('set application_name = %L', app||'>old');
        exe = format($sql$
            insert into tmp
            select a.* from s_grnplm_vd_hr_edp_stg.tb_%1$s_hist a
        $sql$, wf);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe;

        get diagnostics ro = ROW_COUNT;

        execute format('set application_name = %L', app||'>del');
        exe = format('truncate s_grnplm_vd_hr_edp_stg.tb_%s_hist', wf);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe;

        ord = (select string_agg(fl||' desc', ', ') from unnest(not_hsh) fl);
        ord = coalesce(', '||ord, '');

        execute format('set application_name = %L', app||'>ins');
        exe = format($sql$
            insert into s_grnplm_vd_hr_edp_stg.tb_%1$s_hist
            select b.min_tmp, a.hash
                %3$s
            from (
                select distinct on (%5$s, hash) *
                from tmp a
                order by %5$s, hash, %2$s desc %4$s
            ) a
            inner join (
                select %5$s, hash, min(mod_date) min_tmp
                from tmp b
                group by %5$s, hash
            ) b on %6$s and a.hash = b.hash
        $sql$, wf, key_date, sql, ord, db_key, db_whr);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        -- if wf='oss_sep' then return exe; end if;
        execute exe;

        get diagnostics ri = ROW_COUNT;

        
        execute format('set application_name = %L', app||'>log');
        perform pr_Log_end(log_id, format('s_grnplm_vd_hr_edp_stg.tb_%s_hist', wf), key_date, 'mod_date'); 
        
        execute format('set application_name = %L', app);
        RETURN format('Ok %s %s', wf, to_char(ri - ro, 'SGFM999,999,999,999,999,999'));

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

            perform pr_Log_error(log_id, e_txt, coalesce(nullif(e_detail,''),exe), e_hint, e_context) ; 
            
            execute format('set application_name = %L', app);
            return 'Error: '||e_txt;
         end;
    end;
end;

$body$
EXECUTE ON ANY;
	
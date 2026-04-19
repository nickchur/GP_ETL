CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_any2stg_v1(srs text, wf text, act_date text, ord_hsh text[], del_tbl boolean) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    txt text;
    rn int8 = 0;
    ri int8 = 0;
    ro int8 = 0;
    rd int8 = 0;
    
    tbl text;
    fld text;
    hsh text;
    ord text;
    rec record;
    app text;
    exe text;
    -- log_id int4;
    db_key text;
    db_whr text;
    
    -- del_tbl bool = true;
    has_hash bool = false;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    execute 'show application_name' into app;
    -- GET diagnostics txt = PG_CONTEXT;
    -- txt = substring(split_part(txt,'\n', 1) from 'PL/pgSQL function ([\w\.]+)\([ \w\,]*\) line \d+ at GET DIAGNOSTICS');
    -- txt = coalesce(nullif(split_part(txt,'.',2),''), split_part(txt,'.',1));
    -- log_id = pr_Log_start(format('DIA2HIST_%1$s (%2$s)', wf, txt));
    begin
        -- act_date = coalesce(act_date, 'now()');
        -- ord_hsh =  coalesce(ord_hsh, '{actual_date,report_date,ctl_loading,ctl_action,ctl_validfrom,ctl_validto,ctl_pa_loading,archive_name,file_name}'::text[]);
        if split_part(srs, '.', 1) <> srs and srs not like 's_grnplm_vd_hr_edp_%' then
            srs = concat('s_grnplm_vd_hr_edp_', srs);
        end if;
    
        if wf like 's_grnplm_vd_hr_edp_%' then
            tbl = wf;
        elsif split_part(wf, '.', 1) = wf then
            tbl = concat('s_grnplm_vd_hr_edp_stg.tb_', wf);
        else
            tbl = concat('s_grnplm_vd_hr_edp_', wf);
        end if;

        exe = format('lock %s in EXCLUSIVE mode', tbl); -- s_grnplm_vd_hr_edp_stg.tb_
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe;

        db_key = pg_get_table_distributedby(format('%s', tbl)::regclass::oid);  -- s_grnplm_vd_hr_edp_stg.tb_
        db_key = substring(db_key  from 'DISTRIBUTED BY \((\w+)\)');
        db_whr = (select string_agg(format('a.%1$s is not distinct from b.%1$s', fl), ' and ') from  regexp_split_to_table(db_key, ',') fl);
    
        ord = (select string_agg(flds||' desc', ', ' order by ordinality) from unnest(ord_hsh) with ordinality flds);
        ord = coalesce(', '||ord, '');

        fld = '';
        hsh = '';
    
        for rec in (
            select a.attnum num, a.attname fld, format_type(a.atttypid, a.atttypmod) flf
            from pg_catalog.pg_attribute a where a.attnum > 0 and not a.attisdropped 
            and a.attrelid = format('%s', tbl)::regclass::oid  order by a.attnum  -- s_grnplm_vd_hr_edp_stg.tb_
        ) loop
            
            if rec.fld = 'hash' then has_hash = true; end if;

            continue when rec.fld in ('hash', 'mod_date', 'load_date');
            fld = fld || ', ' || format('a.%I::%s', rec.fld, rec.flf);
        
            continue when rec.fld = any(ord_hsh);
            hsh = hsh || ', ' || format('a.%I::%s', rec.fld, rec.flf);
            
        end loop;

        hsh = format($srt$ md5(concat('' %s))::uuid $srt$, hsh);
        
        execute format('set application_name = %L', app||'>new');
        drop table if exists tmp_any;
        exe = format($sql$
            create temp table tmp_any 
            WITH (appendonly=true, orientation=row, compresstype=zstd,compresslevel=3)
            on commit drop as
                select now()::timestamp load_date
                    , %4$s as mod_date
                    %2$s 
                    , %3$s as hash
                from %1$s a
            distributed by (%5$s)
        $sql$, srs, fld, hsh, act_date, db_key);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe;
        get diagnostics rn = ROW_COUNT;
        raise info '%', to_char(rn, 'FM999,999,999,999,999,999');

        if rn = 0 then
            txt = format('No data in %s', srs);
            return txt;
        end if;
        
        if del_tbl is null then
            exe = format($sql$
                select count(1) from %1$s a
            $sql$, tbl); -- s_grnplm_vd_hr_edp_stg.tb_
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe into ro;
            raise info '%', to_char(ro, 'FM999,999,999,999,999,999');
            
            if coalesce(ro, 0) > rn * 10 then 
                del_tbl = true;
            else 
                del_tbl = false;
            end if;
        end if;
        raise info '%', del_tbl;
            
        
        if has_hash then hsh = 'a.hash'; end if;

        execute format('set application_name = %L', app||'>old');
        if del_tbl then
            exe = format($sql$
                insert into tmp_any
                select distinct a.load_date, a.mod_date  %2$s , %3$s as hash
                from %1$s a
                join tmp_any b on %3$s = b.hash and %4$s
            $sql$, tbl, fld, hsh, db_whr);  -- s_grnplm_vd_hr_edp_stg.tb_
        else 
            exe = format($sql$
                insert into tmp_any
                select a.load_date, a.mod_date  %2$s , %3$s as hash
                from %1$s a
            $sql$, tbl, fld, hsh); -- s_grnplm_vd_hr_edp_stg.tb_
        end if;
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe;
        get diagnostics ro = ROW_COUNT;
        raise info '%', to_char(ro, 'FM999,999,999,999,999,999');



        execute format('set application_name = %L', app||'>del');
        if del_tbl then
            exe = format($sql$
                delete from %1$s a
                using tmp_any b
                where %2$s = b.hash and %3$s
            $sql$, tbl, hsh, db_whr); -- s_grnplm_vd_hr_edp_stg.tb_
        else
            exe = format('truncate %s', tbl); -- s_grnplm_vd_hr_edp_stg.tb_
        end if;
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe;
        get diagnostics rd = ROW_COUNT;
        raise info '%', to_char(rd, 'FM999,999,999,999,999,999');

        
        if has_hash then 
            hsh = ', a.hash'; 
        else 
            hsh = '';
        end if;
        
        execute format('set application_name = %L', app||'>ins');
        exe = format($sql$
            insert into %1$s
            select a.load_date 
                , b.min_tmp as mod_date
                %2$s
                %3$s
            from (
                select distinct on (%4$s, hash) *
                from tmp_any a
                order by %4$s, hash %5$s
            ) a
            inner join (
                select %4$s, hash, min(mod_date) min_tmp
                from tmp_any b
                group by %4$s, hash
            ) b on a.hash = b.hash and %6$s
        $sql$, tbl, fld, hsh, db_key, ord, db_whr); -- s_grnplm_vd_hr_edp_stg.tb_
        --     1    2    3     4      5     6     
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe;
        get diagnostics ri = ROW_COUNT;
        raise info '%', to_char(ri, 'FM999,999,999,999,999,999');
        
        -- execute format('set application_name = %L', app||'>log');
        -- perform pr_Log_end(log_id, tbl, 'mod_date', 'load_date', act_date);  -- s_grnplm_vd_hr_edp_stg.tb_
        
        execute format('set application_name = %L', app);
        RETURN format('Ok %s %s (%s)', wf, to_char(ri - ro, 'SGFM999,999,999,999,999,999'), to_char(rn, 'FM999,999,999,999,999,999') );

    exception when OTHERS then
        declare
            e_detail text;
            e_hint text;
            e_context text;
        begin
            get stacked diagnostics e_detail  = PG_EXCEPTION_DETAIL;
            get stacked diagnostics e_hint    = PG_EXCEPTION_HINT;
            get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

            perform pr_Log_error(0, sqlerrm, exe, e_hint, e_context);
            -- return 'Error: '||sqlerrm;
            raise exception using ERRCODE = sqlstate, MESSAGE = sqlerrm, DETAIL = e_detail, HINT = e_hint;            
         end;
    end;
end;


$body$
EXECUTE ON ANY;
	
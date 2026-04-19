CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_smdtodia_v2(log_id integer, wf text, pst text, from_id integer DEFAULT NULL::integer) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    -- log_id integer;
    m_txt text;
    rc int8;
    max_id int8;
    max_ts timestamp;

--    wf text = 'oss_v2';
    sql text = '';
    rec record;
    dst text;
    app text;
    exe text;
begin 
    execute 'show application_name' into app;
     -- log_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start(format('SMDTOGP_%1$s (pr_smdtodia_%1$s)', wf));
     begin
        if coalesce(from_id, 0) >= 0 then
            exe = format('truncate s_grnplm_vd_hr_edp_dia.dia_%s', wf);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe;
        end if;
        
        if from_id is null then
            exe = format('select max(ctl_loading), max(ctl_validfrom) from s_grnplm_vd_hr_edp_stg.tb_%s', wf);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe into max_id, max_ts;
            max_id = coalesce(max_id, 0);
        elsif from_id = -1 then
            exe = format('select min(ctl_loading), max(ctl_validfrom) from s_grnplm_vd_hr_edp_stg.tb_%s', wf);
            raise info '%', clock_timestamp();
            raise info '%', exe;
            execute exe into max_id, max_ts;
            max_id = coalesce(max_id * -1, 0);
        elsif from_id < -1 then
            max_id = from_id * -1 - 1;
        else 
            max_id = from_id - 1;
        end if;

        for rec in (select a.attnum num
                        , a.attname fld_f, format_type(a.atttypid, a.atttypmod) flf_f
                        , b.attname fld_t, format_type(b.atttypid, b.atttypmod) flf_t
                    from pg_catalog.pg_attribute a 
                    inner join pg_catalog.pg_attribute b on a.attnum = b.attnum
                    where a.attnum > 0
                    and a.attrelid = format('s_grnplm_vd_hr_edp_dia.pxf_%s%s', wf, pst)::regclass::oid  
                    and b.attrelid = format('s_grnplm_vd_hr_edp_dia.dia_%s', wf)::regclass::oid  
                    order by 1) loop

            if rec.num <> 1 then
                sql = sql || ', ';
            end if;

            if rec.fld_f = 'ctl_loading' and from_id < 0 then
                sql = sql||format($$ (ctl_loading * -1)::%s as ctl_loading $$, rec.flf_t);
            -- elsif left(rec.fld_f, 4) = 'ctl_' then
            --     sql = sql||format($$ %I::%s $$, rec.fld_f, rec.flf_t);
            elsif rec.fld_f = 'hexportid' then
                sql = sql||format($$ to_timestamp(%I, 'yyyymmddhh24miss')::%s $$, rec.fld_f, rec.flf_t);
            elsif rec.fld_f = 'hexpperio' then
                sql = sql||format($$ to_date(right(%I, 8), 'yyyymmdd')::%s $$, rec.fld_f, rec.flf_t);
            elsif rec.flf_f = 'text' and rec.flf_t = 'date'then
                sql = sql||format($$ nullif(nullif(nullif(nullif(%I, ''), ' '), '00000000'), '0000-00-00')::%s $$, rec.fld_f, rec.flf_t);
            -- elsif rec.flf_f= 'text' and rec.flf_t in ('integer','bigint','int4','int8') then
--            elsif rec.fld_f= '0comp_code' then
--                sql = sql||format($$ s_grnplm_vd_hr_edp_srv_wf.try_cast2int(%I)::%s $$, rec.fld_f, rec.flf_t);
            elsif rec.flf_f = 'text' then
                sql = sql||format($$ nullif(nullif(%I, ''), ' ')::%s $$, rec.fld_f, rec.flf_t);
            else 
                sql = sql||format($$ %I::%s $$, rec.fld_f, rec.flf_t);
            end if;
        end loop;

        execute format('set application_name = %L', app||'>pxf'||pst);
        exe = format($sql$
            insert into s_grnplm_vd_hr_edp_dia.dia_%1$s
            select %2$s 
            from s_grnplm_vd_hr_edp_dia.pxf_%1$s%4$s
--            where ctl_loading > %3$s
            where ctl_loading >= %3$s
        $sql$, wf, sql, max_id, pst);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        -- execute exe;
        -- get diagnostics rc = ROW_COUNT;
        rc = pr_try_exe(exe, 300, 3);
        
        if rc = 0 then
            m_txt = format('No new data in %s%s (> %s %s)', wf, pst, max_ts, max_id);
            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id, m_txt);
            execute format('set application_name = %L', app);
            return m_txt;
        end if;

        -- execute format($$select substring(pg_get_partition_def('s_grnplm_vd_hr_edp_dia.dia_%s'::regclass::oid) from 'PARTITION BY RANGE\((\w+)\)' )$$, wf) into dst;

        -- execute format('set application_name = %L', app||'>anlz');
        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_analyze('s_grnplm_vd_hr_edp_dia.dia_'||wf);
        
        -- execute format('set application_name = %L', app||'>skew');
        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_log_skew('s_grnplm_vd_hr_edp_dia.dia_'||wf); 
        
        execute format('set application_name = %L', app||'>log');
        perform s_grnplm_vd_hr_edp_srv_wf.pr_log_end(log_id,'s_grnplm_vd_hr_edp_dia.dia_'||wf, null, 'now()','hexportid'); 
        
        execute format('set application_name = %L', app);
        RETURN format('Ok %s%s %s (> %s %s)', wf, pst, to_char(rc, 'FM999,999,999,999,999,999'), max_ts, max_id);

   exception when OTHERS then
        declare
            e_txt text;
            e_detail text;
            e_hint text;
            e_context text;
            e_tbl text;
            e_fld text;
        begin
            get stacked diagnostics e_txt = MESSAGE_TEXT;
            get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
            get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
            get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
            
            get stacked diagnostics e_tbl = TABLE_NAME;
            get stacked diagnostics e_fld = COLUMN_NAME;

            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id, e_txt, coalesce(nullif(e_detail,''), exe), coalesce(nullif(e_hint,''), format('%s(%s)', e_tbl, e_fld)), e_context) ; 
            execute format('set application_name = %L', app);
            return 'Error: '||e_txt;
         end;
     end;
end;

$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_smdtodia_v2(integer, text, text, integer) IS 'Загружает данные из PXF-источника в DIA-таблицу порциями с инкрементом по from_id (v2)';

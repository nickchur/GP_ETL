CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_smdtodia_v4(wf text, pst text, max_id bigint DEFAULT NULL::bigint) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    txt text = '';
    ps text;

    sql text;
    ins text;
    rec record;
    app text;
    exe text;

    rc int8 = 0;
    rh int8 = 0;
    frmt text;
begin 
    execute 'show application_name' into app;
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    pst = coalesce(nullif(pst, ''), ' ');

    if max_id is null then
        exe = format('select max(ctl_loading) from s_grnplm_vd_hr_edp_stg.tb_%s', wf);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        execute exe into max_id;
        max_id = coalesce(max_id, 0);
    end if;

    for rec in (
        select a.attnum num
            , a.attname fld_f, format_type(a.atttypid, a.atttypmod) flf_f
            , b.attname fld_t, format_type(b.atttypid, b.atttypmod) flf_t
        from pg_catalog.pg_attribute a 
        join pg_catalog.pg_attribute b on a.attnum = b.attnum and not b.attisdropped
            and b.attrelid = format('s_grnplm_vd_hr_edp_dia.dia_%s', wf)::regclass::oid  
        where a.attnum > 0 and not a.attisdropped 
            and a.attrelid = format('s_grnplm_vd_hr_edp_dia.pxf_%s%s', wf, trim(split_part(pst, ',', 1)))::regclass::oid  
        order by 1
    ) loop

        -- if sql <> '' then sql = sql || ', '; end if;

        -- if rec.fld_f = 'ctl_loading' and max_id < 0 then
        --     sql = sql||format($$ (ctl_loading * -1)::%s as ctl_loading $$, rec.flf_t);
        if rec.fld_f = 'hexportid' then
            txt = $$ to_timestamp(nullif(nullif(%I, ''), '0'), 'yyyymmddhh24miss') $$;
            -- txt = $$ (left(%1$I, 8)::date + right(%1$I, 8)::time) $$;
            -- txt = $$ (left(%1$I, 8) || ' ' || right(%1$I, 8)) $$;
        elsif rec.fld_f = 'hexpperio' and rec.flf_t = 'date' then
            txt = $$ to_date(right(%I, 8), 'yyyymmdd') $$;
        elsif rec.flf_f = 'text' and rec.flf_t = 'date'then
            txt = $$ nullif(nullif(nullif(nullif(%I, ''), ' '), '00000000'), '0000-00-00') $$;
        elsif rec.flf_f = 'text' and rec.flf_t in ('bool', 'boolean') then
            txt = $$ ((%1$I = 'X') or (%1$I = '1')) $$;
        elsif rec.flf_f = 'text' then
            txt = $$ nullif(nullif(%I, ''), ' ') $$;
        else 
            txt = $$ %I $$;
        end if;
        sql = concat(sql||', ', format(trim(txt)||'::%2$s as %3$I', rec.fld_f, rec.flf_t, rec.fld_t));
        ins = concat(ins||', ', format('%I', rec.fld_t));
    end loop;
    -- ins = '('||ins||')';

    txt = '';
    foreach ps in array(string_to_array(pst, ',')) loop
        ps = trim(ps);
        execute format('set application_name = %L', format('%s>pxf%s', app, ps));
        exe = format($sql$
            insert into s_grnplm_vd_hr_edp_dia.dia_%1$s
            ( %5$s )
            select %2$s 
            from s_grnplm_vd_hr_edp_dia.pxf_%1$s%4$s
            where ctl_loading > %3$s
        $sql$, wf, sql, max_id, ps, ins);
        raise info '%', clock_timestamp();
        raise info '%', exe;
        -- rc = pr_try_exe(exe, 150, 3);
        rc = pr_try_exe(exe, 15, 3);
        rh = rh + rc;
        -- txt = concat(txt, format('%s (%s) ', ps, to_char(rc, 'FM999,999,999,999,999,999')));
        txt = concat(txt, json_build_object(ps, to_char(rc, 'FM999,999,999,999,999,999'))::text);
    end loop;

    if rh = 0 then
        txt = format('No new data in %s %s (> %s)', wf, pst, max_id);
        execute format('set application_name = %L', app);
        return txt;
    end if;

    -- execute format('set application_name = %L', app||'>anlz');
    -- perform pr_analyze('s_grnplm_vd_hr_edp_dia.dia_'||wf);

    -- execute format('set application_name = %L', app||'>skew');
    -- perform pr_log_skew('s_grnplm_vd_hr_edp_dia.dia_'||wf); 

    -- txt = format('Ok %s %s %s(> %s)', wf, to_char(rh, 'FM999,999,999,999,999,999'), txt, max_id);
    txt = format('Ok %s %s(> %s)', wf, txt, max_id);
    execute format('set application_name = %L', app);
    RETURN txt;
-- exception when OTHERS then
--     declare 
--         e_txt text;
--         e_detail text;
--         e_hint text;
--         e_context text;
--     begin
--         get stacked diagnostics e_txt = MESSAGE_TEXT;
--         get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
--         get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
--         get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
        
--         raise exception using ERRCODE = sqlstate, MESSAGE = e_txt, DETAIL = e_detail, HINT = e_hint;
--     end;
end; 
$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_smdtodia_v4(text, text, bigint) IS 'Загружает данные из PXF-источника в DIA-таблицу с инкрементом по max_id (v4)';

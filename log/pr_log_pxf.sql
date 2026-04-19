CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_pxf() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    rec record;
    whr text;
    m_txt text;
    beg timestamp;
    new record;
    inf text;
    val text;
    kk int4 = 0;
    nn int4 = 0;
    ee int4 = 0;
    app text;
    new_pxf text;
    ll int4;
begin
    execute 'show application_name' into app;
    
    for rec in (select a.*, b.ts , b.val_new val
                from s_grnplm_vd_hr_edp_srv_wf.tb_pxf a 
                left join (
                    select distinct on (pxf, fld, flt) *
                    from s_grnplm_vd_hr_edp_srv_wf.tb_log_pxf
                    where val_new is not null
                    order by pxf, fld, flt, ts desc
                ) b on a.pxf=b.pxf and a.fld=b.fld and a.flt=b.flt
                where active 
                    -- and (b.ts < current_date or b.ts is null)
                    and (b.ts < now() - '12 hours'::interval or b.val_old is null or b.err is not null)
                order by ts) loop
        
        execute format('set application_name = %L', app||'>'||rec.pxf);
        kk = kk + 1;
        beg = clock_timestamp();
        m_txt = null;
        val = rec.val;
        inf = null;
        ll = 0;
        loop
            if val is null then 
                whr = '';
            else 
                whr = format('where %I > %L::%s ', rec.fld, val, rec.flt);
            end if;
            
            begin 
                ll = ll + 1;
                execute format('set application_name = %L', app||'>'||rec.pxf||' '||ll);
                execute format($sql$
                    select %2$I::%3$s::text val
                        , %5$s::text inf
                    from s_grnplm_vd_hr_edp_dia.%1$s 
                    %4$s limit 1
                $sql$, rec.pxf, rec.fld, rec.flt, whr, rec.inf)
                into new;
                
                exit when new.val is null;
                
                val = new.val;
                inf = new.inf;
                
            exception when OTHERS then
            -- exception when connection_exception then
                -- get stacked diagnostics m_txt = MESSAGE_TEXT;
                m_txt = format('%s (%s)', SQLERRM, SQLSTATE);
                ee = ee + 1;
                exit;
            end;
            
        end loop;
        
        if coalesce(val, '') <> coalesce(rec.val, '') then
            nn = nn + 1;
            new_pxf = coalesce(new_pxf||', ', '')||rec.pxf;
        end if;

        
        if (coalesce(val, '') <> coalesce(rec.val, '') or m_txt is not null) then
            insert into s_grnplm_vd_hr_edp_srv_wf.tb_log_pxf
            (ts, duration, pxf, fld, flt, val_old, val_new, info, err, loops)
            select clock_timestamp() ts
                , clock_timestamp() - beg as duration
                , rec.pxf pxf
                , rec.fld fld
                , rec.flt flt
                , rec.val val_old
                , val val_new
                , inf info
                , m_txt err
                , ll loops
            ;
        end if;

    end loop;
    
    return format('Ok pxf %s new %s err %s (%s)', kk, nn, ee, new_pxf);

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

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(0, e_txt, e_detail, e_hint, e_context) ; 
        return e_txt;
    end;
end;
$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_pxf() IS 'Обходит активные PXF-источники и фиксирует изменения максимального ключевого значения в tb_log_pxf';

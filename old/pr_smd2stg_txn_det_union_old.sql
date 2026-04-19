CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_smd2stg_txn_det_union_old(fd date DEFAULT NULL::date, td date DEFAULT NULL::date) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    log_id int;
    log_sub int;
    rc int8;
    m_txt text default '';
    part text;
--    ld timestamp;
    ld int4;
    dd date;
    nd date;
    sql text;
    rec record;
    -- td date;
    rd int8 default 0;
    ri int8 default 0;
    app text;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    execute 'show application_name' into app;
    log_id = pr_Log_start('SMDTOGP_txn_det_union');
    begin
        log_sub = pr_Log_start('SMDTOGP_txn_det_union/pxf2dia');
        truncate s_grnplm_vd_hr_edp_dia.dia_txn_det_union;
        
        if td is null then
            fd = date_trunc('month', coalesce(fd, current_date - 1))::date;
            td = date_trunc('month', fd + 31)::date - 1;
        end if;
        
        execute format('set application_name = %L', app||'>max');
        -- ld = (select max(ctl_validfrom) from s_grnplm_vd_hr_edp_stg.tb_txn_det_union where day_part >= fd and day_part <= td);
        -- ld = coalesce(ld, '1900-01-01'::timestamp);
        ld = (select max(ctl_loading) from s_grnplm_vd_hr_edp_stg.tb_txn_det_union where day_part >= fd and day_part <= td);
        ld = coalesce(ld, 0);
        
        -- nd = (select ctl_validfrom from s_grnplm_vd_hr_edp_dia.pxf_txn_det_union where ctl_validfrom > ld limit 1);
        -- if nd is null then
        --     m_txt = 'No new data. Max ctl_validfrom '||ld;
        --     log_id =  pr_Log_error(log_id, m_txt); 
        --     return m_txt;
        -- end if;

        execute format('set application_name = %L', app||'>pxf');
        execute format($sql$
            insert into s_grnplm_vd_hr_edp_dia.dia_txn_det_union
            select * from s_grnplm_vd_hr_edp_dia.pxf_txn_det_union
            --where ctl_validfrom > '%1$s'
            where ctl_loading > %1$s
               and day_part >= '%2$s' and day_part <= '%3$s'
        $sql$, ld::text, fd::text, td::text);

        get diagnostics rc = ROW_COUNT;
        if rc = 0 then
             -- m_txt = 'No new data. Max ctl_validfrom '||ld;
             m_txt = 'No new data. Max ctl_loading '||ld;
            log_id =  pr_Log_error(log_id, m_txt); 
            execute format('set application_name = %L', app);
            return m_txt;
        end if;

        log_sub = pr_Log_end(log_sub);
        log_sub = pr_Log_start('SMDTOGP_txn_det_union/dia2stg');
        
        select min(day_part), max(day_part) into rec from s_grnplm_vd_hr_edp_dia.dia_txn_det_union;

        for dd in (select generate_series(date_trunc('month', td), fd, '-1 month'::interval)::date) loop

            part = (select partitionschemaname||'.'||partitiontablename from pg_partitions 
                    where schemaname = 's_grnplm_vd_hr_edp_stg' and tablename = 'tb_txn_det_union'
                       and ''''||dd||'''::date' = partitionrangestart);
            if part is null then
                m_txt = 'Error: From date is out of range '||dd;
                log_id =  pr_Log_error(log_id, m_txt); 
                execute format('set application_name = %L', app);
                return m_txt;
            end if;
            
--             execute 'truncate s_grnplm_vd_hr_edp_dia.tmp_txn_det_union';

--             execute format('set application_name = %L', app||'>tmp1>'||dd);
--             execute format($sql$
--                 insert into s_grnplm_vd_hr_edp_dia.tmp_txn_det_union 
--                 select * from %1$s where day_part < '%2$s' or day_part > '%3$s'
--             $sql$, part, rec.min::text, rec.max::text);
--             get diagnostics rc = ROW_COUNT;
--             ri = ri + rc;

--             execute format($sql$
--                 select count(1) from %1$s where day_part >= '%2$s' and day_part <= '%3$s'
--             $sql$, part, rec.min::text, rec.max::text) into rc;
--             rd = rd + rc;
            
--             execute format('set application_name = %L', app||'>tmp2>'||dd);
--             execute 'truncate '||part;
--             execute 'insert into '||part||' select * from s_grnplm_vd_hr_edp_dia.tmp_txn_det_union';

            execute format('set application_name = %L', app||'>del>'||dd);
            execute format($sql$
                delete from %s where day_part >= %L and day_part <= %L
            $sql$, part, rec.min::text, rec.max::text);
            get diagnostics rc = ROW_COUNT;
            rd = rd + rc;

        end loop;

        sql = '';
        for rec in (select pa.attnum num, pa.attname fld, format_type(pa.atttypid, null) flt, format_type(pa.atttypid, pa.atttypmod) flf
            from pg_catalog.pg_attribute pa where pa.attrelid = ('s_grnplm_vd_hr_edp_stg.tb_txn_det_union')::regclass::oid and pa.attnum > 0 order by pa.attnum) loop
    
            if rec.num <> 1 then
                sql = sql||', ';
            end if;
            sql = sql||format($$%s::%s$$, rec.fld, rec.flf);
        end loop;

        execute format('set application_name = %L', app||'>stg');
        execute format($sql$
            insert into s_grnplm_vd_hr_edp_stg.tb_txn_det_union
            select %s from s_grnplm_vd_hr_edp_dia.dia_txn_det_union
            where day_part < ctl_validfrom
                and nullif(epk_id, -1) is not null
        $sql$,  sql);
        get diagnostics rc = ROW_COUNT;

        m_txt = 'Ok upd '||to_char(ri, 'FM999,999,999,999,999,999')||' + '||to_char(rc, 'FM999,999,999,999,999,999')||' - '||to_char(rd, 'FM999,999,999,999,999,999')
            ||' = '||to_char(ri+rc-rd, 'FM999,999,999,999,999,999')||' > '||fd||' < '||td;
        log_sub = pr_Log_end(log_sub);
        log_id = pr_Log_end(log_id,'s_grnplm_vd_hr_edp_dia.dia_txn_det_union', 'day_part', 'now()', 'ctl_validfrom');
        execute format('set application_name = %L', app);
        return m_txt; 
    
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
            
            log_id =  pr_Log_error(log_id, e_txt, e_detail, e_hint, e_context); 
            execute format('set application_name = %L', app);
            return 'Error: '||e_txt;
       end;
   end;
end; 
$body$
EXECUTE ON ANY;
	
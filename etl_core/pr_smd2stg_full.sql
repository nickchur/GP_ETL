CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_smd2stg_full(wf text, flt text DEFAULT NULL::text, p_date text DEFAULT NULL::text, l_date text DEFAULT NULL::text, k_date text DEFAULT NULL::text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    log_id int4;
    rc int8;
    m_txt text;
    t_txt text;
    tbl oid;
    rec record;
    sql text;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_Log_start('SMDTOGP_'||wf);
    begin
        execute format($sql$
            truncate s_grnplm_vd_hr_edp_dia.dia_%1$s
        $sql$, wf);

        execute format($sql$
            insert into s_grnplm_vd_hr_edp_dia.dia_%1$s select * from s_grnplm_vd_hr_edp_dia.pxf_%1$s
        $sql$, wf);

--      execute format($sql$
--          select count(1) from s_grnplm_vd_hr_edp_dia.dia_%1$s a
--      $sql$, wf) into rc;
--
--      if rc = 0 then
        get diagnostics rc = ROW_COUNT;
        if rc = 0 then
            m_txt = 'No data'; 
            log_id = pr_Log_error(log_id, m_txt);
            return m_txt;
        end if;

        sql = '';
        tbl = ('s_grnplm_vd_hr_edp_stg.tb_'||wf)::regclass::oid;
        for rec in (select pa.attnum num, pa.attname fld, format_type(pa.atttypid, null) flt, format_type(pa.atttypid, pa.atttypmod) flf
            from pg_catalog.pg_attribute pa where pa.attrelid = tbl and pa.attnum > 0 order by pa.attnum) loop
    
            if rec.num <> 1 then
                sql = sql||', ';
            end if;
            sql = sql||format($$%s::%s$$, rec.fld, rec.flf);
    --      raise info '% %::%', rec.num, rec.fld, rec.flf;     
        end loop;
    
        execute format($sql$
            select 1 from (select %2$s from s_grnplm_vd_hr_edp_dia.dia_%1$s  where %3$s) a
            where md5(a.*::text)::uuid not in (select md5(b.*::text)::uuid from s_grnplm_vd_hr_edp_stg.tb_%1$s b)
            limit 1
        $sql$, wf, sql, coalesce(flt, '1 = 1')) into rc;

        if coalesce(rc, 0) = 0  then
            m_txt = 'No New data'; 
--          log_id = pr_Log_error(log_id, m_txt);
            log_id = pr_log_action('end', m_txt, log_id);
            t_txt = pr_log_stat(log_id, 's_grnplm_vd_hr_edp_stg.tb_'||wf);
            return m_txt;
        end if;

        execute format($sql$
            truncate s_grnplm_vd_hr_edp_stg.tb_%1$s
        $sql$, wf);

        execute format($sql$
            insert into s_grnplm_vd_hr_edp_stg.tb_%1$s
            select %2$s from s_grnplm_vd_hr_edp_dia.dia_%1$s
            where %3$s
        $sql$, wf, sql, coalesce(flt, '1 = 1'));

        get diagnostics rc = ROW_COUNT;
        m_txt = pr_log_skew('s_grnplm_vd_hr_edp_stg.tb_'||wf);
        m_txt = 'Ok '||to_char(rc, 'FM999,999,999,999,999,999');
        log_id = pr_Log_end(log_id,'s_grnplm_vd_hr_edp_stg.tb_'||wf, p_date, l_date, k_date);
--        log_id = pr_log_action('end', m_txt, log_id);
--        t_txt = pr_log_stat(log_id, 's_grnplm_vd_hr_edp_stg.tb_'||wf);
        
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
            return e_txt;
       end;
   end;
end; 
$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_smd2stg_full(text, text, text, text, text) IS 'Полная перезагрузка из PXF через DIA в STG с проверкой наличия новых данных';

CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_smd2stg_servmgr(wf text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    max_loading int8;
    log_id int4;
    sub_id int4;
    rc int8;
    m_txt text;
    wf_name text = 'SMDTOGP_SERVMGR_'||upper(wf);
    sql text = '';
    rec record;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_Log_start(wf_name);
    begin
        begin
            sub_id = pr_Log_start(wf_name||'/pxf2dia');

            sql=format('truncate s_grnplm_vd_hr_edp_dia.dia_servmgr_%s', wf);
            execute sql;

            select distinct on (workflow) 
                (try_cast2json(message)->>'lid')::int8 as lid
                , (try_cast2json(message)->>'dt')::timestamp as vfr
            into rec from vw_log_workflow
            where end_action = 'end' and workflow = wf_name
            order by workflow, start_id desc;

            if rec is null then
                sql=format('select max(ctl_loading) lid, max(ctl_validfrom) vfr from s_grnplm_vd_hr_edp_stg.tb_servmgr_%s', wf);
                execute sql into rec;
            -- else
            --     max_loading = rec.lid;
            end if;
            max_loading = coalesce(rec.lid, -1);

            sql = format($sql$
                insert into s_grnplm_vd_hr_edp_dia.dia_servmgr_%1$s
                select * from s_grnplm_vd_hr_edp_dia.pxf_servmgr_%1$s
                where ctl_loading > %2$s
            $sql$, wf, max_loading);
            execute sql;
            get diagnostics rc = ROW_COUNT;

            if rc < 1 then
                m_txt = format('No new data %s >%s (%s)', wf, max_loading, rec.vfr); 
                log_id = pr_Log_error(log_id, m_txt);
                return m_txt;
            end if;

            -- sub_id = pr_Log_end(sub_id);
            sub_id = pr_Log_end(sub_id, 's_grnplm_vd_hr_edp_dia.dia_servmgr_'||wf, Null, 'now()', 'ctl_validfrom');
        end;
        
        begin
            sub_id = pr_Log_start(wf_name||'/dia2stg');

            sql = format($sql$
                insert into s_grnplm_vd_hr_edp_stg.tb_servmgr_%1$s
                SELECT now()::timestamp as load_date, * 
                FROM s_grnplm_vd_hr_edp_dia.dia_servmgr_%1$s
            $sql$, wf);
            execute sql;
            get diagnostics rc = ROW_COUNT;

            sql = format($sql$
                select max(ctl_loading) lid, max(ctl_validfrom) vfr from s_grnplm_vd_hr_edp_dia.pxf_servmgr_%1$s
            $sql$, wf);
            execute sql into rec;

            -- m_txt = pr_log_skew('s_grnplm_vd_hr_edp_stg.tb_servmgr_'||wf);
            -- sub_id = pr_Log_end(sub_id);
            sub_id = pr_Log_end(sub_id, 's_grnplm_vd_hr_edp_stg.tb_servmgr_'||wf, null, 'now()', 'ctl_validfrom');
        end;
        -- log_id = pr_Log_end(log_id, 's_grnplm_vd_hr_edp_stg.tb_servmgr_'||wf, Null, 'load_date', 'ctl_validfrom');
        log_id = pr_log_action('end', json_build_object('rc', to_char(rc, 'FM999,999,999,999,999,999'), 'lid', rec.lid, 'dt', rec.vfr)::text, log_id);
                   
        -- return 'Ok '||wf||' '||to_char(rc, 'FM999,999,999,999,999,999')||' > '||max_loading;
        return format('Ok %s +%s >%s <=%s (%s)', wf, to_char(rc, 'FM999,999,999,999,999,999'), max_loading, rec.lid, rec.vfr);

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
           
            log_id =  pr_Log_error(log_id, e_txt, coalesce(e_detail, sql), e_hint, e_context); 
            return 'Error: '||e_txt;
        end;
    end;
end; 

$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_smd2stg_servmgr(text) IS 'Полная перезагрузка данных ServiceManager из PXF через DIA в STG';

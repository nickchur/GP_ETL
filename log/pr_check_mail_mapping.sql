CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_check_mail_mapping() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    log_id int;
    m_txt text;
    e_detail text;
    e_hint text;
    e_context text;
    rw_cnt int8;
    rec record;
begin
    log_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('CHECK_mail_mapping'); --Log
    begin
    
    --Проверка на дубли на критические даты
        select a.dt, b.mail_id, count(1)
        into rec
        from (
               select distinct bgn_date as dt from s_grnplm_vd_hr_edp_stg.tb_mail_mapping
               union select distinct end_date from s_grnplm_vd_hr_edp_stg.tb_mail_mapping
               union select current_date
               ) a
        inner join s_grnplm_vd_hr_edp_stg.vw_mail_mapping b on 
        -- inner join s_grnplm_vd_hr_edp_stg.tb_mail_mapping b on 
        -- inner join s_grnplm_vd_hr_edp_stg.tb_mail_mapping_ad b on 
               a.dt between coalesce (b.bgn_date, '2000-01-01') and coalesce (b.end_date, current_date)
        group by 1,2
        having count(1) > 1
        limit 1;

        get diagnostics rw_cnt = ROW_COUNT;
        
        if rw_cnt = 0 then
            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id); --ЛОГИРОВАНИЕ
            return 'Ok '||row_to_json(rec)::text;
        else 
            m_txt = row_to_json(rec)::text; 
            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id, m_txt); --ЛОГИРОВАНИЕ
            return 'Error: '||m_txt;
        end if;

    exception when OTHERS then
        get stacked diagnostics m_txt = MESSAGE_TEXT;
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
        get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
        get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
        
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id, m_txt, e_detail, e_hint, e_context) ; --Log
        return 'Error: '||m_txt;
    end;
end;
$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_check_mail_mapping() IS 'Проверяет маппинг почты на наличие дублей на критических датах';

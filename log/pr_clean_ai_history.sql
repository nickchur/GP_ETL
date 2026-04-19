CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_clean_ai_history() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    dd date;
    min_d date;
    log_id int;
    m_txt text; 
    e_detail text;
    e_hint text;
    e_context text;
    rc int8;
    rw_cnt int8;
begin
    log_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_clean_ai_history');   --Р›РћР“Р�Р РћР’РђРќР�Р•
    begin
        rw_cnt = 0;
        for dd in (select generate_series('2021-12-11',current_date-2,'1 day')::date) loop
            raise info '%',dd;
            insert into s_grnplm_vd_hr_edp_dia.tb_ai_history_dub
            SELECT h2.*
            FROM s_grnplm_vd_hr_edp_stg.tb_ai_history h2
            inner join (
                select md5(h1.action_type::text||h1.sessionid::text||h1.messageid::text||h1.timecreated::text
                   ||coalesce(h1.contents_uuid_userid,'')||coalesce(h1.contents_uuid_userchannel,'')||coalesce(h1.contents_messagename,''))
                ,count(1),max(id) id--,max(data_date)
                FROM s_grnplm_vd_hr_edp_stg.tb_ai_history h1
                where h1.data_date = dd or h1.data_date = dd - 1
                group by 1
                having count(1)>1
            ) h3 on h2.id=h3.id
            where h2.data_date = dd or h2.data_date = dd - 1
            ;

            -- delete from s_grnplm_vd_hr_edp_stg.tb_ai_history 
            -- where (data_date = dd or data_date = dd + 1)
            -- and id in (
            --     SELECT id
            --     FROM (
            --         select md5(h1.action_type::text||h1.sessionid::text||h1.messageid::text||h1.timecreated::text
            --            ||coalesce(h1.contents_uuid_userid,'')||coalesce(h1.contents_uuid_userchannel,'')||coalesce(h1.contents_messagename,''))
            --         ,count(1),max(id) id--,max(data_date)
            --         FROM s_grnplm_vd_hr_edp_stg.tb_ai_history h1
            --         where h1.data_date = dd or h1.data_date = dd + 1
            --         group by 1
            --         having count(1)>1
            --     ) h3 
            --     );
            get diagnostics rc = ROW_COUNT;
            rw_cnt = rw_cnt + rc;
        end loop;

       delete from s_grnplm_vd_hr_edp_stg.tb_ai_history_request_response where request_id in (select id from s_grnplm_vd_hr_edp_dia.tb_ai_history_dub);

       delete from s_grnplm_vd_hr_edp_stg.tb_ai_history_request_response where response_id in (select id from s_grnplm_vd_hr_edp_dia.tb_ai_history_dub);

       delete from s_grnplm_vd_hr_edp_stg.tb_ai_history_i_pernr where ai_id in (select id from s_grnplm_vd_hr_edp_dia.tb_ai_history_dub);

       delete from s_grnplm_vd_hr_edp_stg.tb_ai_history_historymeta where ai_id in (select id from s_grnplm_vd_hr_edp_dia.tb_ai_history_dub);

       delete from s_grnplm_vd_hr_edp_stg.tb_ai_history_payload where ai_id in (select id from s_grnplm_vd_hr_edp_dia.tb_ai_history_dub);

       delete from s_grnplm_vd_hr_edp_stg.tb_ai_history where id in (select id from s_grnplm_vd_hr_edp_dia.tb_ai_history_dub);
       get diagnostics rc = ROW_COUNT;
       m_txt = 'Ok '||to_char(rc,'FM999,999,999,999,999,999,999')||' ('||to_char(rw_cnt,'FM999,999,999,999,999,999,999')||')';
        -- m_txt = 'Ok '||to_char(rw_cnt,'FM999,999,999,999,999,999,999');

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_action('end', m_txt, log_id);
        return m_txt;

    exception when OTHERS then
        get stacked diagnostics m_txt = MESSAGE_TEXT;
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
        get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
        get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt,e_detail,e_hint,e_context);

        RETURN m_txt;
    end; 
end;
$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_clean_ai_history() IS 'Удаляет дублирующиеся записи из tb_ai_history и связанных таблиц за всё время';

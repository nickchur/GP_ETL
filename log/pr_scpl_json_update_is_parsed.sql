CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_scpl_json_update_is_parsed() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
declare
	log_id integer;
	m_txt text;
	e_detail text;
	e_hint text;
	e_context text;
	rc int8;
	rn int8 = 0;
begin
    log_id := s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('FILE_TO_GP_SCPL_JSON (pr_scpl_json_update_is_parsed)');

    begin

        -- Обновление is_parsed в tb_scpl_json_schemas
        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 1
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_agent_services_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 2
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_agents_change_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;
            
        with tbl as (
            select distinct file_name, hash
            from s_grnplm_vd_hr_edp_stg.tb_scpl_bulk_agent_services_event
        )
        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 3
        set is_parsed = 1
        from tbl tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 4
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_capacity_change_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 5
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_content_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 6
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_finish_interaction_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 7
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_interaction_change_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 8
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_mass_attempts_result tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 9
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_party_list_change_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 10
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_records_data tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 11
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_service_data_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 12
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_services_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 13
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_start_interaction_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 14
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_transcription_phrase_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 15
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_update_platform_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 16
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_workflow_process_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 17
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_workitem_state_change_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;

        update s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas js -- 18
        set is_parsed = 1
        from s_grnplm_vd_hr_edp_stg.tb_scpl_sms_gateway_event tb
        where tb.hash = js.hash 
            and tb.file_name = js.file_name;
        -----------------------------------------------------------------------

        -- Обновление is_parsed в tb_scpl_json
        with tbl as (
            select distinct on (file_name) 
                file_name, is_parsed
            from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas
            order by file_name, coalesce(is_parsed, 0)
        ) 
        update s_grnplm_vd_hr_edp_stg.tb_scpl_json j
        set is_parsed = 1::bit
        from tbl t 
        where j.file_name = t.file_name 
        and t.is_parsed = 1;

    -- Логирование
    perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id,'s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas',null,null,'load_date'); --ЛОГИРОВАНИЕ
    return 'OK';
 
	exception when OTHERS then
        get stacked diagnostics m_txt = MESSAGE_TEXT;
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
        get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
        get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
 
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt,e_detail,e_hint,e_context) ; --ЛОГИРОВАНИЕ
        return m_txt;
    end;
end;


$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_scpl_json_update_is_parsed() IS 'Обновляет флаг is_parsed в tb_scpl_json_schemas и tb_scpl_json по факту наличия записей в целевых таблицах';

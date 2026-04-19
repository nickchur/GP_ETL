CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_scpl_json_parse() 
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
    log_id := s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('FILE_TO_GP_SCPL_JSON (pr_scpl_json_parse)');
    begin

        -- newcall_events -- 1
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_start_interaction_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            interaction_id,
            message_id,
            channel,
            direction,
            tenant_id,
            callback,
            message_serial_number,
            "mode",
            moc_service_id,
            chat_client_id,
            chat_client_name,
            primary_calling_number,
            primary_destination_number,
            global_interaction_id
        )
        select
            load_date,
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as "timestamp",
            payload_json ->> 'interaction_id' as interaction_id,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'channel' as channel,
            payload_json ->> 'direction' as direction,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'callback' as callback,
            (payload_json ->> 'message_serial_number')::int8 as message_serial_number,
            payload_json ->> 'mode' as "mode",
            payload_json ->> 'moc_service_id' as moc_service_id,
            payload_json ->> 'chat_client_id' as chat_client_id,
            payload_json ->> 'chat_client_name' as chat_client_name,
            payload_json ->> 'primary_calling_number' as primary_calling_number,
            payload_json ->> 'primary_destination_number' as primary_destination_number,
            payload_json ->> 'global_interaction_id' as global_interaction_id
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'newcall_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- partylist_events -- 2
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_party_list_change_event(
            load_date,
            file_name,
            hash,
            "timestamp",
            workitem_id,
            interaction_id,
            tenant_id,
            division_id,
            division_name,
            full_division_names,
            channel,
            direction,
            party_info,
            message_id,
            message_serial_number
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'workitem_id' as workitem_id,
            payload_json ->> 'interaction_id' as interaction_id,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'division_name' as division_name,
            payload_json ->> 'full_division_names' as full_division_names,
            payload_json ->> 'channel' as channel,
            payload_json ->> 'direction' as direction,
            payload_json ->> 'party_info' as party_info,
            payload_json ->> 'message_id' as message_id,
            (payload_json ->> 'message_serial_number')::int8 as message_serial_number
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'partylist_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- agent_events -- 3
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_agents_change_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            user_id,
            last_name,
            first_name,
            patronymic,
            employee_id,
            tenant_id,
            division_id,
            division_name,
            full_division_names,
            user_state,
            reason_code,
            reason_code_name,
            message_id,
            next_agent_state,
            message_serial_number,
            "mode",
            previous_mode,
            computer_ip,
            agent_operation_mode,
            phone_set_number,
            moc_default_srv_id,
            moc_default_srv_name,
            personal_number,
            login
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as "timestamp",
            payload_json ->> 'user_id' as user_id,
            payload_json ->> 'last_name' as last_name,
            payload_json ->> 'first_name' as first_name,
            payload_json ->> 'patronymic' as patronymic,
            payload_json ->> 'employee_id' as employee_id,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'division_name' as division_name,
            payload_json ->> 'full_division_names' as full_division_names,
            payload_json ->> 'user_state' as user_state,
            payload_json ->> 'reason_code' as reason_code,
            payload_json ->> 'reason_code_name' as reason_code_name,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'next_agent_state' as next_agent_state,
            (payload_json ->> 'message_serial_number')::int8 as message_serial_number,
            payload_json ->> 'mode' as "mode",
            payload_json ->> 'previous_mode' as previous_mode,
            payload_json ->> 'computer_ip' as computer_ip,
            payload_json ->> 'agent_operation_mode' as agent_operation_mode,
            payload_json ->> 'phone_set_number' as phone_set_number,
            payload_json ->> 'moc_default_srv_id' as moc_default_srv_id,
            payload_json ->> 'moc_default_srv_name' as moc_default_srv_name,
            payload_json ->> 'personal_number' as personal_number,
            payload_json ->> 'login' as login
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'agent_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'workitem_events' -- 4
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_workitem_state_change_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            user_id,
            last_name,
            first_name,
            patronymic,
            employee_id,
            workitem_id,
            workitem_status,
            disposition_code,
            disposition_code_name,
            interaction_id,
            service_id,
            service_name,
            tenant_id,
            division_id,
            division_name,
            full_division_names,
            message_id,
            direction,
            channel,
            transfer,
            transfer_type,
            transfer_to,
            primary_calling_number,
            primary_destination_number,
            callback,
            destination,
            callback_accept,
            mute,
            message_serial_number,
            "mode",
            disposition_code_id,
            disposition_code_kind,
            ringing_timestamp,
            inprogress_timestamp,
            internal,
            service_number,
            first_transition,
            dialing_mode,
            parent_interaction,
            parent_workitem,
            agent_operation_mode,
            phone_set_number,
            redirection,
            redirection_number,
            dialed_number,
            is_remote,
            queue_entry_id,
            service_kind,
            service_blended,
            personal_number,
            srv_division_id,
            login,
            global_interaction_id
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as "timestamp",
            payload_json ->> 'user_id' as user_id,
            payload_json ->> 'last_name' as last_name,
            payload_json ->> 'first_name' as first_name,
            payload_json ->> 'patronymic' as patronymic,
            payload_json ->> 'employee_id' as employee_id,
            payload_json ->> 'workitem_id' as workitem_id,
            payload_json ->> 'workitem_status' as workitem_status,
            payload_json ->> 'disposition_code' as disposition_code,
            payload_json ->> 'disposition_code_name' as disposition_code_name,
            payload_json ->> 'interaction_id' as interaction_id,
            payload_json ->> 'service_id' as service_id,
            payload_json ->> 'service_name' as service_name,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'division_name' as division_name,
            payload_json ->> 'full_division_names' as full_division_names,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'direction' as direction,
            payload_json ->> 'channel' as channel,
            payload_json ->> 'transfer' as transfer,
            payload_json ->> 'transfer_type' as transfer_type,
            payload_json ->> 'transfer_to' as transfer_to,
            payload_json ->> 'primary_calling_number' as primary_calling_number,
            payload_json ->> 'primary_destination_number' as primary_destination_number,
            payload_json ->> 'callback' as callback,
            payload_json ->> 'destination' as destination,
            payload_json ->> 'callback_accept' as callback_accept,
            payload_json ->> 'mute' as mute,
            (payload_json ->> 'message_serial_number')::int8 as message_serial_number,
            payload_json ->> 'mode' as "mode",
            payload_json ->> 'disposition_code_id' as disposition_code_id,
            payload_json ->> 'disposition_code_kind' as disposition_code_kind,
            payload_json ->> 'ringing_timestamp' as ringing_timestamp,
            payload_json ->> 'inprogress_timestamp' as inprogress_timestamp,
            payload_json ->> 'internal' as internal,
            payload_json ->> 'service_number' as service_number,
            payload_json ->> 'first_transition' as first_transition,
            payload_json ->> 'dialing_mode' as dialing_mode,
            payload_json ->> 'parent_interaction' as parent_interaction,
            payload_json ->> 'parent_workitem' as parent_workitem,
            payload_json ->> 'agent_operation_mode' as agent_operation_mode,
            payload_json ->> 'phone_set_number' as phone_set_number,
            payload_json ->> 'redirection' as redirection,
            payload_json ->> 'redirection_number' as redirection_number,
            payload_json ->> 'dialed_number' as dialed_number,
            payload_json ->> 'is_remote' as is_remote,
            payload_json ->> 'queue_entry_id' as queue_entry_id,
            payload_json ->> 'service_kind' as service_kind,
            payload_json ->> 'service_blended' as service_blended,
            payload_json ->> 'personal_number' as personal_number,
            payload_json ->> 'srv_division_id' as srv_division_id,
            payload_json ->> 'login' as login,
            payload_json ->> 'global_interaction_id' as global_interaction_id
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'workitem_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'interactionrouter_events' then -- 5
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_interaction_change_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            interaction_id,
            service_id,
            service_name,
            channel,
            tenant_id,
            division_id,
            division_name,
            full_division_names,
            message_id,
            callback,
            message_serial_number,
            queue_entry_id,
            direction,
            "mode",
            service_blended,
            global_interaction_id
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'interaction_id' as interaction_id,
            payload_json ->> 'service_id' as service_id,
            payload_json ->> 'service_name' as service_name,
            payload_json ->> 'channel' as channel,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'division_name' as division_name,
            payload_json ->> 'full_division_names' as full_division_names,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'callback' as callback,
            (payload_json ->> 'message_serial_number')::int8 as message_serial_number,
            payload_json ->> 'queue_entry_id' as queue_entry_id,
            payload_json ->> 'direction' as direction,
            payload_json ->> 'mode' as mode,
            payload_json ->> 'service_blended' as service_blended,
            payload_json ->> 'global_interaction_id' as global_interaction_id
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'interactionrouter_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'agentservices_events' then -- 6 
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_agent_services_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            user_id,
            last_name,
            first_name,
            patronymic,
            employee_id,
            tenant_id,
            division_id,
            division_name,
            full_division_names,
            service_id,
            "service_name",
            proficiency_level,
            message_id,
            message_serial_number,
            service_kind,
            connection_type,
            interaction_search_tactic
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'user_id' as user_id,
            payload_json ->> 'last_name' as last_name,
            payload_json ->> 'first_name' as first_name,
            payload_json ->> 'patronymic' as patronymic,
            payload_json ->> 'employee_id' as employee_id,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'division_name' as division_name,
            payload_json ->> 'full_division_names' as full_division_names,
            payload_json ->> 'service_id' as service_id,
            payload_json ->> 'service_name' as "service_name",
            (payload_json ->> 'proficiency_level')::int8 as proficiency_level,
            payload_json ->> 'message_id' as message_id,
            (payload_json ->> 'message_serial_number')::int8 as message_serial_number,
            payload_json ->> 'service_kind' as service_kind,
            payload_json ->> 'connection_type' as connection_type,
            payload_json ->> 'interaction_search_tactic' as interaction_search_tactic
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'agentservices_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'workflowprocess_events' -- 7
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_workflow_process_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            interaction_id,
            workflow_id,
            workflow_version_id,
            routing_point_id,
            routing_point_kind,
            channel,
            primary_calling_number,
            primary_destination_number,
            tenant_id,
            direction,
            message_id,
            reason,
            tags,
            routing_point_name,
            workflow_name,
            division_id,
            division_name,
            full_division_names,
            message_serial_number,
            transfer_number,
            workflow_item_id,
            tag_comment
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'interaction_id' as interaction_id,
            payload_json ->> 'workflow_id' as workflow_id,
            payload_json ->> 'workflow_version_id' as workflow_version_id,
            payload_json ->> 'routing_point_id' as routing_point_id,
            payload_json ->> 'routing_point_kind' as routing_point_kind,
            payload_json ->> 'channel' as channel,
            payload_json ->> 'primary_calling_number' as primary_calling_number,
            payload_json ->> 'primary_destination_number' as primary_destination_number,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'direction' as direction,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'reason' as reason,
            payload_json ->> 'tags' as tags,
            payload_json ->> 'routing_point_name' as routing_point_name,
            payload_json ->> 'workflow_name' as workflow_name,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'division_name' as division_name,
            payload_json ->> 'full_division_names' as full_division_names,
            (payload_json ->> 'message_serial_number')::int8 as message_serial_number,
            payload_json ->> 'transfer_number' as transfer_number,
            payload_json ->> 'workflow_item_id' as workflow_item_id,
            payload_json ->> 'tag_comment' as tag_comment
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'workflowprocess_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'finishcall_events' -- 8
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_finish_interaction_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            interaction_id,
            message_id,
            finish_reason,
            tenant_id,
            message_serial_number,
            "mode",
            sip_code
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'interaction_id' as interaction_id,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'finish_reason' as finish_reason,
            payload_json ->> 'tenant_id' as tenant_id,
            (payload_json ->> 'message_serial_number')::int8 as message_serial_number,
            payload_json ->> 'mode' as mode,
            payload_json ->> 'sip_code' as sip_code
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'finishcall_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'servicedata_events' -- 9
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_service_data_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            interaction_id,
            service_id,
            service_name,
            tenant_id,
            division_id,
            division_name,
            full_division_names,
            waiting_time,
            sl_sec_max,
            sl_target,
            treshhold,
            sl_entry,
            treshhold_entry,
            message_id,
            lostcall,
            message_serial_number,
            channel,
            service_number,
            queue_time,
            ringing_time
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> '"timestamp"' as "timestamp",
            payload_json ->> 'interaction_id' as interaction_id,
            payload_json ->> 'service_id' as service_id,
            payload_json ->> 'service_name' as service_name,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'division_name' as division_name,
            payload_json ->> 'full_division_names' as full_division_names,
            payload_json ->> 'waiting_time' as waiting_time,
            (payload_json ->> 'sl_sec_max')::int8 as sl_sec_max,
            (payload_json ->> 'sl_target')::int8 as sl_target,
            (payload_json ->> 'treshhold')::int8 as treshhold,
            payload_json ->> 'sl_entry' as sl_entry,
            payload_json ->> 'treshhold_entry' as treshhold_entry,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'lostcall' as lostcall,
            (payload_json ->> 'message_serial_number')::int8 as message_serial_number,
            payload_json ->> 'channel' as channel,
            payload_json ->> 'service_number' as service_number,
            payload_json ->> 'queue_time' as queue_time,
            payload_json ->> 'ringing_time' as ringing_time
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'servicedata_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'transcribe_events' -- 10
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_transcription_phrase_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            message_id,
            workitem_id,
            interaction_id,
            tenant_id,
            speaker_type,
            "text",
            normalized_text,
            "start",
            "end",
            emotions_positive,
            emotions_neutral,
            emotions_negative,
            model_name,
            model_version,
            server_version,
            insight_result,
            agent_workspace,
            word_alignments,
            processed_audio_start,
            processed_audio_end
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'workitem_id' as workitem_id,
            payload_json ->> 'interaction_id' as interaction_id,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'speaker_type' as speaker_type,
            payload_json ->> 'text' as text,
            payload_json ->> 'normalized_text' as normalized_text,
            payload_json ->> 'start' as start,
            payload_json ->> 'end' as end,
            (payload_json ->> 'emotions_positive')::numeric as emotions_positive,
            (payload_json ->> 'emotions_neutral')::numeric as emotions_neutral,
            (payload_json ->> 'emotions_negative')::numeric as emotions_negative,
            payload_json ->> 'model_name' as model_name,
            payload_json ->> 'model_version' as model_version,
            payload_json ->> 'server_version' as server_version,
            payload_json ->> 'insight_result' as insight_result,
            payload_json ->> 'agent_workspace' as agent_workspace,
            payload_json ->> 'word_alignments' as word_alignments,
            payload_json ->> 'processed_audio_start' as processed_audio_start,
            payload_json ->> 'processed_audio_end' as processed_audio_end
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'transcribe_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'capacitychange_events' -- 11
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_capacity_change_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            user_id,
            last_name,
            first_name,
            patronymic,
            employee_id,
            workitem_id,
            interaction_id,
            service_id,
            service_name,
            tenant_id,
            division_id,
            division_name,
            full_division_names,
            message_id,
            "active",
            avail_agent_capacity,
            message_serial_number
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'user_id' as user_id,
            payload_json ->> 'last_name' as last_name,
            payload_json ->> 'first_name' as first_name,
            payload_json ->> 'patronymic' as patronymic,
            payload_json ->> 'employee_id' as employee_id,
            payload_json ->> 'workitem_id' as workitem_id,
            payload_json ->> 'interaction_id' as interaction_id,
            payload_json ->> 'service_id' as service_id,
            payload_json ->> 'service_name' as service_name,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'division_name' as division_name,
            payload_json ->> 'full_division_names' as full_division_names,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'active' as active,
            (payload_json ->> 'avail_agent_capacity')::int8 as avail_agent_capacity,
            (payload_json ->> 'message_serial_number')::int8 as message_serial_number
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'capacitychange_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        --'services_events' -- 12
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_services_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            service_id,
            service_name,
            tenant_id,
            division_id,
            division_name,
            full_division_names,
            direction,
            sl_max_sec,
            sl_plan_perc,
            treshhold,
            service_kind,
            message_id,
            agent_rule,
            channel,
            service_number
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'service_id' as service_id,
            payload_json ->> 'service_name' as service_name,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'division_name' as division_name,
            payload_json ->> 'full_division_names' as full_division_names,
            payload_json ->> 'direction' as direction,
            (payload_json ->> 'sl_max_sec')::int8 as sl_max_sec,
            (payload_json ->> 'sl_plan_perc')::int8 as sl_plan_perc,
            (payload_json ->> 'treshhold')::int8 as treshhold,
            payload_json ->> 'service_kind' as service_kind,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'agent_rule' as agent_rule,
            payload_json ->> 'channel' as channel,
            payload_json ->> 'service_number' as service_number
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'services_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'records_events' -- 13
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_records_data(
            load_date,
            file_name,
            hash,
            event_type,
            message_id,
            interaction_id,
            workitem_id,
            segment_id,
            media_type,
            workflow_id,
            routing_point_id,
            "timestamp",
            segment_start_datetime,
            segment_end_datetime,
            service_id,
            division_id,
            division_name,
            full_division_names,
            routing_point_name,
            service_name,
            tenant_id,
            destroyed_workitem_id,
            destroyed_segment_id
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'interaction_id' as interaction_id,
            payload_json ->> 'workitem_id' as workitem_id,
            payload_json ->> 'segment_id' as segment_id,
            payload_json ->> 'media_type' as media_type,
            payload_json ->> 'workflow_id' as workflow_id,
            payload_json ->> 'routing_point_id' as routing_point_id,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'segment_start_datetime' as segment_start_datetime,
            payload_json ->> 'segment_end_datetime' as segment_end_datetime,
            payload_json ->> 'service_id' as service_id,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'division_name' as division_name,
            payload_json ->> 'full_division_names' as full_division_names,
            payload_json ->> 'routing_point_name' as routing_point_name,
            payload_json ->> 'service_name' as service_name,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'destroyed_workitem_id' as destroyed_workitem_id,
            payload_json ->> 'destroyed_segment_id' as destroyed_segment_id
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'records_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'content_events' -- 14
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_content_event(
            load_date,
            file_name,
            hash,
            "timestamp",
            message_id,
            interaction_id,
            event_type,
            content_id,
            sender_type,
            user_id,
            content_timestamp,
            content_text,
            content_type,
            workitem_id,
            segment_id,
            routing_point_id,
            workflow_id,
            service_id,
            client_id,
            client_name,
            client_token,
            client_url,
            custom_data,
            ext_messenger_id,
            client_email,
            client_phone_number,
            tenant_id,
            file_id,
            division_id,
            division_name,
            full_division_names,
            routing_point_name,
            service_name,
            last_name,
            first_name,
            patronymic,
            employee_id
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'interaction_id' as interaction_id,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'content_id' as content_id,
            payload_json ->> 'sender_type' as sender_type,
            payload_json ->> 'user_id' as user_id,
            payload_json ->> 'content_timestamp' as content_timestamp,
            payload_json ->> 'content_text' as content_text,
            payload_json ->> 'content_type' as content_type,
            payload_json ->> 'workitem_id' as workitem_id,
            payload_json ->> 'segment_id' as segment_id,
            payload_json ->> 'routing_point_id' as routing_point_id,
            payload_json ->> 'workflow_id' as workflow_id,
            payload_json ->> 'service_id' as service_id,
            payload_json ->> 'client_id' as client_id,
            payload_json ->> 'client_name' as client_name,
            payload_json ->> 'client_token' as client_token,
            payload_json ->> 'client_url' as client_url,
            payload_json ->> 'custom_data' as custom_data,
            payload_json ->> 'ext_messenger_id' as ext_messenger_id,
            payload_json ->> 'client_email' as client_email,
            payload_json ->> 'client_phone_number' as client_phone_number,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'file_id' as file_id,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'division_name' as division_name,
            payload_json ->> 'full_division_names' as full_division_names,
            payload_json ->> 'routing_point_name' as routing_point_name,
            payload_json ->> 'service_name' as service_name,
            payload_json ->> 'last_name' as last_name,
            payload_json ->> 'first_name' as first_name,
            payload_json ->> 'patronymic' as patronymic,
            payload_json ->> 'employee_id' as employee_id
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'content_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'bulkagentservices_events' -- 15
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_bulk_agent_services_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            tenant_id,
            division_id,
            division_name,
            full_division_names,
            service_id,
            service_name,
            message_id,
            service_kind,
            user_info,

            user_id,
            last_name,
            first_name,
            patronymic,
            employee_id,
            tenant_user_id,
            proficiency_level,
            connection_type,
            interaction_search_tactic
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'division_name' as division_name,
            payload_json ->> 'full_division_names' as full_division_names,
            payload_json ->> 'service_id' as service_id,
            payload_json ->> 'service_name' as service_name,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'service_kind' as service_kind,
            payload_json ->> 'user_info' as user_info,

            jsonb_array_elements(cast(payload_json ->> 'user_info' as jsonb)) ->> 'user_id' as user_id,
            jsonb_array_elements(cast(payload_json ->> 'user_info' as jsonb)) ->> 'last_name' as last_name,
            jsonb_array_elements(cast(payload_json ->> 'user_info' as jsonb)) ->> 'first_name' as first_name,
            jsonb_array_elements(cast(payload_json ->> 'user_info' as jsonb)) ->> 'patronymic' as patronymic,
            jsonb_array_elements(cast(payload_json ->> 'user_info' as jsonb)) ->> 'employee_id' as employee_id,
            jsonb_array_elements(cast(payload_json ->> 'user_info' as jsonb)) ->> 'tenant_user_id' as tenant_user_id,
            jsonb_array_elements(cast(payload_json ->> 'user_info' as jsonb)) ->> 'proficiency_level' as proficiency_level,
            jsonb_array_elements(cast(payload_json ->> 'user_info' as jsonb)) ->> 'connection_type' as connection_type,
            jsonb_array_elements(cast(payload_json ->> 'user_info' as jsonb)) ->> 'interaction_search_tactic' as interaction_search_tactic
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'bulkagentservices_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'massattempts_events' -- 16
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_mass_attempts_result(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            message_id,
            tenant_id,
            campaign_id,
            campaign_name,
            division_id,
            job_id,
            list_id,
            list_name,
            interaction_id,
            client_id,
            completion_code_id,
            completion_code_name,
            phone,
            phone_field,
            phone_tz,
            ani,
            agent_id,
            agent_name,
            "type",
            "attributes",
            result_type,
            reschedule_time,
            attempt_count,
            start_time,
            ringing_time,
            connect_time,
            queue_time,
            agent_connect_time,
            disconnect_time,
            end_time,
            employee_id,
            workitem_id,
            service_id,
            service_name,
            uvsk
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'campaign_id' as campaign_id,
            payload_json ->> 'campaign_name' as campaign_name,
            payload_json ->> 'division_id' as division_id,
            payload_json ->> 'job_id' as job_id,
            payload_json ->> 'list_id' as list_id,
            payload_json ->> 'list_name' as list_name,
            payload_json ->> 'interaction_id' as interaction_id,
            payload_json ->> 'client_id' as client_id,
            payload_json ->> 'completion_code_id' as completion_code_id,
            payload_json ->> 'completion_code_name' as completion_code_name,
            payload_json ->> 'phone' as phone,
            payload_json ->> 'phone_field' as phone_field,
            payload_json ->> 'phone_tz' as phone_tz,
            payload_json ->> 'ani' as ani,
            payload_json ->> 'agent_id' as agent_id,
            payload_json ->> 'agent_name' as agent_name,
            payload_json ->> 'type' as type,
            payload_json ->> 'attributes' as attributes,
            payload_json ->> 'result_type' as result_type,
            payload_json ->> 'reschedule_time' as reschedule_time,
            (payload_json ->> 'attempt_count')::int8 as attempt_count,
            payload_json ->> 'start_time' as start_time,
            payload_json ->> 'ringing_time' as ringing_time,
            payload_json ->> 'connect_time' as connect_time,
            payload_json ->> 'queue_time' as queue_time,
            payload_json ->> 'agent_connect_time' as agent_connect_time,
            payload_json ->> 'disconnect_time' as disconnect_time,
            payload_json ->> 'end_time' as end_time,
            payload_json ->> 'employee_id' as employee_id,
            payload_json ->> 'workitem_id' as workitem_id,
            payload_json ->> 'service_id' as service_id,
            payload_json ->> 'service_name' as service_name,
            payload_json ->> 'uvsk' as uvsk
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'massattempts_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;

        -- 'updateplatform_events' -- 17
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_update_platform_event(
            load_date,
            file_name,
            hash,
            event_type,
            "timestamp",
            tenant_id,
            message_id,
            tenant_name
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'event_type' as event_type,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'message_id' as message_id,
            payload_json ->> 'tenant_name' as tenant_name
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'updateplatform_events'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;
        
        -- 'smsgatewayevent' -- 18
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_sms_gateway_event(
            load_date,
            file_name,
            hash,
            "timestamp",
            sms_id,
            idempotency_key,
            msg,
            sent_at,
            from_number,
            "to_number",
            gateway_account_name,
            gateway_account_provider,
            tenant_id,
            message_id
        )
        select
            now(),
            file_name,
            hash,
            payload_json ->> 'timestamp' as timestamp,
            payload_json ->> 'sms_id' as sms_id,
            payload_json ->> 'idempotency_key' as idempotency_key,
            payload_json ->> 'msg' as msg,
            payload_json ->> 'sent_at' as sent_at,
            payload_json ->> 'from_number' as from_number,
            payload_json ->> 'to_number' as to_number,
            payload_json ->> 'gateway_account_name' as gateway_account_name,
            payload_json ->> 'gateway_account_provider' as gateway_account_provider,
            payload_json ->> 'tenant_id' as tenant_id,
            payload_json ->> 'message_id' as message_id
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas a
        where
            lower(a.schema_name) = 'smsgatewayevent'
            --and schema_version_md5 = ''::uuid -- прописать из s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings.schema_version_md5
            and coalesce(is_parsed, 0) = 0
        ;
    -------------------------------------------------------------------------------------------------------------------------------------
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
        -------------------------------------------------------------------------

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
	
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_scpl_schemas_parse() 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$


declare
    log_id int4;
    inf_id int4;
    rc int8;
    rd int8;
    m_txt text;
    rec record;
    e_txt text;
    e_detail text;
    e_hint text;
    e_context text;
begin
    log_id := s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('FILE_TO_GP_SCPL_JSON (pr_scpl_json_update_schemas_settings)');

    begin
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_json_schemas (
            load_date,
            file_name,
            schema_name,
            schema_version_md5,
            message_id,
            hash,
            payload_json
        )
        select distinct
            now() as load_date,
            file_name,
            lower(elem -> 'schema' ->> 'name') as schema_name,
            md5(elem ->> 'schema')::uuid as schema_version_md5,
            ((elem ->> 'payload')::jsonb ->> 'message_id') as message_id,
            md5( concat( ((elem ->> 'payload')::jsonb ->> 'message_id')::text, (elem ->> 'payload')::text )  )::uuid as hash,
            (elem ->> 'payload')::jsonb as payload_json
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json,
        lateral jsonb_array_elements(cast(json_text as jsonb)->'data') as elem
        where 1 = 1
            and coalesce(is_parsed, 0::bit) = 0::bit;

        -- Логирование
        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id,'s_grnplm_vd_hr_edp_stg.tb_scpl_json',null,null,'schema_actual_date_from'); --ЛОГИРОВАНИЕ
        return 'OK';

    exception when OTHERS then
        begin
            get stacked diagnostics e_txt = MESSAGE_TEXT;
            get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
            get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
            get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

            log_id =  pr_Log_error(log_id, e_txt, e_detail, e_hint, e_context);
            return 'Error: '||e_txt;
        end;
    end;
end;


$body$
EXECUTE ON ANY;
	
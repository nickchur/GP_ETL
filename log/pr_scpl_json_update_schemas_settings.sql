CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_scpl_json_update_schemas_settings() 
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
    log_id := s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('FILE_TO_GP_SCPL_JSON (pr_scpl_json_update_schemas_settings)');
    begin

		with tbl as (
            select *
            from s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings
        )
        insert into s_grnplm_vd_hr_edp_stg.tb_scpl_json_parse_settings (
            schema_name,
            schema_version_num,
            schema_version_md5,
            schema_json,
            schema_actual_date_from,
            schema_is_used
        )
        select distinct
			lower(elem -> 'schema' ->> 'name') as schema_name,
			t.schema_version_num + 1 as schema_version_num,
			md5(elem ->> 'schema')::uuid as schema_version_md5,
			(elem -> 'schema')::jsonb as schema_json,
			current_date as schema_actual_date_from,
            1::int as schema_is_used
        from s_grnplm_vd_hr_edp_stg.tb_scpl_json j,
            lateral jsonb_array_elements(cast(json_text as jsonb)->'data') as elem
        left join tbl t on t.schema_version_md5 = md5(elem ->> 'schema')::uuid
        where t.schema_name is null
        	and j.add_dtts = (select max(add_dtts) from s_grnplm_vd_hr_edp_stg.tb_scpl_json)
		;

    	-- Логирование
    	perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id,'s_grnplm_vd_hr_edp_stg.tb_scpl_json',null,null,'schema_actual_date_from'); --ЛОГИРОВАНИЕ
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
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_scpl_json_update_schemas_settings() IS 'Добавляет новые версии JSON-схем из tb_scpl_json в таблицу настроек tb_scpl_json_parse_settings';

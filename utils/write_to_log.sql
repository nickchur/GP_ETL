CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.write_to_log(vlog s_grnplm_vd_hr_edp_srv_wf.log_table_type[]) 
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
as $body$

	   declare
	       i int4;
       begin
	       for i in 1..array_length(vlog, 1) loop
		       insert into s_grnplm_vd_hr_edp_srv_wf.log_table(load_event_cd, load_event_ts, workflow_run_id, load_event_text, tgr_entity_cd)
		       select (t.a::s_grnplm_vd_hr_edp_srv_wf.log_table_type).* from (select vlog[i] a) as t ;  
	       end loop;
       end;

$body$
EXECUTE ON ANY;

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.write_to_log(s_grnplm_vd_hr_edp_srv_wf.log_table_type[]) IS 'Записывает массив событий в таблицу log_table';

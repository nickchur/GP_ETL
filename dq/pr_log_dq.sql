CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.pr_log_dq(action text, message json, lid integer DEFAULT NULL::integer, ts timestamp without time zone DEFAULT clock_timestamp()) 
	RETURNS integer
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    nid int4;
begin
    INSERT INTO s_grnplm_vd_hr_edp_srv_dq.tb_dq_log
    (parent, dq_action, dq_message, ts) values (nullif(lid, 0), action, message, ts)
    returning id into nid ;  

    return nid; 

end; 
$body$
EXECUTE ON ANY;
	
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_dq.pr_log_dq(text, json, integer, timestamp without time zone) IS 'Записывает действие DQ-проверки с JSON-сообщением в лог, возвращает идентификатор записи';

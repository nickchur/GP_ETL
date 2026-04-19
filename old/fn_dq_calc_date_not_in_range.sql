CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_date_not_in_range(table_name character varying, field_name character varying) 
	RETURNS integer
	LANGUAGE plpgsql
	VOLATILE
as $body$
	
declare 
	cnt_not_in_range int;
BEGIN	
	--параметры для вызова
	--table_name - таблица, для которой производится проверка
	--field_name - поле, содержащее проверяемую дату
	execute 'select count(*) from '|| table_name ||' where '|| field_name ||' not between ''19700101'' and ''21000101'';' into cnt_not_in_range;
	insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_date_not_in_range(table_name, field_name, cnt_not_in_range) values (table_name, field_name, cnt_not_in_range);
	return cnt_not_in_range;
end; 

$body$
EXECUTE ON ANY;
	
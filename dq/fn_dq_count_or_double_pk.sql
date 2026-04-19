CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_count_or_double_pk() 
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
       	start_t timestamp;
       	end_t timestamp;
       	my_row record;
       	count_rows smallint;
       	count_duplicate_pk smallint; -- 1, если есть задвоения для ключа из таблицы, иначе null
        
	begin 
		log_id := s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('Сollection of statistics');
			
		begin
			for my_row in
				select object_name, pk, max_count, date_name from s_grnplm_vd_hr_edp_stg.tb_checking_tables
			loop
				start_t := clock_timestamp();
			
				execute 'select 1 from (select ' || my_row.pk || ' from ' || my_row.object_name || ' group by ' || my_row.pk || ' having count(*) > 1) as t limit 1'
				into count_duplicate_pk;
			
				-- 1) Если есть отчетная дата с примерно фиксированным количеством строк за каждый день, то проверка будет в первом if (вывод в insert values OK или ERROR)
				-- 2) Если отчетной даты нет и данные постоянно перезаписываются (truncate), тогда будем определять общее количество строк и выводить их в итоговой таблице
				-- 3) Если п. 1 и 2 не подошли, тогда count_rows вообще не считаем, а в таблице выводим null
				if (my_row.date_name is not null) and (my_row.max_count is not null) then
					execute 'select 1 from (select ' || my_row.date_name || ' from ' || my_row.object_name || ' group by ' || my_row.date_name || ' having count(*) > ' || my_row.max_count || ') as t limit 1'
					into count_rows;
				elsif (my_row.date_name is null) and (my_row.max_count is not null) then
					execute 'select count(*) from ' || my_row.object_name
					into count_rows;
				else
					count_rows := -1;
				end if;
			
				end_t := clock_timestamp();
			
				insert into s_grnplm_vd_hr_edp_stg.tb_log_checking_tables (start_t, end_t, object_name, count_status, pk_status)
				values (
					start_t
					, end_t
					, my_row.object_name
					, case 
						when count_rows = -1
							then null
						when count_rows is null
							then 'OK'
						when count_rows = 1
							then 'ERROR'
						when count_rows < my_row.max_count 
							then 'OK (' || count_rows || ')'
						else
							'ERROR (' || count_rows || ')'
					end
					, case 
						when count_duplicate_pk is null
							then 'OK'
						else 
							'ERROR'
					end);
				
			end loop;
			
			perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id,'s_grnplm_vd_hr_edp_srv_wf.tb_log_checking_tables',null,null,null);
                
			RETURN 'OK';
		
		exception when OTHERS then
           	get stacked diagnostics m_txt = MESSAGE_TEXT;
           	get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
           	get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
          	get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
                
            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt,e_detail,e_hint,e_context);
            return m_txt;
				
		end;
	end;

$body$
EXECUTE ON ANY;
	
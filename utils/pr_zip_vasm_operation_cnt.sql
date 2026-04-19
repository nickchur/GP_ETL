CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_zip_vasm_operation_cnt() 
	RETURNS character varying
	LANGUAGE plpgsql
	VOLATILE
as $body$

				 	 	
	begin
		create temp table delta_vasm as (
			select * from (
				select date_report_src, count(*) d_cnt
				from s_grnplm_vd_hr_edp_stg.tbl_vasm_operation
				group by date_report_src
			) dst
			full join s_grnplm_vd_hr_edp_dia.vasm_operation_cnt as src 
				on src.date_month = dst.date_report_src
		);
	
		delete from s_grnplm_vd_hr_edp_dia.vasm_operation_cnt where 1 = 1;
	
		insert into s_grnplm_vd_hr_edp_dia.vasm_operation_cnt
		select coalesce(date_month, date_report_src), coalesce(cnt,0), coalesce(d_cnt,0) 
		from delta_vasm
		where (cnt - d_cnt != 0 
		and (cnt - d_cnt) is not null)
		or (d_cnt is null and cnt is not null)
		order by date_report_src desc;
		
		drop table delta_vasm;
		RETURN 's_grnplm_vd_hr_edp_dia.vasm_operation_cnt подготовлена';
	END;


$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_zip_vasm_operation_cnt() IS 'Пересчитывает счётчики операций ВАСМ по датам и обновляет таблицу vasm_operation_cnt';

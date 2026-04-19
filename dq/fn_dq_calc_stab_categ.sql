CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_stab_categ(src_table_name character varying, tgt_table_name character varying, field_name character varying, bench_dt_val date, tgt_dt_val date, rule_name character varying, src_date_field_name1 character varying, tgt_date_field_name1 character varying, bench_interval_type character varying, bench_interval_qty integer, tgt_interval_type character varying, tgt_interval_qty integer) 
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
declare
	delta_perc numeric(18,4) default 0;
	src_date_from date default '2017-06-01';
	src_date_to date default '2017-07-01';
	tgt_date_from date default '2020-08-01';
	tgt_date_to date default '2020-09-01';
	tbl_name character varying;
	t_tbl_name character varying;
begin
	tbl_name = src_table_name;

	drop table if exists vtgttab;
	drop table if exists vdisttab;
	drop table if exists res;
	
	src_date_from  = s_grnplm_vd_hr_edp_srv_dq.fn_dq_calculate_interval_dt_from(bench_dt_val,bench_interval_type,bench_interval_qty);
	tgt_date_from  = s_grnplm_vd_hr_edp_srv_dq.fn_dq_calculate_interval_dt_from(tgt_dt_val,tgt_interval_type,tgt_interval_qty);

execute 'create temp table res as
WITH bench AS (
SELECT 
	CAST('||field_name||' as VARCHAR(100)) AS val,
	COUNT(*) AS Qty,
	CAST(COUNT(*) AS FLOAT) /  SUM(COUNT(*)) OVER () AS Pct
		FROM '||tbl_name||'
		WHERE  '||src_date_field_name1||'::date BETWEEN $1 AND $2
		GROUP BY 1), 
		tgt AS (
		SELECT 
			CAST('||field_name||' as VARCHAR(100)) AS val,
			COUNT(*) AS Qty,
			CAST(COUNT(*) AS FLOAT) /  SUM(COUNT(*)) OVER () AS Pct
		FROM '||tbl_name||' 
		WHERE '||tgt_date_field_name1||'::date BETWEEN $3 AND $4
		GROUP BY 1
		)
		SELECT 
			-- COALESCE(bench.val, tgt.val, null) 	 CatValue
			/*NULL (float) AS LBound,
			NULL (float) AS UBound,*/
		--	,CAST(COALESCE(bench.Pct, 0) AS FLOAT) AS PctBench
			--,CAST(COALESCE(tgt.Pct, 0) AS FLOAT) AS PctTarget
			abs(sum(CAST((CAST(COALESCE(bench.Pct, 0) AS FLOAT) - CAST(COALESCE(tgt.Pct, 0) AS FLOAT)) * LN(COALESCE(NULLIF(CAST(COALESCE(bench.Pct, 0) AS FLOAT),0),1E-4)/COALESCE(NULLIF(CAST(COALESCE(tgt.Pct, 0) AS FLOAT),0),1E-4)) AS FLOAT))) AS PSIValue
			--,SBX_048_HRDLake_MAIN.GetPSIGroup(PSIValue) AS PSIGroup
		FROM bench FULL OUTER JOIN tgt ON (COALESCE(bench.val,''xxx'') = COALESCE(tgt.val,''xxx''))
		--GROUP BY GROUPING SETS((COALESCE(bench.val, tgt.val,''xxx''),CAST(COALESCE(bench.Pct, 0) AS FLOAT),CAST(COALESCE(tgt.Pct, 0) AS FLOAT)), ())
		--ORDER BY GROUPING(COALESCE(bench.val, tgt.val,''xxx'')) desc, CatValue
		--group by 1 '
		using src_date_from, bench_dt_val, tgt_date_from, tgt_dt_val;
	
	select psivalue into delta_perc from res;
	insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_stability values (rule_name, tbl_name, field_name, cast(tgt_dt_val as date), delta_perc, current_date);
	delete from s_grnplm_vd_hr_edp_srv_dq.tb_dq_stability where load_dt is null;
	
	
end; 

$body$
EXECUTE ON ANY;
	
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_stab_categ(character varying, character varying, character varying, date, date, character varying, character varying, character varying, character varying, integer, character varying, integer) IS 'Вычисляет стабильность категориального поля между эталонной и целевой выборками и сохраняет метрику PSI в DQ-лог';

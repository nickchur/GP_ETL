CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_stab_date(src_table_name character varying, tgt_table_name character varying, field_name character varying, bench_dt_val date, tgt_dt_val date, rule_name character varying, src_date_field_name character varying, tgt_date_field_name character varying, bench_interval_type character varying, bench_interval_qty integer, tgt_interval_type character varying, tgt_interval_qty integer) 
	RETURNS numeric
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
	
		
declare
	delta_perc numeric(18,4) default 0;
	src_date_from date;
	src_date_to date default '2017-07-01';
	tgt_date_from date default '2020-08-01';
	tgt_date_to date default '2020-09-01';
	tbl_name character varying;
	t_tbl_name character varying;
	zero_day date default '20000101';  -- нулевой день, для пересчета произвольной даты в количество прошедших дней 
	
	rec record;

	date_from date;
begin
	
	-- src_table_name  -- таблица, из которой берется базовый период (БП), обычно *_stg.tb_*
    -- tgt_table_name  -- таблица, из которой берется оцениваемый период (ОП), обычно *_dia.*
    -- field_name  -- имя поля для проверки качества данных (дата или дата с timestamp)
    -- bench_dt_val  -- дата окончания базового периода, bench_date из блока declare. 
    --                  Дата начала базового периода определяется по параметрам bench_dt_val - interval_type * interval_qty
    -- tgt_dt_val  -- дата за которую проводится оценка данных витрины, tgt_date из блока declare
    -- rule_name  -- название создаваемого правила ККД, правило составления см. выше
    -- src_date_field_name  -- поле даты, по которому определяется БП, обычно тоже, что используется для задания bench_date в блоке declare
    -- tgt_date_field_name  -- поле даты, по которому определяется БП, обычно совпадает с src_date_field_name1
    -- bench_interval_type  -- интервал ('year', 'quarter', 'month', 'week', 'day'), за который берутся данные БП, обычно используем 'month'
    -- bench_interval_qty  -- множитель интервала, обычно 1
    -- tgt_interval_type  -- интервал ('year', 'quarter', 'month', 'week', 'day') за который берутся данные ОП, обычно совпадает с bench_interval_type
    -- tgt_interval_qty  -- множитель интервала, обычно 1 и совпадает с bench_interval_qty

	
	tbl_name = src_table_name;

	-- определяем левую границу базового периода на основе заданной даты базового периода, интервала и множителя интервала
	src_date_from  = s_grnplm_vd_hr_edp_srv_dq.fn_dq_calculate_interval_dt_from(bench_dt_val,bench_interval_type,bench_interval_qty);
	tgt_date_from  = s_grnplm_vd_hr_edp_srv_dq.fn_dq_calculate_interval_dt_from(tgt_dt_val,tgt_interval_type,tgt_interval_qty);
	

	drop table if exists vtgttab;
	drop table if exists vdisttab;
	drop table if exists res;


	execute 'create temp table vdisttab as 
	SELECT 
		row_number() over() rn,
		decile,					
		lbound,
		CASE
			WHEN decile<= 0 THEN ubound
			ELSE COALESCE(MAX(lbound) OVER (ORDER BY decile ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),24)
		END AS ubound,
		Qty,
		SUM(Qty) OVER () AS TotQty,
		CAST(Qty AS FLOAT) / SUM(Qty) over() AS Pct
	FROM (
		SELECT
			decile  AS decile,
			(MIN(val) ::date - $3) ::float AS lbound,  -- макс. значение преобразую в дату, если вдруг timestamp
			(MAX(val) ::date - $3) ::float AS ubound,  -- и преобразую в количество дней после 1 янв 2000 г.
			COUNT(*) AS Qty
		FROM (	
			SELECT 
				'||field_name||' AS val,
				(rank() over (order by '||field_name||'))*10/count('||field_name||' ) over()  AS decile
			FROM '||tbl_name||' pr
			WHERE '||src_date_field_name||' BETWEEN $1 AND $2
		--	AND per_grade_num between 1 and 19									
			) x
		GROUP BY 1) d
		group by decile, lbound, ubound, qty'
		using   src_date_from, bench_dt_val, zero_day
	;
	execute 'create temp table vtgttab as 
	SELECT 
		row_number() over() rn,
		decile,					
		lbound,
		CASE
			WHEN decile<= 0 THEN ubound
			ELSE COALESCE(MAX(lbound) OVER (ORDER BY decile ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),24)
		END AS ubound,
		Qty,
		SUM(Qty) OVER () AS TotQty,
		CAST(Qty AS FLOAT) / SUM(Qty) over() AS Pct
	FROM (
		SELECT
			decile  AS decile,
			(MIN(val) ::date - $3) ::float AS lbound,
			(MAX(val) ::date - $3) ::float AS ubound,
			COUNT(*) AS Qty
		FROM (	
			SELECT 
				'||field_name||' AS val,
				(rank() over (order by '||field_name||'))*10/count('||field_name||' ) over()  AS decile	
			FROM '||tbl_name||' pr
			WHERE '||tgt_date_field_name||' BETWEEN $1 AND $2
		--	AND per_grade_num between 1 and 19									
			) x
		GROUP BY 1) d
		group by decile, lbound, ubound, qty'
		using   tgt_date_from , tgt_dt_val, zero_day
	;

	WITH x AS (
				SELECT
				src.decile
						Decile,
					src.lbound,
					src.ubound,
					src.Pct AS PctBench,
					tgt.Pct AS PctTarget,
					CAST((src.Pct - tgt.Pct) * LN(COALESCE(NULLIF(src.Pct,0),1E-4)/COALESCE(NULLIF(tgt.Pct,0),1E-4)) AS FLOAT) AS PSI
				FROM vdisttab src
				LEFT JOIN vtgttab tgt ON (src.decile = tgt.decile)
				)
				SELECT into rec
					CASE
						WHEN GROUPING(decile) = 1 THEN 9999
						ELSE decile
					END ::varchar(50) AS GroupCd,
					lbound AS LBound,
					ubound AS UBound,
					PctBench AS PctBench,
					PctTarget AS PctTarget,
					SUM(PSI) AS PSIValue	
				FROM x 
				GROUP BY GROUPING SETS((decile,lbound,ubound,PctBench,PctTarget), ())
				ORDER BY GROUPING(decile) DESC, decile;
	
	delta_perc=rec.psivalue;
	insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_stability values (rule_name, tbl_name, field_name, cast(tgt_dt_val as date), delta_perc, current_date);
	delete from s_grnplm_vd_hr_edp_srv_dq.tb_dq_stability where load_dt is null;
	return delta_perc;
	
end; 





$body$
EXECUTE ON ANY;
	
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_stab_date(character varying, character varying, character varying, date, date, character varying, character varying, character varying, character varying, integer, character varying, integer) IS 'Вычисляет стабильность поля типа дата между эталонной и целевой выборками и возвращает метрику PSI';

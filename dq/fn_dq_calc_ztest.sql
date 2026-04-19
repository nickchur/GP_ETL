CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_ztest(_object_name text, _column_name text, _date_field_name text, _correct_date_field_value text, _date_field_value text, _interval_type text, _interval_qty integer) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$


	declare
		_tgt_dt_val date;  -- 2023-01-01 // 2023-01-20
		_date_from date;  -- 2023-01-01 - interval '5 month' // 2023-01-20 - interval '5 month'
		_calc_result record;
	begin
		if length(_date_field_value) = 10
			then _tgt_dt_val = to_date(_date_field_value, 'yyyy-mm-dd');
		else
			_tgt_dt_val = to_date(right(_date_field_value, 8), 'yyyymmdd');
		end if;
	
		_date_from = s_grnplm_vd_hr_edp_srv_dq.fn_dq_calculate_interval_dt_from(_tgt_dt_val,_interval_type,_interval_qty);

		execute '
		with cte_help as (
			select '||_date_field_name||', '||_column_name||' as _value
			from '||_object_name||'
			where '||_date_field_name||' between '''||_date_from||''' and '''||_tgt_dt_val||'''
			'||case when _column_name = 'count(*)' then 'group by 1' else '' end||'
		),
		cte as (
			select
				*,
				avg(_value) over () as _avg,
				stddev_pop(_value) over () as _stddev 
			from cte_help
			group by 1, 2
		),
		calc as (
			select
				*,
				(_value - _avg) / coalesce(nullif(_stddev, 0), 1) as _zscore,
				row_number() over (order by '||_date_field_name||' desc) as _rnum
			from cte
		)
		select
			_value as field_value,
			_avg as avg_value,
			_stddev as stddev_value,
			_zscore as zscore,
			case
				when ztable.zscore is null then 0
				else 2 * ztable.p_value
			end as p_value
		from
			calc as main
			left join s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_ztable as ztable
				on ztable.zscore = abs(main._zscore::numeric(3, 1))
		where '||_date_field_name||' = '''||_correct_date_field_value||'''
		;
		'
		into _calc_result;
		
		delete from s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_calculation as main
		where
			main.object_name = _object_name
			and main.column_name = _column_name
			and main.date_field_name = _date_field_name
			and main.date_field_value = _correct_date_field_value
		;

		insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_calculation (
			calc_dt,
			object_name,
			column_name,
			date_field_name,
			date_field_value,
			field_value,
			date_from,
			date_to,
			avg_value,
			stddev_value,
			zscore,
			confidence_persentage
		) values (
			current_timestamp,
			_object_name,
			_column_name,
			_date_field_name,
			_correct_date_field_value,
			_calc_result.field_value,
			_date_from,
			_tgt_dt_val,
			_calc_result.avg_value,
			_calc_result.stddev_value,
			_calc_result.zscore,
			100 * _calc_result.p_value
		)
		;

		return 'OK';
end; 


$body$
EXECUTE ON ANY;
	
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_ztest(src_table_name character varying, field_name character varying, tgt_dt_val date, rule_name character varying, date_field_name character varying, interval_type character varying, interval_qty integer) 
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
as $body$

	
	
declare
	delta_perc numeric(18,4) default 0;
	tbl_name character varying;
	date_from date;
begin
	tbl_name = src_table_name;

	drop table if exists zres;
	date_from = s_grnplm_vd_hr_edp_srv_dq.fn_dq_calculate_interval_dt_from(tgt_dt_val,interval_type,interval_qty);
	execute '
	with cte as (
	select '||date_field_name||' , AVG('||field_name||') as _value, STDDEV_POP(AVG('||field_name||')) OVER () _stddev 
	FROM '||tbl_name||'
	group by 1
	),
	cte2 as (
	select avg(_value) as _avg
	FROM cte
	)
	SELECT 
	max(z)
	--|/(sum((_value-_avg)^2)/count(*))
	FROM (
					select '||date_field_name||' AS _date, 
						_avg,
						_value,
						STDDEV_POP(_value) OVER (),
						ABS(_value - _avg) / COALESCE(NULLIF(_stddev,0),1) AS z,
	--					(MIN('||date_field_name||')) AS _min_date,
	--			(MAX('||date_field_name||')) AS _max_date,
						--_max_date - _min_date + 1 AS _days_qty,
						ROW_NUMBER() OVER (ORDER BY '||date_field_name||' DESC) AS _rnum
					FROM cte
					cross join cte2
					WHERE '||date_field_name||'  BETWEEN $1 AND $2
	 				 )x
	 				 order by 1 '
		into delta_perc using date_from, tgt_dt_val;
	
	insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest values (rule_name, tbl_name, field_name, cast(tgt_dt_val as date), delta_perc, current_date);
	delete from s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest where load_dt is null;
	
end; 



$body$
EXECUTE ON ANY;
	
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_ztest(_object_name text, _column_name text, _date_field_name text, _true_object_name text, _true_column_name text, _true_date_field_name text, _correct_date_field_value text, _date_field_value text, _interval_type text, _interval_qty integer) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    _tgt_dt_val date;  -- 2023-01-01 // 2023-01-20
    _date_from date;  -- 2023-01-01 - interval '5 month' // 2023-01-20 - interval '5 month'
    _calc_result record;
begin
    if length(_date_field_value) = 10
        then _tgt_dt_val = to_date(_date_field_value, 'yyyy-mm-dd');
    else
        _tgt_dt_val = to_date(right(_date_field_value, 8), 'yyyymmdd');
    end if;

    _date_from = s_grnplm_vd_hr_edp_srv_dq.fn_dq_calculate_interval_dt_from(_tgt_dt_val,_interval_type,_interval_qty);

    execute '
    with cte_help as (
        select '||_date_field_name||', '||_column_name||' as _value
        from '||_object_name||'
        where '||_date_field_name||' between '''||_date_from||'''::date and '''||_tgt_dt_val||'''::date
        '||case when _column_name like '%(%)%' then 'group by 1' else '' end||'
    ),
    cte as (
        select
            *,
            avg(_value) over() as _avg,
            stddev_pop(_value) over() as _stddev 
        from cte_help
        ),
    calc as (
        select
            *,
            (_value - _avg) / coalesce(nullif(_stddev, 0), 1) as _zscore
        from cte
    )
    select
        _value as field_value,
        _avg as avg_value,
        _stddev as stddev_value,
        _zscore as zscore,
        case
            when ztable.zscore is null then 0
            else 2 * ztable.p_value
        end as p_value
    from
        calc as main
        left join s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_ztable as ztable
            on ztable.zscore = abs(main._zscore::numeric(3, 1))
    where '||_date_field_name||' = '''||_correct_date_field_value||'''
    ;
    '
    into _calc_result;

    delete from s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_calculation as main
    where
        main.object_name = _true_object_name
        and main.column_name = _true_column_name
        and main.date_field_name = _true_date_field_name
        and main.date_field_value = _correct_date_field_value
    ;

    insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_calculation (
        calc_dt,
        object_name,
        column_name,
        date_field_name,
        date_field_value,
        field_value,
        date_from,
        date_to,
        avg_value,
        stddev_value,
        zscore,
        confidence_persentage
    ) values (
        current_timestamp,
        _true_object_name,
        _true_column_name,
        _true_date_field_name,
        _correct_date_field_value,
        _calc_result.field_value,
        _date_from,
        _tgt_dt_val,
        _calc_result.avg_value,
        _calc_result.stddev_value,
        _calc_result.zscore,
        100 * _calc_result.p_value
    )
    ;

    return 'OK';
end; 
$body$
EXECUTE ON ANY;
	
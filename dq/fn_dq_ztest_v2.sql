CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_ztest_v2(_object_name text, _column_name text, _date_field_name text, _date_start date, _date_end date, _days integer DEFAULT 0) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

begin
    execute format($exe$
        with ztest as (
            select distinct on (date_field_value) a.*
            from (
                select now()::timestamp calc_dt
                    , %1$L::text object_name 
                    , %2$L::text column_name
                    , %3$L::text date_field_name

                    , %3$s::date date_field_value
                    , %2$s::int4 field_value

                    , %4$L::date date_from
                    , %5$L::date date_to
                from %1$s
                where %3$s >= %4$L and %3$s <= %5$L
                group by date_field_value

                union all

                select calc_dt, object_name, column_name, date_field_name, date_field_value::date, field_value, date_from, date_to
                from s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_calculation
                where object_name = %1$L and column_name = %2$L and date_field_name = %3$L
            ) a    
            order by date_field_value desc, calc_dt desc
        )
        insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_calculation 
        select a.*, 100 * coalesce(2 * z.p_value, 0) confidence_persentage
        from (
            select a.*, (field_value - avg_value) / coalesce(nullif(stddev_value, 0), 1) as zscore
            from (
                select a.*
                    , avg(field_value) over() as avg_value
                    , stddev_pop(field_value) over() as stddev_value 
                from (
                    select a.calc_dt, a.object_name, a.column_name, a.date_field_name
                    , a.date_field_value
                    , sum(b.field_value) as field_value
                    , a.date_from, a.date_to
                    from ztest a    
                    inner join ztest b on b.date_field_value between a.date_field_value - %6$s  and a.date_field_value
                    group by 1,2,3,4,5 ,7,8
                ) a    
            ) a
        ) a
        left join s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_ztable z on z.zscore = abs(a.zscore::numeric(3, 1))
        where date_field_value >= %4$L and date_field_value <= %5$L
        order by date_field_value desc
    $exe$, _object_name, _column_name, _date_field_name, _date_start, _date_end, _days);

    return 'OK';

exception when OTHERS then
    declare
        e_txt text;
        e_detail text;
        e_hint text;
        e_context text;
    begin
        get stacked diagnostics e_txt = MESSAGE_TEXT;
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
        get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
        get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(0, e_txt, e_detail, e_hint, e_context) ; 

        return 'Error: '||e_txt;
     end;
end; 
$body$
EXECUTE ON ANY;
	
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_ztest_v2(text, text, text, date, date, integer) IS 'Выполняет z-тест для колонки таблицы за диапазон дат (v2) и возвращает статус проверки';

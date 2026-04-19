CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.fn_dq_ztest(_object_name text, _column_name text, _date_field_name text, _date_start date, _date_end date) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    _cur_date date;
begin
    return 'OK';
    truncate s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_help;

    execute '
        insert into s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_help (
            date_field_value, result_count
        )
        select '||_date_field_name||', '||_column_name||'
        from '||_object_name||'
        group by 1
    ';

    for _cur_date in execute 'select date_field_value from s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_help where date_field_value between '''||_date_start||''' and '''||_date_end||''' '
        loop
        execute '
            select s_grnplm_vd_hr_edp_srv_dq.fn_dq_calc_ztest (
                ''s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_help'', 
                ''result_count'', 
                ''date_field_value'',
                '''||_object_name||''',
                '''||_column_name||''',
                '''||_date_field_name||''',
                '''||_cur_date||''',
                s_grnplm_vd_hr_edp_srv_dq.last_day(('''||_cur_date||'''::date + interval ''1 month'')::date)::text,
                ''month'',
                5
            );
        ';
    end loop;

    return 'OK';
end; 
$body$
EXECUTE ON ANY;
	
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.pr_ztest_recalc(_object text, _date date DEFAULT NULL::date) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    txt text;
    rec record;
    log_id int4;
    rd int8;
    rc int8 = 0;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_log_action('start', concat('ztest_recalc ', _object));

    begin
        -- Автодополнение схемы: если передано 'vd.table' → 's_grnplm_vd_hr_edp_vd.table'
        if split_part(_object, '.', 1) in ('stg','dia','vd','vda','dm','fcts','srv_dq','srv_wf','udlprod','udlapprove','dac','dds','md','res') then
            _object = concat('s_grnplm_vd_hr_edp_', _object);
        end if;
        _object = _object::regclass::text; -- Проверка существования объекта

        -- Удаление старых результатов Z-теста для данного объекта и даты
        delete from s_grnplm_vd_hr_edp_srv_dq.tb_ztest_data where object = _object and ts >= _date;
        get diagnostics rd = ROW_COUNT; -- Количество удалённых записей

        -- Цикл по новым пакетам данных (из статистики ETL)
        for rec in (
            select row_number() over(order by a.log_id) rw
                , a.log_id, a.wf_obj
                , array_append(a.key_dates, ts::date) key_dates -- Даты из ключевых полей
                , a.rw_cnt
                , b.ts
            from (
                select a.log_id, a.wf_obj
                , array[try_cast2timestamp(a.key_max)::date, a.load_max::date, a.data_max::date] key_dates
                , a.rw_cnt
                from tb_log_workflow_stat a
                left join s_grnplm_vd_hr_edp_srv_dq.tb_ztest_data z on a.log_id = z.log_id
                where nullif(a.rw_cnt, 0) is not null 
                    and a.wf_obj = _object
                    and z.log_id is null -- Только новые пакеты
                    and b.ts >= _date
            ) a
            inner JOIN tb_log_workflow b ON a.log_id = b.id
            order by 1
            limit 1000 -- Защита от перегрузки
        ) loop
            begin
                -- Вызов основной функции Z-теста
                txt = s_grnplm_vd_hr_edp_srv_dq.pr_ztest_all_diff(rec.wf_obj, rec.key_dates, rec.rw_cnt, - rec.log_id, rec.ts);
            end;
            rc = rc + 1;
        end loop;

        txt = format('Ok %s >= %s  %s (%s)', _object, _date::text, rc, rd);
        log_id = s_grnplm_vd_hr_edp_srv_wf.pr_log_action('end', txt, log_id);
        return txt;

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

            perform s_grnplm_vd_hr_edp_srv_wf.pr_log_error(log_id, e_txt, e_detail, e_hint, e_context);

            return concat('Error: ', e_txt);
        end;
    end;
end;

$body$
EXECUTE ON ANY;
	
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_dq.pr_ztest_recalc(text, date) IS 'Пересчитывает z-тест для указанного объекта начиная с заданной даты';

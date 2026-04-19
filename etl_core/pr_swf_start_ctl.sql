CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_ctl(wf_jsn json) 
	RETURNS json
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    -- Основные параметры
    swf text = 'ctl';                          -- Тип воркфлоу
    wf text = (wf_jsn->>'wf');                 -- Имя воркфлоу (например, 'pr_load_employee')
    
    -- Параметры ETL
    wf_sch text = (wf_jsn->>'sch');            -- Схема (краткое имя: stg, vd и т.д.)
    wf_exe text = (wf_jsn->>'exe');            -- Исполняемый код (по умолчанию — pr_<wf>())
    wf_lid int4 = coalesce((wf_jsn->>'lid')::int4, 0);  -- ID из tb_ctl_loading
    wf_try int4 = coalesce((wf_jsn->'rtr'->>'try')::int4, 1); -- Номер попытки
    wf_lft int4 = coalesce((wf_jsn->'rtr'->>'left')::int4, 0); -- Осталось попыток
    wf_sdt text = coalesce((wf_jsn->>'sdt'), now()::timestamp::text); -- Дата запуска
    wfp json = coalesce((wf_jsn->>'wfp')::json, '{}'::json); -- Доп. параметры

    -- Параметры Контроля Качества Данных (ККД / Z-тест)
    wf_zts text = (wf_jsn->>'zts')::text;      -- Схема для Z-теста (кратко: stg, vd)
    wf_ztt text = (wf_jsn->>'ztt')::text;      -- Таблица для Z-теста
    wf_zta bool = (wf_jsn->>'zta')::bool;      -- Активность Z-теста
    wf_zrb bool = (wf_jsn->>'ztb')::bool;      -- Откат при ошибке
    wf_zte bool = (wf_jsn->>'zte')::bool;      -- Требовать успеха Z-теста
    wf_ztp json = (wf_jsn->>'ztp')::json;      -- Доп. параметры Z-теста
    wf_zt bool = False;                        -- Флаг: использовать ККД

    -- Идентификатор воркфлоу
    swf_id int4 = (wf_jsn->>'swf')::int4;

    -- Внутренние переменные
    log_id int4;                               -- ID записи в логе
    wf_ret text;                               -- Результат выполнения функции
    res int4;                                  -- Код результата (1=OK, <0=ошибка)
    m_jsn json;                                -- JSON-результат
    m_txt text;                                -- Временный текст
    rec record;                                -- Для циклов
    hub json;                                  -- Данные для DataHub
    stt json;                                  -- Статистики для CTL
    zts json;                                  -- Результат Z-теста
    cdc text;                                  -- Дата изменения данных
    msg text;                                  -- Сообщение
    tag text;                                  -- Тип ответа: 'msg' или 'html'
    html text;                                 -- HTML-отчёт (если есть)
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_swf_log_action('start', swf, wf_jsn, 0, now()::timestamp); -- Лог начала

    begin
        set search_path to s_grnplm_vd_hr_edp_srv_wf;
        execute format('set application_name = %L', coalesce('ctl.'||wf, 'ctl'));

        raise info '%', wf_jsn; -- Отладка

        -- === ИНИЦИАЛИЗАЦИЯ ККД (Z-тест) ===
        wf_zt = coalesce(wf_zts, wf_ztt, wf_zta::text, wf_zrb::text, wf_ztp::text, wf_zte::text) is not null;
        wf_zts = coalesce(nullif(wf_zts, ''), 'stg');  -- По умолчанию — stg
        wf_ztt = coalesce(nullif(wf_ztt, ''), 'tb'||substring(wf, length(split_part(wf, '_', 1)) + 1)); -- Автоимя таблицы

        if wf_zt then
            m_txt = s_grnplm_vd_hr_edp_srv_dq.pr_ztest_set(
                's_grnplm_vd_hr_edp_'||wf_zts||'.'||wf_ztt, wf_zta, wf_zrb, wf_ztp);
            raise INFO 'ККД: %', m_txt;
        end if;

        -- === ФОРМИРОВАНИЕ ИСПОЛНЯЕМОГО КОДА ===
        wf_exe = concat('s_grnplm_vd_hr_edp_'||wf_sch||'.', coalesce(nullif(wf_exe, ''), format('pr_%s()', wf)));

        -- Подстановка переменных
        wf_exe = replace(wf_exe, '$wf$', quote_literal(wf));
        wf_exe = replace(wf_exe, '$sdt$', quote_literal(wf_sdt));
        wf_exe = replace(wf_exe, '$lid$', wf_lid::text);
        wf_exe = replace(wf_exe, '$try$', wf_try::text);
        wf_exe = replace(wf_exe, '$left$', wf_lft::text);
        wf_exe = replace(wf_exe, '$wfp$', quote_literal(wfp::text));

        -- Подстановка дополнительных параметров
        for rec in (select * from json_each_text(wfp)) loop
            wf_exe = replace(wf_exe, '$'||rec.key||'$', quote_literal(rec.value));
        end loop;

        -- === ВЫПОЛНЕНИЕ ВОРКФЛОУ ===
        raise INFO 'wf_exe: %', wf_exe;
        execute 'select '||wf_exe into wf_ret;
        wf_ret = coalesce(wf_ret, '');
        raise INFO 'wf_exe: %', wf_exe;

        -- === ОБРАБОТКА РЕЗУЛЬТАТА ===
        res = case
            when lower(left(wf_ret||' ',3)) = ' '   then 1
            when lower(left(wf_ret||' ',3)) = 'ok ' then 1
            when lower(left(wf_ret||' ',3)) = 'no ' then 0
            when lower(left(wf_ret||' ',6)) = 'empty ' then -1
            when lower(left(wf_ret||' ',7)) = 'expire ' then -3
            when position('query_canceled or statement_timeout' in lower(wf_ret)) > 0 then -2
            when position('uniq check error' in lower(wf_ret)) > 0 then -4
            when position('ztest error' in lower(wf_ret)) > 0 then -5
            when position('pxf server error' in lower(wf_ret)) > 0 then -8
            else -9 end;

        m_jsn = try_cast2json(wf_ret);
        if m_jsn is not null then
            res = coalesce((m_jsn->>'result')::int4, (m_jsn->>'res')::int4, res);
        end if;

        -- === КОНТРОЛЬ КАЧЕСТВА ДАННЫХ (Z-тест) ===
        if res = 1 then
            if wf_zt and coalesce(wf_zta, True) then
                -- Загрузка последнего результата Z-теста
                select row_to_json(z.*) into zts from s_grnplm_vd_hr_edp_srv_dq.tb_ztest_data z
                where object = 's_grnplm_vd_hr_edp_'||wf_zts||'.'||wf_ztt
                order by z.log_id desc nulls last limit 1;
            else
                select row_to_json(z.*) into zts from s_grnplm_vd_hr_edp_srv_dq.tb_ztest_data z
                where ts = now() order by z.log_id desc nulls last limit 1;
            end if;

            -- Проверка на ошибку Z-теста
            if wf_zte then
                raise info 'ККД: %', zts;
                if not (zts#>>'{notes,ztest}')::bool then
                    res = -5; -- Ошибка ККД
                end if;
            end if;

            -- Формирование данных для DataHub
            if zts is not null then
                hub = json_build_object('dataBusiness', to_char((zts->>'key_date')::date, 'DD.MM.YYYY')
                        , 'idMetricPath', 'Z_TEST'
                        , 'numberValue', greatest(round(100 - ABS((zts->>'zscore')::numeric), 0), 0));
            else
                hub = json_build_object('dataBusiness', to_char(current_date - 1, 'DD.MM.YYYY')
                        , 'idMetricPath', 'S_TEST', 'numberValue', 99);
            end if;
        end if;

        -- === СБОР ДОПОЛНИТЕЛЬНЫХ ДАННЫХ ===
        hub = coalesce((m_jsn->'hub')::json, hub, '{}'::json);   -- Данные для DataHub
        stt = coalesce((m_jsn->'stat')::json, '{}'::json);       -- Статистики
        zts = coalesce((m_jsn->'ztest')::json, zts, '{}'::json); -- Результат Z-теста
        cdc = coalesce((m_jsn->>'cdc'), now()::timestamp::text); -- Дата изменения
        html = (m_jsn->>'html')::text;                           -- HTML-отчёт

        -- Очистка JSON от служебных полей
        m_jsn = (
            select json_object_agg(key, value) from json_each(m_jsn)
            where key not in ('result', 'res', 'hub', 'stat', 'ztest', 'cdc', 'html')
        );

        msg = coalesce(m_jsn->>'msg', m_jsn::text, translate(wf_ret, '"', ''''));

        -- Формирование итогового ответа
        if html is null then
            tag = 'msg';
            m_jsn = json_build_object('res', res, 'swf', swf, 'wf', wf, 'exe', wf_exe
                        , 'msg', msg, 'hub', hub, 'ztest', zts, 'stat', stt, 'cdc', cdc);
        else
            tag = 'html';
            m_jsn = json_build_object('res', res, 'swf', swf, 'wf', wf, 'exe', wf_exe
                        , 'msg', msg, 'hub', hub, 'ztest', zts, 'stat', stt, 'cdc', cdc, 'html', html);
        end if;

        log_id = pr_swf_log_action('end', swf, m_jsn, log_id); -- Лог завершения
        return m_jsn;

    -- === ОБРАБОТКА ОТМЕНЫ (таймаут) ===
    exception
        when query_canceled then
            m_txt = 'query_canceled or statement_timeout';
            raise log '%', m_txt;
            m_jsn = json_build_object('res', -2,'swf', swf, 'wf', wf, 'exe', wf_exe, 'msg', m_txt);
            log_id = pr_swf_log_action('cancel', swf, m_jsn, log_id);
            set lock_timeout = 0;
            return m_jsn;

        -- === ОБРАБОТКА ОШИБОК ===
        when OTHERS then
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
            perform pr_Log_error(0, e_txt, e_detail, e_hint, e_context);
            raise log '%', e_txt;
            m_jsn = json_build_object('res', -7, 'swf', swf, 'wf', wf, 'exe', wf_exe, 'msg', translate(e_txt,'"',''''));
            log_id = pr_swf_log_action('error', swf, m_jsn, log_id);
            return m_jsn;
        end;
    end;
end; 
$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_ctl(json) IS 'Обрабатывает одну итерацию CTL-workflow согласно JSON-конфигурации';

CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_ctl_alerts(grp text DEFAULT NULL::text, bck_end time without time zone DEFAULT '12:00:00'::time without time zone, hist interval DEFAULT '30 days'::interval) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

-- E360-6367. Алерты Пакетной выгрузки.
-- 2026-09-07 21:47 MSK, v1.1, Чуркин Николай
--
-- Функцию зовёт CTL раз в 15 минут. Появился новый алерт - возвращаем res = -6 и отчёт;
-- письмо по statusNotifications рассылает сам CTL. Почту отсюда не шлём: в Greenplum нет
-- SMTP, и всё семейство pr_mail_* лишь собирает HTML (pr_send_mail тоже ничего не
-- отправляет, имя историческое).
--
-- Правила живут в параметрах потоков CTL и читаются из vw_log_ctl_wf - так же, как эта
-- вьюха достаёт из параметров wf_interval. Проброс через pr_swf_start_ctl не нужен.
--   wf_alert_group - имя группы, по нему фильтрует аргумент grp (NULL - все группы);
--   wf_alert       - JSON-правило:
--     {"kind":"daily",     "at":"09:00", "lag":"1 day",  "obj":"<схема.таблица>"}
--     {"kind":"workdays",  "at":"13:00", "lag":"10 day", "obj":"..."}   -- ПН-ПТ
--     {"kind":"weekly",    "at":"09:00", "dow":1, "lag":"1 day", "obj":"..."}
--     {"kind":"monthly",   "day":20,     "lag":"11 day", "obj":"..."}
--     {"kind":"yearly",    "month":12, "day":16, "lag":"1 day", "obj":"..."}
--     {"kind":"quarterly", "day":"last", "lag":"0 day",  "obj":"..."}
--     {"kind":"heartbeat", "stat":1,  "every":"1 hour"}   -- изменение данных
--     {"kind":"heartbeat", "stat":12, "every":"1 day"}    -- 12 статистика
--   Необязательные поля календарных видов:
--     field - колонка tb_log_workflow_stat, по которой меряем свежесть (по умолчанию data_max);
--     expr  - выражение бизнес-даты прямо по объекту, если поток не идёт через движок и
--             строк в tb_log_workflow_stat у него нет (ue_aimodel_rating, vw_predict_buckets).
--
-- Cron не используем намеренно: в plpgsql пришлось бы писать свой разбор, а "последнее
-- число квартала" им всё равно не выражается. Явные поля покрывают весь список тикета.
--
-- Дедупликация: на один алерт реагируем один раз за период правила. Ключ - тройка
-- (wf_id, alert_key, period_ts) в tb_ctl_alerts, где period_ts - начало текущего периода.
-- Прошёл следующий период - появляется новая строка и новая реакция. В отчёт при этом
-- идут ВСЕ алерты за окно hist, свежие сверху, а не только новые.
--
-- Воскресенье: с 00:00 до bck_end идёт BACKUP GP, данные не обновляются - алерты в это
-- окно не заводим вовсе. Границу правят в параметре потока wf_exe, без выкладки функции:
--   pr_mail_ctl_alerts(null, '18:00')
--
-- obj и expr подставляются в динамический SQL - как tbl/bdate в pr_check_bd4ds. obj
-- обязан выглядеть как схема.таблица, а expr не должен содержать ';': правило, которое
-- этого не проходит, пропускается и попадает в счётчик skipped. Это защита от опечатки и
-- от многооператорного текста, а не от злого умысла: expr по замыслу произвольное
-- выражение бизнес-даты, и настоящая граница доверия - у кого есть права править
-- параметры потоков в CTL.
--
-- Время серверное: и now(), и граница bck_end берутся в часовом поясе сессии. Если
-- сервер живёт не в московском времени, границу задавать с поправкой.

declare 
    m_txt text;
    e_detail text;
    e_hint text;
    e_context text;

    sql text;
    log_id int4;
    mail_id int4;
    end_id int4;
    m_res int4 = 1;
    new_cnt int4 = 0;
    bad_cnt int4 = 0;

    r record;
    r_jsn json;
    r_kind text;
    r_at time;
    r_lag interval;
    r_every interval;
    r_key text;
    r_msg text;

    last_dt timestamp;
    need_dt date;
    per_ts timestamp;

    style json;
    html text;
    mail_txt text;
    m_jsn json;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_Log_start(format('ALERTS (pr_mail_ctl_alerts %s)', coalesce(grp, 'all')));
    begin
        -- Воскресный бэкап: молчим, ничего не заводя.
        if extract(dow from now()) = 0 and now()::time < bck_end then
            m_txt = format('backup window till %s', bck_end);
            log_id = pr_log_action('end', m_txt, log_id);
            return json_build_object('res', 1, 'msg', m_txt)::text;
        end if;

        -- Правила из параметров потоков CTL. "Поток активен" читаем как "не удалён":
        -- alive описывает загрузку, а не поток, и к настройке алерта отношения не имеет.
        drop table if exists tmp_alert_rule;
        create temp table tmp_alert_rule on commit drop as
        select a.id as wf_id
             , a.name as wf_name
             , (select j.value->>'prior_value' from jsonb_array_elements(a.msg->'wf'->'param') j
                 where j.value->>'param' = 'wf_alert' limit 1) as rule_txt
             , (select j.value->>'prior_value' from jsonb_array_elements(a.msg->'wf'->'param') j
                 where j.value->>'param' = 'wf_alert_group' limit 1) as alert_grp
        from vw_log_ctl_wf a
        where coalesce(a.deleted, false) = false
        distributed randomly;

        -- Нечитаемое правило молча пропадать не должно: считаем и показываем в msg,
        -- иначе опечатка в параметре выглядит как "алертов нет".
        select count(1) into bad_cnt from tmp_alert_rule
         where rule_txt is not null and not is_valid_json(rule_txt);
        delete from tmp_alert_rule where rule_txt is null or not is_valid_json(rule_txt);
        delete from tmp_alert_rule where grp is not null and coalesce(alert_grp, '') <> grp;

        drop table if exists tmp_alert_new;
        create temp table tmp_alert_new (
            wf_id bigint, wf_name text, alert_grp text, alert_key text,
            period_ts timestamp, msg text, jsn json
        ) on commit drop distributed randomly;

        for r in select wf_id, wf_name, rule_txt, alert_grp from tmp_alert_rule order by wf_name loop
            r_jsn = r.rule_txt::json;
            r_kind = coalesce(nullif(r_jsn->>'kind', ''), 'daily');
            last_dt = null;

            if r_kind = 'heartbeat' then
                r_every = coalesce(nullif(r_jsn->>'every', ''), '1 day')::interval;
                -- Окно хартбита прибито к сетке, иначе период "плыл" бы от вызова к вызову.
                per_ts = to_timestamp(floor(extract(epoch from now()) / extract(epoch from r_every))
                                      * extract(epoch from r_every))::timestamp;

                select max(a.ts) into last_dt
                from tb_log_ctl a
                join vw_log_ctl_loading l on l.id = a.id
                where a.obj = 'statval'
                  and (a.msg->>'stat_id')::int4 = coalesce((r_jsn->>'stat')::int4, 1)
                  and l.wf_id = r.wf_id;

                if last_dt is null or last_dt < now() - r_every then
                    r_key = format('stat %s / %s', coalesce(r_jsn->>'stat', '1'), r_every);
                    r_msg = format('нет статистики %s за %s, последняя %s'
                        , coalesce(r_jsn->>'stat', '1'), r_every
                        , coalesce(left(last_dt::text, 19), 'никогда'));
                    insert into tmp_alert_new
                    values (r.wf_id, r.wf_name, r.alert_grp, r_key, per_ts, r_msg
                          , json_build_object('rule', r_jsn, 'last', left(last_dt::text, 19)));
                end if;
            else
                r_at  = coalesce(nullif(r_jsn->>'at', ''), '00:00')::time;
                r_lag = coalesce(nullif(r_jsn->>'lag', ''), '0 day')::interval;

                -- Последний наступивший дедлайн. Перебором по календарю: так все виды,
                -- включая "последнее число квартала", считаются одной формулой. 400 дней -
                -- запас к годовому правилу: между двумя его наступлениями максимум 366.
                select max(d + r_at) into per_ts
                from generate_series(current_date - 400, current_date, '1 day'::interval) d
                where ( r_kind = 'daily'
                     or (r_kind = 'workdays'  and extract(dow from d) between 1 and 5)
                     or (r_kind = 'weekly'    and extract(dow from d) = coalesce((r_jsn->>'dow')::int4, 1))
                     or (r_kind = 'monthly'   and extract(day from d) = coalesce((r_jsn->>'day')::int4, 1))
                     or (r_kind = 'yearly'    and extract(month from d) = coalesce((r_jsn->>'month')::int4, 1)
                                              and extract(day from d) = coalesce((r_jsn->>'day')::int4, 1))
                     or (r_kind = 'quarterly' and d::date = (date_trunc('quarter', d) + interval '3 month' - interval '1 day')::date)
                      )
                  and d + r_at <= now();

                if per_ts is null then
                    bad_cnt = bad_cnt + 1;   -- вид правила неизвестен
                    continue;
                end if;
                if coalesce(r_jsn->>'obj', '') !~ '^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$'
                   or coalesce(r_jsn->>'expr', '') ~ ';' then
                    bad_cnt = bad_cnt + 1;
                    continue;
                end if;
                need_dt = (per_ts - r_lag)::date;

                if nullif(r_jsn->>'expr', '') is not null then
                    sql = format('select (%s)::timestamp from %s', r_jsn->>'expr', r_jsn->>'obj');
                else
                    sql = format('select max(%I)::timestamp from tb_log_workflow_stat where wf_obj = %L'
                               , coalesce(nullif(r_jsn->>'field', ''), 'data_max'), r_jsn->>'obj');
                end if;
                execute sql into last_dt;

                if last_dt is null or last_dt::date < need_dt then
                    r_key = format('%s %s T-%s', r_kind, r_at, r_lag);
                    r_msg = format('нет данных за %s, последние %s'
                        , need_dt, coalesce(left(last_dt::text, 19), 'никогда'));
                    insert into tmp_alert_new
                    values (r.wf_id, r.wf_name, r.alert_grp, r_key, per_ts, r_msg
                          , json_build_object('rule', r_jsn, 'need', need_dt, 'last', left(last_dt::text, 19)));
                end if;
            end if;
        end loop;

        -- Заводим только то, чего в этом периоде ещё не было.
        insert into tb_ctl_alerts (ts, wf_id, wf_name, alert_grp, alert_key, period_ts, res, msg, jsn)
        select clock_timestamp(), a.wf_id, a.wf_name, a.alert_grp, a.alert_key, a.period_ts, -6, a.msg, a.jsn
        from tmp_alert_new a
        where not exists (
            select 1 from tb_ctl_alerts b
            where b.wf_id = a.wf_id and b.alert_key = a.alert_key and b.period_ts = a.period_ts
        );
        get diagnostics new_cnt = ROW_COUNT;

        if new_cnt > 0 then
            m_res = -6;
            m_txt = format('%s new alert(s)', new_cnt);
            -- Реакция только по своим строкам: чужую группу, ждущую своего вызова, не трогаем.
            update tb_ctl_alerts a set reacted_ts = now()
            from tmp_alert_new b
            where a.wf_id = b.wf_id and a.alert_key = b.alert_key and a.period_ts = b.period_ts
              and a.reacted_ts is null;
        else
            m_txt = 'no new alerts';
        end if;
        if bad_cnt > 0 then
            m_txt = format('%s, %s rule(s) skipped', m_txt, bad_cnt);
        end if;

        -- В отчёт идут все алерты за окно, свежие сверху.
        drop table if exists tmp_alerts;
        create temp table tmp_alerts on commit drop as
        select left(a.ts::text, 19) as ts
             , a.wf_name
             , a.alert_grp
             , a.alert_key
             , left(a.period_ts::text, 16) as period
             , case when a.reacted_ts is null then 'new' else left(a.reacted_ts::text, 19) end as reacted
             , a.msg
        from tb_ctl_alerts a
        where a.ts > now() - hist
          and (grp is null or a.alert_grp = grp)
        distributed randomly;

        style = pr_mail_style();
        html = format('<div style="color:%1$s"><h2> CTL Alerts %2$s </h2><h4> %3$s </h4></div>'
            , case when new_cnt > 0 then 'red' else 'green' end, coalesce(grp, 'all'), m_txt);
        html = concat(html, pr_tbl2html('tmp_alerts', 'CTL Alerts', 'order by ts desc, wf_name', style));

        mail_id = pr_swf_log_action('CTL Alerts', 'mail', json_build_object('len', length(html), 'html', html));
        end_id = pr_swf_log_action('end', 'mail', null, mail_id);
        mail_txt = pr_send_mail(mail_id::text);
        -- pr_send_mail при своей ошибке отдаёт голый текст, а не JSON. Разбирать его нечем,
        -- но терять из-за этого сам алерт нельзя: он уже заведён и отреагирован.
        if not is_valid_json(coalesce(mail_txt, '')) then
            m_txt = format('%s (mail: %s)', m_txt, left(coalesce(mail_txt, 'null'), 200));
            mail_txt = '{}';
        end if;

        -- res и msg свои, остальное - от pr_send_mail (id, ts, report, html).
        m_jsn = (
            select json_object_agg(key, value) from (
                select 'res' as key, to_json(m_res) as value
                union all select 'msg', to_json(m_txt)
                union all select * from json_each(mail_txt::json) where key not in ('res', 'msg')
            ) a
        );

        log_id = pr_log_action('end', format('%s, %s rules', m_txt, (select count(1) from tmp_alert_rule)), log_id);
        return m_jsn::text;

    exception when OTHERS then
        get stacked diagnostics m_txt = MESSAGE_TEXT;
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
        get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
        get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

        perform pr_Log_error(log_id, m_txt, e_detail, sql, e_context) ; 
        return format('Error: %s', m_txt);
    end;
end;

$body$
EXECUTE ON ANY;

-- DEFAULT в сигнатуре COMMENT ON недопустим, как и в DROP FUNCTION — только типы.
COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_ctl_alerts(text, time without time zone, interval) IS 'Алерты Пакетной выгрузки. v1.1';

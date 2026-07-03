/*
 pr_swf_start(swf text, wf text) -> text (json)

 Запуск одного sub-workflow wf в рамках super-workflow swf. Выполняет
 проверки предусловий, вызывает функцию-исполнитель воркфлоу (wf_exec),
 нормализует результат в JSON и фиксирует его в tb_swf и логе действий.
 Всегда возвращает JSON-строку с полем reselt (код результата).

 ПАРАМЕТРЫ
   swf - имя super-workflow (группы), в контексте которого идёт запуск.
   wf  - имя запускаемого sub-workflow (строка vw_swf.wf_name).

 КОДЫ РЕЗУЛЬТАТА (поле reselt в JSON)
    >0 / 1  - успех (Ok или reselt из JSON, вернувшегося из wf_exec)
    -1      - нет данных (ответ wf_exec начинается с 'no')
    -2      - query_canceled / statement_timeout
    -3      - непойманная ошибка (блок OTHERS)
    -4      - прочий/ошибочный ответ wf_exec (не Ok/No/deadlock)
    -5      - deadlock detected
    -8      - просрочено: с wf_next прошло больше wf_expire
    -9      - пропуск: зависимости не в статусе OK (rel_ok != true)
   -10      - TODO-статус воркфлоу не true
   -11      - воркфлоу не найден в vw_swf (wf_id is null)
   -12      - воркфлоу уже запущен (активная сессия по application_name)

 АЛГОРИТМ
  1. application_name = wf; search_path на схему SWF; лог действия 'start'.
  2. Пауза pg_sleep(10), затем защита от повторного запуска: поиск активных
     сессий в pg_stat_activity по application_name = wf (или wf/%). При
     срабатывании - reselt=-12, лог 'error', возврат.
  3. Чтение метаданных воркфлоу: select * from vw_swf where wf_name = wf.
     Предусловия (при нарушении - лог и ранний возврат/пропуск):
       - wf_id is null            -> -11 'error';
       - todo is not true         -> -10 'todo';
       - rel_ok is not true       ->  -9 'skip'  (без раннего возврата);
       - now()-wf_next > wf_expire ->  -8 'late'  (без раннего возврата).
  4. Иначе - вызов исполнителя: execute 'select <wf_exec>' into ret.
     Разбор ответа:
       - если ret это JSON с полем reselt -> берём это значение;
       - иначе по префиксу текста: 'ok ' -> 1; 'no ' -> -1;
         'dedlock detected' -> -5; прочее -> -4.
     Лог действия 'end'.
  5. Фиксация: update tb_swf (wf_last, wf_duration, wf_reselt, wf_swf),
     возврат JSON-строки m_txt.
  6. Исключения:
       - query_canceled -> reselt=-2, лог 'cancel', update tb_swf, возврат;
       - OTHERS -> pr_Log_error, reselt=-3, лог 'error', update tb_swf, возврат.

 EXECUTE ON ANY - может выполняться на любом сегменте Greenplum.
 См. также pr_swf_wf_group (оркестрация группы workflow).
 */
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_start(swf text, wf text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    m_txt text;
    m_jsn jsonb;
    ret text;
    wf_rec record;
    log_id int4 default null;
    sql text;
    e_txt text;
    e_detail text;
    e_hint text;
    e_context text;
    res int8;
begin
    sql = format('set application_name = %L', wf);
    execute sql;
    set search_path to s_grnplm_vd_hr_edp_srv_wf;

--  begin
--      set lock_timeout = 10;
--  --  execute format('update tb_swf_%s_log set ts = now() where id=0', lower(swf));
--      execute format('truncate tb_swf_%s_log', lower(swf));
--      execute format('insert into tb_swf_%s_log (id, ts) values(0, now())', lower(swf));
--      set lock_timeout = 0;
--  exception when lock_not_available then
--      m_txt = 'SWF busy (lock_timeout)';
--      raise log '%', m_txt;
--      m_txt = json_build_object('reselt', -1,'swf', swf, 'wf', wf, 'msg', m_txt)::text;
--      set lock_timeout = 0;
--      return m_txt;
--  end;

    log_id = pr_swf_log_action('start', swf);
    begin
        perform pg_sleep(10);
    
        select count(1) into wf_rec from pg_stat_activity a
        where application_name = wf or application_name like wf||'/%'
            and pid <> pg_backend_pid() and state <> 'idle';
        
        if wf_rec.count is null then
            m_txt = json_build_object('reselt', -12, 'swf', swf, 'wf', wf, 'msg', 'WF is  allready started')::text;
            log_id = pr_swf_log_action('error', swf, m_txt::json, log_id);
            return m_txt;
        end if;
        

        select * into wf_rec from vw_swf where wf_name = wf;

        raise info '%', wf_rec;

        if wf_rec.wf_id is null then
            m_txt = json_build_object('reselt', -11, 'swf', swf, 'wf', wf, 'msg', 'No WF')::text;
            log_id = pr_swf_log_action('error', swf, m_txt::json, log_id);
            return m_txt;
        end if;

        if wf_rec.todo is not true then
            m_txt = json_build_object('reselt', -10, 'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg', 'WF TODO status is not True')::text;
            log_id = pr_swf_log_action('todo', swf, m_txt::json, log_id);
            return m_txt;
        end if;
        
        if wf_rec.rel_ok is not true then
            m_txt = json_build_object('reselt', -9, 'swf', swf, 'wf', wf, 'msg', format('WF skipped %s status is not OK', wf_rec.not_ok_note))::text;
            log_id = pr_swf_log_action('skip', swf, m_txt::json, log_id);
        elseif (now() - wf_rec.wf_next) > wf_rec.wf_expire then 
            m_txt = json_build_object('reselt', -8, 'swf', swf, 'wf', wf, 'msg',  'Too late. More then '||wf_rec.wf_expire||' passed. '||wf_rec.wf_next);
            log_id = pr_swf_log_action('late', swf, m_txt::json, log_id);
        else
            sql = format('select %s',wf_rec.wf_exec);
            raise info '%', sql ;
            execute sql  into ret;
            raise info '%', ret ;
            set search_path to s_grnplm_vd_hr_edp_srv_wf;

            m_jsn = try_cast2jsonb(ret);
            res = try_cast2int(m_jsn->>'reselt');
            if res is null then
                if (lower(ret||' ') like 'no %') then
                    m_txt = json_build_object('reselt', -1,'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg',translate(ret,'"',''''))::text;
                elseif (lower(ret||' ') like 'ok %') then
                    m_txt = json_build_object('reselt', 1, 'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg',translate(ret,'"',''''))::text;
                elseif (lower(ret) like 'dedlock detected%') then
                    m_txt = json_build_object('reselt', -5,'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg',translate(ret,'"',''''))::text;
                else 
--                elseif (lower(ret) like '%error%') then
                    m_txt = json_build_object('reselt', -4,'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg',translate(ret,'"',''''))::text;
                end if;
            else 
                m_txt = json_build_object('reselt', res, 'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg',translate(ret,'"',''''))::text;
            end if;
        
            log_id = pr_swf_log_action('end', swf, m_txt::json, log_id);
        end if;
        raise log '%', m_txt;
        update tb_swf set wf_last = now(), wf_duration = clock_timestamp() - now(), wf_reselt = m_txt::json, wf_swf = swf where wf_name = wf;
        return m_txt;
    
    exception 
        when query_canceled then
            m_txt = 'query_canceled or statement_timeout';
            raise log '%', m_txt;
            m_txt = json_build_object('reselt', -2,'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg',m_txt)::text;
            log_id = pr_swf_log_action('cancel', swf, m_txt::json, log_id);
            update tb_swf set wf_last = now(), wf_duration = clock_timestamp() - now(), wf_reselt = m_txt::json, wf_swf = swf where wf_name = wf;
            set lock_timeout = 0;
            return m_txt;
        when OTHERS then
            get stacked diagnostics e_txt = MESSAGE_TEXT;
            get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
            get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
            get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
            perform pr_Log_error(0, e_txt, e_detail, e_hint, e_context) ; 
            raise log '%', e_txt;
            m_txt = json_build_object('reselt', -3,'swf', swf, 'wf', wf, 'msg', translate(e_txt,'"',''''))::text;
            log_id = pr_swf_log_action('error', swf, m_txt::json, log_id);
            update tb_swf set wf_last =  now(), wf_duration = clock_timestamp() - now(), wf_reselt = m_txt::json, wf_swf = swf where wf_name = wf;
            set lock_timeout = 0;
            return m_txt;
    end;
end; 
$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_start(text, text) IS 'Запускает указанный sub-workflow в рамках super-workflow';

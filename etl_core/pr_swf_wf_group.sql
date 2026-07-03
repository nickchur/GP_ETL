/*
 * pr_swf_wf_group(fnc text[], rel text[]) -> text
 * ---------------------------------------------------------------------------
 * Оркестратор одной ГРУППЫ workflow в Super Workflow Engine. Последовательно
 * выполняет функции-загрузки из fnc, управляя запуском каждой следующей по
 * графу зависимостей rel. Возвращает статус группы: 'Ok' / 'No' / 'Error'
 * вместе с массивом кодов res и построчным логом.
 *
 * ПАРАМЕТРЫ
 *   fnc  - упорядоченный список имён функций-загрузок
 *          (напр. {smdtodia_ref_pos, diatostg_ref_pos, ...}).
 *   rel  - для каждого шага k строка-массив индексов ("{}", "{1}", "{1,2}"),
 *          задающая гейт для СЛЕДУЮЩЕГО шага: какие ранее выполненные функции
 *          и с каким результатом проверять.
 *
 * КОДЫ РЕЗУЛЬТАТА res[k]
 *    1  - успех  (ответ 'Ok ...' или пусто)
 *    0  - нет данных / пропуск (ответ 'No ...')
 *   -1  - ошибка
 *
 * АЛГОРИТМ
 *  1. Инициализация: mif = true (можно запускать), err = true (аккумулятор),
 *     запоминается текущий application_name.
 *  2. Цикл по функциям k = 1..N:
 *     - если гейт mif = true:
 *         a) нормализация имени (trim; префикс 'pr_'; скобки '()' при отсутствии);
 *         b) в application_name дописывается имя функции (трассировка);
 *         c) вызов с ретраями - до 3 попыток: при 'transfer error (18)' или
 *            'PXF server error' пауза pg_sleep(60) и повтор; при try>1 в текст
 *            дописывается счётчик попыток;
 *         d) разбор ответа: 'ok'/пусто -> res[k]=1; 'no' -> res[k]=0;
 *            иначе -> res[k]=-1.
 *     - иначе (шаг пропущен): m_txt='WF skipped', res[k] = -err::int
 *         (пропагация ошибки: -1 если гейт закрылся ошибкой, иначе 0).
 *     - результат дописывается в лог ret.
 *     - пересчёт гейта по rel[k]:
 *         непустой список  -> mif = OR(res[dep]=1), err = OR(res[dep]=-1);
 *         пустой ("{}")    -> mif = true (следующий шаг безусловно).
 *  3. Финальная агрегация по последнему элементу rel:
 *     - пустой  -> успех требует ВСЕ шаги: mif = AND(res[i]=1), err = OR(res[i]=-1);
 *     - непустой -> итоговые mif/err берутся из последней итерации цикла.
 *  4. Возврат: err -> 'Error ...'; иначе mif -> 'Ok ...'; иначе -> 'No ...'.
 *  5. Исключения: любая непойманная ошибка логируется через pr_Log_error,
 *     возвращается 'Error <текст> <ret>'.
 *
 * Гейты промежуточных шагов работают по OR (достаточно одного успешного
 * предшественника); финал при пустом последнем rel - по AND (нужны все).
 * EXECUTE ON ANY - может выполняться на любом сегменте Greenplum.
 *
 * ПРИМЕР ГРАФА ЗАВИСИМОСТЕЙ
 *   fnc = {smdtodia_ref_pos, smdtodia_ref_pos_hist,
 *          diatostg_ref_pos, diatostg_ref_pos_hist}
 *   rel = {"{}", "{1}", "{1,2}", "{3,4}"}
 *
 *   rel[k] задаёт гейт для запуска СЛЕДУЮЩЕГО шага (k+1);
 *   rel[N] служит критерием итогового статуса группы.
 *
 *      [1] smdtodia_ref_pos           (старт: mif=true, безусловно)
 *           |
 *           |  rel[1]="{}"  -> безусловно
 *           v
 *      [2] smdtodia_ref_pos_hist
 *           |
 *           |  rel[2]="{1}"  -> если res[1]=1
 *           v
 *      [3] diatostg_ref_pos
 *           |
 *           |  rel[3]="{1,2}"  -> если res[1]=1 OR res[2]=1
 *           v
 *      [4] diatostg_ref_pos_hist
 *           |
 *           |  rel[4]="{3,4}"  -> ИТОГ: Ok если res[3]=1 OR res[4]=1
 *           v
 *        [ status ]  Error, если по пути встретился res=-1
 *
 *   Если гейт закрыт (res предшественников != 1), шаг помечается 'WF skipped'
 *   и res[k] наследует ошибку (-1) или пропуск (0) от предшественников.
 * ---------------------------------------------------------------------------
 */
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_wf_group(fnc text[], rel text[])
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$


declare
--    fnc text[] default '{smdtodia_ref_pos,smdtodia_ref_pos_hist,diatostg_ref_pos,diatostg_ref_pos_hist}';
--    rel text[] default '{"{}","{1}","{1,2}","{3,4}"}';
    res  int[];
    mif  bool default true;
    err  bool default true;
    rif int[];
    k int;
    i int;
    ret text default '';
    log_id int4 default null;
    m_txt text;
    sql text;
    app text;
    func text;
    try int;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    execute 'show application_name' into app;
    begin
        for k  in 1..array_length(fnc,1) loop
            if (mif) then
            
                func = trim(fnc[k]);

                sql = format('set application_name = %L', coalesce(app||'/', '')||func);
                execute sql;
                
                if left(lower(func),3) <> 'pr_' then
                    func = 'pr_' || func;
                end if;
                
                if right(func, 1) <> ')' then
                    func = func || '()';
                end if;
            
                try = 1;
                for i  in 1..3 loop
                    execute format('select %s', func) into m_txt;  
                    exit when m_txt not like '%transfer error (18)%' and m_txt not like '%PXF server error%';
                    try = try + 1;
                    perform pg_sleep(60);
                end loop;
                
                if try > 1 then
                    m_txt = concat(m_txt, ' (try=', try, ')');
                end if;

                if (lower(left(coalesce(trim(m_txt),'')||' ',3)) in ('ok ', ' ')) then
                    res[k] =  1;
                elsif (lower(left(coalesce(trim(m_txt),'')||' ',3)) in ('no ')) then
                    res[k] =  0;
                else 
                    res[k] = -1;
                end if;
            else 
                m_txt = 'WF skipped';
                -- res[k] = 0;
                res[k] = - err::int;
            end if;
            -- ret = concat(chr(10),'< ', fnc[k], ': ', m_txt, ret);
            ret = concat(ret, chr(10),'> ', fnc[k], ': ', m_txt);
       
            rif = rel[k]::int[];
            if array_length(rif,1)>=1 then
                mif = false;
                err = false;
                for i  in 1..array_length(rif,1) loop
                    mif = (mif or (res[rif[i]] =  1));
                    err = (err or (res[rif[i]] = -1));
                end loop;
            else 
                mif = true;
            end if;
        end loop;

--        if coalesce(array_length(rif,1), 0) = 0 then 
        if coalesce(array_length(rel[array_length(fnc, 1)]::int[], 1), 0) = 0 then 
            mif = true;
            err = false;
            for i  in 1..array_length(fnc,1) loop
                mif = (mif and (res[i] =  1));
                err = (err or  (res[i] = -1));
            end loop;
        end if;
        
        if (err) then
            return format('Error %s %s', res, ret);
        elsif (mif) then
            return format('Ok %s %s', res, ret);
        else 
            return format('No %s %s', res, ret);
        end if;

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
            
            log_id = pr_Log_error(log_id,e_txt,e_detail,e_hint,e_context) ;
            return concat('Error ', e_txt, ret);
        end;
    end;
end; 
$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_wf_group(text[], text[]) IS 'Формирует группу workflow из списка функций и зависимостей для super-workflow';

CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_rebuild_org_structure_year(y integer) 
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$


     
      
       declare
             v_cal_date date; -- Дата расчета
             query text; -- Текст за
             max_lvl int; -- Максимальный уровень орг структуры
             cur_iter_0 int; -- Счетчик итератора 1 уровня
             cur_iter_1 int; -- Счетчик итератора 2 уровня
             org_id text; -- Орг единица
             org_id_lvl text; -- Уровень орг единицы
             join_stmt text; -- Выражение для формирования соединения с одной таблицей
             join_full text; -- Выражение для формирования соединения с одной таблицей
             next_lvl_data text; -- Выражение формирует список полей для каждого нового уровня
             comma char(1); -- символ запятой
             m_txt text; -- Текст ошибки
             wf_start_dttm timestamp; --
             wf_name varchar(100); --
             log_id int;
             e_detail text;
             e_hint text;
             e_context text;
        step_id text;
--           q_query text;
     
       declare
             cur cursor for
       -- Формируем календарь на каждый день с 1 января 2015г.
--           select distinct
--                  cal_dates::date
--           from
--                  generate_series('2015-01-01 00:00'::timestamp
--                                             , current_timestamp(0)
--                                             , '1 day'
--                                             ) as cal_dates
            select generate_series(to_date(concat('01-01-', y), 'DD-MM-YYYY'), to_date(concat('31-12-', y), 'DD-MM-YYYY'), INTERVAL '1 day')::date as cal_date
             where 1 = 1
             order by 1;
     
             cur2 cursor for
       -- Формируем календарь на каждый месяц с 1 января 2015г.
--           select distinct
--                  cal_dates::date
--           from
--                  generate_series('2015-01-01 00:00'::timestamp
--                                             , current_timestamp(0)
--                                             , '1 month'
--                                             ) as cal_dates
             select generate_series(to_date(concat('01-01-', y), 'DD-MM-YYYY'), to_date(concat('31-12-', y), 'DD-MM-YYYY'), INTERVAL '1 month')::date as cal_date
             where 1 = 1
             order by 1;
            
       begin      
             -- логирование воркфлоу
             wf_start_dttm = (select clock_timestamp());
             wf_name = 'GPUPDATE_PR_REBUILD_ORGSTRUCTURE';
             log_id = s_grnplm_vd_hr_edp_srv_wf.pr_Log_start('GPUPDATE_PR_REBUILD_ORGSTRUCTURE (pr_rebuild_org_structure_year)');   --Log
             begin
           
             -- insert into s_grnplm_vd_hr_edp_srv_wf.workflow_status(workflow, start_ts)
             -- values ('GPDDS_PR_REBUILD_ORGSRTUCTURE', now());
                  
             -- Собираем оргструктуру на каждый день
             -- (орг единица
             -- , родительская орг единица
             -- , даты существования
             -- , уровень
             -- , дата на которую актуальна запись)
           
             -- Раскомментировать для отладки
--           drop table if exists s_grnplm_ld_hr_edp_mvp.cur_test;
--           create table s_grnplm_ld_hr_edp_mvp.cur_test
--           (ins_date timestamp default current_timestamp
--           , query_txt text, block int, query_num int);
           
             -- Очищаем данные
--                  truncate s_grnplm_vd_hr_edp_dds.nfbk_org_tree;
                    delete from s_grnplm_vd_hr_edp_stg.nfbk_org_tree
                    where extract('year' from cal_date) = y;
           
             -- Определяем шаблон запроса для формирования таблицы
                    query = 'insert into s_grnplm_vd_hr_edp_stg.nfbk_org_tree'
                                  || E'\nwith recursive org_tree as ('
                                  || E'\n\tselect'
                                  || E'\n\t\t''%1$s''::date as cal_date'
                                  || E'\n\t\t, id_object as org_id'
                                  || E'\n\t\t, id_connection_object as parent_org_id'
                                  || E'\n\t\t, 1 as lvl'
                                  || E'\n\t\t, date_start'
                                  || E'\n\t\t, date_finish'
                                  || E'\n\tfrom s_grnplm_vd_hr_edp_stg.tb_infotype_1001_sm ti'
                                  || E'\n\twhere 1 = 1'
                                  || E'\n\t\tand connection_type = ''A002'''
                                  || E'\n\t\tand id_connection_object in (10283181, 10293178, 10000203, 10319600, 10319601, 10319604, 10319605, 10319606, 10319607, 10320576, 10325250, 10319603)'
                                  || E'\n\t\tand ''%1$s'' between date_start and date_finish'
                                  || E'\n\tunion all'
                                  || E'\n\tselect'
                                  || E'\n\t\t''%1$s''::date as cal_date'
                                  || E'\n\t\t, ti.id_object as org_id'
                                  || E'\n\t\t, ti.id_connection_object as parent_org_id'
                                  || E'\n\t\t, ot.lvl + 1 as lvl'
                                  || E'\n\t\t, ti.date_start'
                                  || E'\n\t\t, ti.date_finish'
                                  || E'\n\tfrom s_grnplm_vd_hr_edp_stg.tb_infotype_1001_sm ti'
                                  || E'\n\t\tright join org_tree as ot'
                                  || E'\n\t\t\ton ot.org_id = ti.id_connection_object'
                                  || E'\n\t\t\t\tand (ot.date_start, ot.date_finish) overlaps (ti.date_start, ti.date_finish)'
                                  || E'\n\twhere 1 = 1'
                                  || E'\n\t\tand ti.connection_type = ''A002'''
                                  || E'\n\t\tand ''%1$s'' between ti.date_start and ti.date_finish'
                                  || E'\n\t\t)'
                                  || E'\nselect * from org_tree;'
                                  ;
                         
             -- Сбрасываем счетчик
                    cur_iter_0 = 0;
                    open cur;
                    loop
                           cur_iter_0 = cur_iter_0 + 1;
                           fetch cur into v_cal_date;
                           if not found then exit; end if;
             -- Формируем запрос по шаблону и выполняем его для каждого дня в календаре
                                  execute format(query, v_cal_date);
             -- Запись в таблицу для отладки
--                  insert into s_grnplm_ld_hr_edp_mvp.cur_test(query_txt, block, query_num)
--                  values(format(query, v_cal_date), 1, cur_iter_0);
                    end loop;
                    close cur;
     
             -- Формируем шаблон для создания временных таблиц
                    query = 'drop table if exists lvl_%1$s;'
                                  || E'\ncreate temp table lvl_%1$s as ('
                                  || E'\nselect'
                                  || E'\n\tparent_org_id as lvl_%2$s_org_id'
                                  || E'\n\t, org_id as lvl_%1$s_org_id'
                                  || E'\n\t, date_start as lvl_%1$s_date_start'
                                  || E'\n\t, date_finish  as lvl_%1$s_date_end'
                                  || E'\n\t, cal_date'
                                  || E'\n\t, lvl'
                                  || E'\nfrom s_grnplm_vd_hr_edp_stg.nfbk_org_tree'
                                  || E'\nwhere 1 = 1'
                                  || E'\n\tand lvl = %1$s'
                                  || E'\n)'
                                  || E'\ndistributed by (lvl_%2$s_org_id);'
                                  ;
             -- Определяем максимальный уровень включенности
                    max_lvl = (select max(lvl) from s_grnplm_vd_hr_edp_stg.nfbk_org_tree);
           
             -- Сбрасываем счетчик
                    cur_iter_0 = 0;
     
             -- Создаем для каждого уровня включенности свою таблицу подставляя данные в шаблон
                    while cur_iter_0 < max_lvl loop
                           cur_iter_0 = cur_iter_0 + 1;
                           execute format(query, cur_iter_0, (cur_iter_0 - 1));
             -- Запись в таблицу для отладки
--                  insert into s_grnplm_ld_hr_edp_mvp.cur_test(query_txt, block, query_num)
--                  values(format(query, cur_iter_0, (cur_iter_0-1)), 2 , cur_iter_0);
                    end loop;
           
             -- Создаем временную таблицу для записи в нее данных полученных в цикле.
             -- Нужна для того что бы в финальной таблице были данные по всем родителям всех
             -- орг единиц, а не только для "концов" веток
--                  drop table if exists dnf_org_tree;
                    drop table if exists s_grnplm_vd_hr_edp_dia.dnf_org_tree_tmp;
--                  create temp table dnf_org_tree (
                    create table s_grnplm_vd_hr_edp_dia.dnf_org_tree_tmp (
                           cal_date date null,
                           org_id int4 null,
                           org_id_lvl int4 null,
                           lvl_0_org_id int4 null,
                           lvl_1_org_id int4 null,   lvl_1_date_start date null,       lvl_1_date_end date null,
                           lvl_2_org_id int4 null,   lvl_2_date_start date null,       lvl_2_date_end date null,
                           lvl_3_org_id int4 null,   lvl_3_date_start date null,       lvl_3_date_end date null,
                           lvl_4_org_id int4 null,   lvl_4_date_start date null,       lvl_4_date_end date null,
                           lvl_5_org_id int4 null,   lvl_5_date_start date null,       lvl_5_date_end date null,
                           lvl_6_org_id int4 null,   lvl_6_date_start date null,       lvl_6_date_end date null,
                           lvl_7_org_id int4 null,   lvl_7_date_start date null,       lvl_7_date_end date null,
                           lvl_8_org_id int4 null,   lvl_8_date_start date null,       lvl_8_date_end date null,
                           lvl_9_org_id int4 null,   lvl_9_date_start date null,       lvl_9_date_end date null,
                           lvl_10_org_id int4 null, lvl_10_date_start date null, lvl_10_date_end date null,
                           lvl_11_org_id int4 null, lvl_11_date_start date null, lvl_11_date_end date null,
                           lvl_12_org_id int4 null, lvl_12_date_start date null, lvl_12_date_end date null,
                           lvl_13_org_id int4 null, lvl_13_date_start date null, lvl_13_date_end date null,
                           lvl_14_org_id int4 null, lvl_14_date_start date null, lvl_14_date_end date null,
                           lvl_15_org_id int4 null, lvl_15_date_start date null, lvl_15_date_end date null,
                           lvl_16_org_id int4 null, lvl_16_date_start date null, lvl_16_date_end date null
                    )
                    with (
                           appendonly=true
                           , orientation=column
--                         , compresstype=zstd
                    )
--                  distributed by (cal_date, org_id);
                    distributed by (
                           cal_date, org_id,
                           lvl_0_org_id ,
                           lvl_1_org_id ,     lvl_1_date_start , lvl_1_date_end ,
                           lvl_2_org_id ,     lvl_2_date_start , lvl_2_date_end ,
                          lvl_3_org_id ,     lvl_3_date_start , lvl_3_date_end ,
                           lvl_4_org_id ,     lvl_4_date_start , lvl_4_date_end ,
                           lvl_5_org_id ,     lvl_5_date_start , lvl_5_date_end ,
                           lvl_6_org_id ,     lvl_6_date_start , lvl_6_date_end ,
                           lvl_7_org_id ,     lvl_7_date_start , lvl_7_date_end ,
                           lvl_8_org_id ,     lvl_8_date_start , lvl_8_date_end ,
                           lvl_9_org_id ,     lvl_9_date_start , lvl_9_date_end ,
                           lvl_10_org_id , lvl_10_date_start , lvl_10_date_end ,
                           lvl_11_org_id , lvl_11_date_start , lvl_11_date_end ,
                           lvl_12_org_id , lvl_12_date_start , lvl_12_date_end ,
                           lvl_13_org_id , lvl_13_date_start , lvl_13_date_end ,
                           lvl_14_org_id , lvl_14_date_start , lvl_14_date_end ,
                           lvl_15_org_id , lvl_15_date_start , lvl_15_date_end ,
                           lvl_16_org_id , lvl_16_date_start , lvl_16_date_end  );
           
             -- Сбрасываем счетчик
                  
             raise info 'created temp table';
    step_id = 'insert into s_grnplm_vd_hr_edp_dia.dnf_org_tree_tmp';
       open cur2;
             loop
                    fetch cur2 into v_cal_date;
                    if not found then exit; end if;
                    cur_iter_0 = 0;
                    raise info 'v_cal_date is %', v_cal_date;
           
             -- Определяем шаблон для формирования всей "ветки" для орг единицы на дату
--                  query = 'insert into dnf_org_tree'
                    query = 'insert into s_grnplm_vd_hr_edp_dia.dnf_org_tree_tmp'
                                  || E'\n\tselect'
                                  || E'\n\t\tl1.cal_date'
                                  || E'\n\t\t, coalesce( %1$s'
                                  || E'\n\t\t  ) as org_id'
                                  || E'\n\t\t, coalesce( %2$s'
                                  || E'\n\t\t  ) as org_id_lvl'
                                  || E'\n\t\t, l1.lvl_0_org_id %3$s'
                                  || E'\n\tfrom %4$s'
                                  || E'\n\twhere 1 = 1'
                                  || E'\n\t\tand date_trunc(''month'', l1.cal_date)::date = ''%5$s''::date;'
                                  ;
           
             -- Инициируем переменные
                    org_id = '';
                    org_id_lvl = '';
                    next_lvl_data = '';
                    join_full = '';
                    join_stmt = '';
                  
             -- Выполняем цикл
                    while cur_iter_0 < max_lvl loop
                           cur_iter_0 = cur_iter_0 + 1;
                         
                           cur_iter_1 = cur_iter_0;
                  
             -- Цикл для корректного формирования полей org_id, org_id_lvl
             -- Поля формируются динамически
                           while cur_iter_1 > 0 loop
                                  if cur_iter_1 <> cur_iter_0
                                        then comma = ',';
                                  else
                                        comma = '';
                                  end if;
                                  org_id = org_id || format(E'\n\t\t\t%2$s l%1$s.lvl_%1$s_org_id', cur_iter_1, comma);
                                  org_id_lvl = org_id_lvl || format(E'\n\t\t\t%2$s l%1$s.lvl', cur_iter_1, comma);
                                  cur_iter_1 = cur_iter_1 - 1;
                           end loop;
                  
             -- Готовим динамически формируемые соединения
                           next_lvl_data = next_lvl_data ||
                                  format(E'\n\t\t, l%1$s.lvl_%1$s_org_id, l%1$s.lvl_%1$s_date_start, l%1$s.lvl_%1$s_date_end', cur_iter_0);
                           if cur_iter_0 = 1 then
                                  join_stmt = 'lvl_%1$s as l%1$s';
                           else
                                  join_stmt = E'\n\tleft join lvl_%1$s as l%1$s'
                                                      ||E'\n\t\ton l%2$s.lvl_%2$s_org_id = l%1$s.lvl_%2$s_org_id'
                                                      ||E'\n\t\t\tand l%2$s.cal_date = l%1$s.cal_date'
                                                      ||E'\n\t\t\tand (l%1$s.lvl_%1$s_date_start, l%1$s.lvl_%1$s_date_end)'
                                                      ||' overlaps (l%2$s.lvl_%2$s_date_start, l%2$s.lvl_%2$s_date_end)'
                                                      ;
                           end if;
                  
                           join_full = join_full || format(join_stmt, cur_iter_0, (cur_iter_0 - 1));
                  
             -- Выполняем сформированный по шаблону запрос
--                         execute format(query, org_id, org_id_lvl, next_lvl_data, join_full);
                           raise info 'Обабатываем месяц %',v_cal_date;
                           execute format(query, org_id, org_id_lvl, next_lvl_data, join_full, v_cal_date);
                         
             -- Запись в таблицу для отладки
--                         insert into s_grnplm_ld_hr_edp_mvp.cur_test(query_txt, block, query_num)
--                         values(format(query, org_id, org_id_lvl, next_lvl_data, join_full), 3, cur_iter_0);
     
             -- Сбрасываем нужные переменные
                           org_id = '';
                           org_id_lvl = '';
                           join_stmt = '';
--                         query = 'insert into dnf_org_tree'
                           query = 'insert into s_grnplm_vd_hr_edp_dia.dnf_org_tree_tmp'
                                  || E'\n\tselect'
                                  || E'\n\t\tl1.cal_date'
                                  || E'\n\t\t, coalesce( %1$s'
                                  || E'\n\t\t  ) as org_id'
                                  || E'\n\t\t, coalesce( %2$s'
                                  || E'\n\t\t  ) as org_id_lvl'
                                  || E'\n\t\t, l1.lvl_0_org_id %3$s'
                                  || E'\n\tfrom %4$s'
                                  || E'\n\twhere 1 = 1'
                                  || E'\n\t\tand date_trunc(''month'', l1.cal_date)::date = ''%5$s''::date;'
                                  ;
                                
                    end loop;
             end loop;
       close cur2;
           
                    -- Очищаем целевую таблицу
            step_id = 'insert into  s_grnplm_vd_hr_edp_stg.dnf_org_tree';
--                  truncate s_grnplm_vd_hr_edp_dds.dnf_org_tree;
            truncate s_grnplm_vd_hr_edp_stg.tb_org_tree_history_hor_cdm;
          
             delete from s_grnplm_vd_hr_edp_stg.dnf_org_tree
                    where extract('year' from cal_date) = y;
                  
--                  delete from s_grnplm_vd_hr_edp_stg.tb_org_tree_history_hor_cdm
--                  where extract('year' from cal_date) = y;
           
                    -- Вставляем уникальные значения из временной таблицы
                    insert into s_grnplm_vd_hr_edp_stg.dnf_org_tree
                    select distinct * from s_grnplm_vd_hr_edp_dia.dnf_org_tree_tmp;
          
            step_id = 'insert into  s_grnplm_vd_hr_edp_stg.tb_org_tree_history_hor_cdm';
          
            INSERT INTO s_grnplm_vd_hr_edp_stg.tb_org_tree_history_hor_cdm(
                load_date, cal_date, org_id, org_id_lvl
                , lvl_1_org_id, lvl_2_org_id, lvl_2_date_start, lvl_2_date_end
                , lvl_3_org_id, lvl_3_date_start, lvl_3_date_end
                , lvl_4_org_id, lvl_4_date_start, lvl_4_date_end
                , lvl_5_org_id, lvl_5_date_start, lvl_5_date_end
                , lvl_6_org_id, lvl_6_date_start, lvl_6_date_end
                , lvl_7_org_id, lvl_7_date_start, lvl_7_date_end
                , lvl_8_org_id, lvl_8_date_start, lvl_8_date_end
                , lvl_9_org_id, lvl_9_date_start, lvl_9_date_end
                , lvl_10_org_id, lvl_10_date_start, lvl_10_date_end
                , lvl_11_org_id, lvl_11_date_start, lvl_11_date_end
                , lvl_12_org_id, lvl_12_date_start, lvl_12_date_end
                , lvl_13_org_id, lvl_13_date_start, lvl_13_date_end
                , lvl_14_org_id, lvl_14_date_start, lvl_14_date_end
                , lvl_15_org_id, lvl_15_date_start, lvl_15_date_end
                , lvl_16_org_id, lvl_16_date_start, lvl_16_date_end
                , lvl_17_org_id, lvl_17_date_start, lvl_17_date_end)
            SELECT
                current_date, cal_date, t.org_id, t.org_id_lvl
                , lvl_0_org_id, lvl_1_org_id, lvl_1_date_start, lvl_1_date_end
                , lvl_2_org_id, lvl_2_date_start, lvl_2_date_end
                , lvl_3_org_id, lvl_3_date_start, lvl_3_date_end
                , lvl_4_org_id, lvl_4_date_start, lvl_4_date_end
                , lvl_5_org_id, lvl_5_date_start, lvl_5_date_end
                , lvl_6_org_id, lvl_6_date_start, lvl_6_date_end
                , lvl_7_org_id, lvl_7_date_start, lvl_7_date_end
                , lvl_8_org_id, lvl_8_date_start, lvl_8_date_end
                , lvl_9_org_id, lvl_9_date_start, lvl_9_date_end
                , lvl_10_org_id, lvl_10_date_start, lvl_10_date_end
                , lvl_11_org_id, lvl_11_date_start, lvl_11_date_end
                , lvl_12_org_id, lvl_12_date_start, lvl_12_date_end
                , lvl_13_org_id, lvl_13_date_start, lvl_13_date_end
                , lvl_14_org_id, lvl_14_date_start, lvl_14_date_end
                , lvl_15_org_id, lvl_15_date_start, lvl_15_date_end
                , lvl_16_org_id, lvl_16_date_start, lvl_16_date_end
            FROM s_grnplm_vd_hr_edp_stg.dnf_org_tree t;
                  
                    -- логирование воркфлоу GPDDS_REBUILD_MOVEMENT_DETAIL
                    perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_end(log_id, 's_grnplm_vd_hr_edp_stg.tb_org_tree_history_hor_cdm','cal_date','cal_date',null); --Log
                    return 'OK';
           
     exception when OTHERS then
            get stacked diagnostics m_txt = MESSAGE_TEXT;
            get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
            get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
            get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

            perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt,e_detail,e_hint,e_context) ; --Log
            return format('Error : %s %s', m_txt, step_id);
    end;
end;

$body$
EXECUTE ON ANY;
	

COMMENT ON FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_rebuild_org_structure_year(integer) IS 'Пересобирает орг. структуру за указанный год в dnf_org_tree';

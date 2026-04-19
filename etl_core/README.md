# etl_core — Ядро ETL-системы

Папка содержит центральные процедуры и движки ETL-системы GP_ETL. Здесь сосредоточена вся универсальная логика загрузки данных: super workflow engine (SWF), обобщённые загрузчики между слоями (SMD→DIA, DIA→STG, STG→CDM), управление партициями, анализ и мягкая очистка таблиц. Компоненты этой папки являются фундаментом, на котором строятся конкретные ETL-пайплайны из `srv_wf/`.

## Ключевые файлы

| Файл | Описание |
|------|----------|
| `pr_swf_start.sql` | Запуск super workflow — точка входа для старта группы ETL-задач |
| `pr_swf_start_all.sql` | Массовый запуск всех воркфлоу в группе |
| `pr_swf_start_ctl.sql` | Запуск воркфлоу с контролем выполнения и обработкой ошибок |
| `pr_swf_get_next.sql` | Получение следующей задачи из очереди SWF-движка |
| `pr_swf_get_status.sql` | Получение текущего статуса воркфлоу или отдельной задачи |
| `pr_swf_log_action.sql` | Логирование действия в рамках воркфлоу |
| `pr_swf_log_activity.sql` | Детальное логирование активности SWF-движка |
| `pr_swf_log_clean.sql` | Очистка устаревших записей в логах SWF |
| `pr_swf_skew.sql` | Анализ и логирование data skew для таблиц в рамках воркфлоу |
| `pr_swf_wf_group.sql` | Управление группами воркфлоу (создание, изменение) |
| `pr_swf_wf_group_replace.sql` | Замена/обновление группы воркфлоу |
| `pr_super_workflow_tmt.sql` | Управление таймаутами super workflow |
| `pr_any2stg_v1.sql` | Универсальный загрузчик данных в слой STG (версия 1) |
| `pr_dia2stg_full.sql` | Полная перезагрузка из слоя DIA в STG |
| `pr_diatostg_v2.sql` | Инкрементальная загрузка DIA→STG (версия 2) |
| `pr_smd2stg_full.sql` | Полная перезагрузка из SMD в STG |
| `pr_smd2stg_add.sql` | Инкрементальное добавление данных из SMD в STG |
| `pr_smd2stg_servmgr.sql` | Загрузка данных ServiceManager из SMD в STG |
| `pr_smdtodia_v2.sql` | Инкрементальная загрузка SMD→DIA (версия 2) |
| `pr_smdtodia_v3.sql` | Инкрементальная загрузка SMD→DIA (версия 3) |
| `pr_smdtodia_v4.sql` | Инкрементальная загрузка SMD→DIA (версия 4, актуальная) |
| `pr_stg2hist.sql` | Загрузка данных из STG в историческую таблицу (SCD2) |
| `pr_stg2hist_infotype_9102.sql` | Специализированная загрузка в историю для infotype 9102 |
| `pr_update_v2.sql` | Универсальное обновление CDM-таблиц (версия 2) |
| `pr_etl_get_config.sql` | Получение конфигурации ETL-процесса из tb_etl_config |
| `pr_etl_source_to_target.sql` | Обобщённое копирование данных из источника в приёмник |
| `pr_etl_dia_to_stg.sql` | Специализированный загрузчик DIA→STG |
| `pr_etl_truncate_dia.sql` | Очистка таблиц слоя DIA |
| `pr_etl_truncate_source.sql` | Очистка исходных таблиц перед загрузкой |
| `pr_add_partitions.sql` | Создание новых партиций в партиционированных таблицах |
| `pr_analyze.sql` | Запуск ANALYZE для обновления статистики таблицы |
| `pr_soft_truncate.sql` | "Мягкая" очистка таблицы без удаления партиций |
| `pr_rebuild_org_structure.sql` | Пересборка организационной структуры |
| `pr_rebuild_org_structure_year.sql` | Пересборка оргструктуры в разрезе года |
| `pr_rebuild_movement_detail.sql` | Пересборка детализации кадровых перемещений |
| `pr_refresh_vw_m_actions.sql` | Обновление материализованного представления действий |
| `pr_scpl_json_parse.sql` | Парсинг JSON в рамках SCPL-процессов |
| `pr_scpl_schemas_parse.sql` | Парсинг схем в рамках SCPL-процессов |
| `pr_std_sql.sql` | Выполнение стандартного SQL-блока с логированием |
| `pr_try_exe.sql` | Безопасное выполнение SQL с перехватом исключений |
| `pr_dia2hist.sql` | Загрузка данных из DIA в историческую таблицу |

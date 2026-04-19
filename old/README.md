# old — Архив устаревших компонентов

> **Внимание: это архивная папка. Файлы здесь не используются в production и не должны модифицироваться или запускаться без явного согласования.**

Папка содержит устаревшие версии функций и процедур, которые были заменены актуальными реализациями в других папках проекта. Компоненты перенесены сюда для сохранения истории и возможности сравнения с новыми версиями. Ряд файлов является экспериментальными или отладочными заготовками, которые не были введены в эксплуатацию.

## Состав архива

| Файл | Статус | Заменён на |
|------|--------|-----------|
| `fn_dq_calc_control_pre_dia2stg.sql` | Устарел | Логика перенесена в `dq/` и `etl_core/` |
| `fn_dq_calc_control_post_dia2stg.sql` | Устарел | Логика перенесена в `dq/` и `etl_core/` |
| `fn_dq_calc_date_not_in_range.sql` | Устарел | `dq/fn_dq_calc_not_in_range.sql` |
| `fn_dq_calc_dm.sql` | Устарел | Заменён улучшенной версией в `dq/` |
| `fn_dq_calc_dm2.sql` | Устарел | Заменён улучшенной версией в `dq/` |
| `fn_dq_calc_group_aggregate.sql` | Устарел | `dq/fn_dq_calc_aggregate.sql` |
| `fn_dq_calc_scd2_validity.sql` | Устарел | Логика перенесена в `dq/` |
| `fn_dq_count_or_double_pk.sql` | Устарел | Заменён проверкой в `log/pr_chk_uniq.sql` |
| `fn_dq_delta_perc_prev.sql` | Устарел | `dq/fn_dq_delta_perc_prev.sql` |
| `fn_dq_ztest.sql` | Устарел | `dq/fn_dq_ztest_v2.sql` |
| `pr_diatostg_employment_record_sm_old.sql` | Архив | `srv_wf/pr_diatostg_employment_record_sm.sql` |
| `pr_smdtodia_employment_record_sm_old.sql` | Архив | `srv_wf/pr_smdtodia_employment_record_sm.sql` |
| `pr_smd2stg_txn_det_union_old.sql` | Архив | `srv_wf/pr_smd2stg_txn_det_union.sql` |
| `pr_update_contractor_movement_cdm_old.sql` | Архив | `srv_wf/pr_update_contractor_movement_cdm.sql` |
| `pr_swfstart.sql` | Устарел | `etl_core/pr_swf_start.sql` |
| `pr_swf_wf_group_test.sql` | Тестовый | Не используется |
| `pr_dummy.sql` | Заглушка | Не используется |
| `pr_test_func.sql` | Отладочный | Не используется |
| `pr_test.sql` | Отладочный | Не используется |
| `pr_try_etl.sql` | Прототип | Не используется |
| `pr_try_ppl.sql` | Прототип | Не используется |
| `pr_try_vd.sql` | Прототип | Не используется |
| `tb_log_fld_stat_tmp.sql` | Устарел | `tables/tb_log_fld_stat.sql` |
| `tb_ztest_config_old.sql` | Устарел | `tables/tb_ztest_config.sql` |

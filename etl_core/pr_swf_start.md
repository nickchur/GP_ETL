# pr_swf_start

```sql
pr_swf_start(swf text, wf text) RETURNS text  -- JSON-строка
```

Запуск одного **sub-workflow** `wf` в рамках **super-workflow** `swf`. Проверяет
предусловия, вызывает функцию-исполнитель воркфлоу (`wf_exec`), нормализует
результат в JSON и фиксирует его в `tb_swf` и логе действий. Всегда возвращает
JSON-строку с полем `reselt` (код результата).

## Параметры

| Параметр | Тип | Назначение |
|----------|-----|------------|
| `swf` | `text` | Имя super-workflow (группы), в контексте которого идёт запуск |
| `wf` | `text` | Имя запускаемого sub-workflow (значение `vw_swf.wf_name`) |

## Коды результата (`reselt` в JSON)

| Код | Значение |
|-----|----------|
| `>0` / `1` | Успех (`Ok` или `reselt` из JSON, вернувшегося из `wf_exec`) |
| `-1` | Нет данных (ответ `wf_exec` начинается с `no`) |
| `-2` | `query_canceled` / `statement_timeout` |
| `-3` | Непойманная ошибка (блок `OTHERS`) |
| `-4` | Прочий/ошибочный ответ `wf_exec` (не `Ok`/`No`/deadlock) |
| `-5` | Deadlock detected |
| `-8` | Просрочено: с `wf_next` прошло больше `wf_expire` |
| `-9` | Пропуск: зависимости не в статусе OK (`rel_ok != true`) |
| `-10` | TODO-статус воркфлоу не `true` |
| `-11` | Воркфлоу не найден в `vw_swf` (`wf_id is null`) |
| `-12` | Воркфлоу уже запущен (активная сессия по `application_name`) |

## Алгоритм

1. `application_name = wf`; `search_path` на схему SWF; лог действия `start`.
2. Пауза `pg_sleep(10)`, затем **защита от повторного запуска**: поиск активных сессий в `pg_stat_activity` по `application_name = wf` (или `wf/%`). При срабатывании — `reselt = -12`, лог `error`, возврат.
3. **Чтение метаданных воркфлоу:** `select * from vw_swf where wf_name = wf`. Предусловия (при нарушении — лог и ранний возврат / пропуск):

   | Условие | reselt | Действие лога | Ранний возврат |
   |---------|:------:|:-------------:|:--------------:|
   | `wf_id is null` | `-11` | `error` | да |
   | `todo is not true` | `-10` | `todo` | да |
   | `rel_ok is not true` | `-9` | `skip` | нет |
   | `now() - wf_next > wf_expire` | `-8` | `late` | нет |

4. Иначе — **вызов исполнителя:** `execute 'select <wf_exec>' into ret`. Разбор ответа:
   - если `ret` — это JSON с полем `reselt` → берём это значение;
   - иначе по префиксу текста: `ok ` → `1`; `no ` → `-1`; `dedlock detected` → `-5`; прочее → `-4`.

   Лог действия `end`.
5. **Фиксация:** `update tb_swf` (`wf_last`, `wf_duration`, `wf_reselt`, `wf_swf`), возврат JSON-строки `m_txt`.
6. **Исключения:**
   - `query_canceled` → `reselt = -2`, лог `cancel`, `update tb_swf`, возврат;
   - `OTHERS` → `pr_Log_error`, `reselt = -3`, лог `error`, `update tb_swf`, возврат.

> `EXECUTE ON ANY` — может выполняться на любом сегменте Greenplum.
> См. также [`pr_swf_wf_group`](pr_swf_wf_group.md) — оркестрация группы workflow.

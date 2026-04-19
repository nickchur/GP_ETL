CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys AS
 WITH log AS (
         SELECT DISTINCT ON (a_1.wf_name, a_1.id, a_1.wf_key) a_1.wf_name,
            a_1.id,
            a_1.wf_key,
            a_1.cnt,
            a_1.sum_len,
            a_1.min_len,
            a_1.max_len,
            a_1."time",
            a_1.ts,
            a_1.completed,
            a_1.wf_data,
            b_1.status_dttm,
            b_1.auto,
            b_1.alive,
            b_1.status,
            b_1.status_log,
            (((NULLIF(b_1.alive, 'ACTIVE'::text) = ANY (ARRAY['COMPLETED'::text, 'ABORTED'::text])) AND ((b_1.status <> 'SUCCESS'::text) OR (NOT a_1.completed))) OR ((NOT a_1.completed) AND (a_1.ts < (now() - '00:30:00'::interval)))) AS retry
           FROM (s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log a_1
             LEFT JOIN s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading b_1 ON ((b_1.id = a_1.id)))
          ORDER BY a_1.wf_name DESC, a_1.id DESC, a_1.wf_key DESC, a_1.ts DESC
        )
 SELECT DISTINCT ON (a.wf_name, a.wf_key) a.wf_name,
    a.wf_key,
    a.completed,
    a.retry,
    a.ts AS last_ts,
    a.id AS last_id,
    b.count AS ids_cnt,
    b.ids,
    a.cnt,
    a.sum_len,
    a."time",
    a.status_dttm,
    (a.auto)::boolean AS auto,
    a.alive,
    a.status,
    a.status_log
   FROM (log a
     JOIN ( SELECT log.wf_name,
            log.wf_key,
            count(1) AS count,
            json_agg(log.id ORDER BY log.id DESC) AS ids
           FROM log
          GROUP BY log.wf_name, log.wf_key) b ON (((a.wf_name = b.wf_name) AND (a.wf_key = b.wf_key))))
  WHERE (a.completed OR (a.retry IS NOT NULL))
  ORDER BY a.wf_name, a.wf_key DESC, a.id DESC;

comment on view s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys is '
Сводка по ключам обмена (например, по датам).
Показывает историю попыток и признак необходимости повтора.
';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.wf_name is 'Имя потока';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.wf_key is 'Ключ (дата/ID)';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.completed is 'Успешно передано';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.retry is 'Нужен повтор';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.last_ts is 'Время последней попытки';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.last_id is 'ID последней попытки';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.ids_cnt is 'Число попыток';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.ids is 'Список ID попыток';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.cnt is 'Число строк';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.sum_len is 'Объём данных';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.time is 'Длительность';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.status_dttm is 'Время статуса';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.auto is 'Автоматически';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.alive is 'Текущий статус';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.status is 'Результат';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys.status_log is 'Лог процесса';
COMMENT ON VIEW s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys IS 'Лог обменов с ключами инкремента и статусами по каждой записи';

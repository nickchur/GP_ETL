CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log AS
 SELECT a.wf_name,
    a.id,
    a.wf_key,
    min(a.cnt) AS cnt,
    min(a.sum_len) AS sum_len,
    min(a.min_len) AS min_len,
    min(a.max_len) AS max_len,
    sum(a."time") AS "time",
    min(a.ts) AS ts,
    ((((((count(1) = 2) AND (max(a.cnt) = min(a.cnt))) AND (NOT (max(a.sum_len) IS DISTINCT FROM min(a.sum_len)))) AND (NOT (max(a.min_len) IS DISTINCT FROM min(a.min_len)))) AND (NOT (max(a.max_len) IS DISTINCT FROM min(a.max_len)))) AND (NOT (max(a.type) IS DISTINCT FROM min(a.type)))) AS completed,
    (concat((('{"ts":"'::text || max(a.ts)) || '"'::text), ((',"type":"'::text || min(a.type)) || '"'::text),
        CASE
            WHEN (count(1) <> 2) THEN (',"count":'::text || count(1))
            ELSE ''::text
        END,
        CASE
            WHEN (max(a.cnt) <> min(a.cnt)) THEN (',"cnt":'::text || max(a.cnt))
            ELSE ''::text
        END,
        CASE
            WHEN (max(a.sum_len) IS DISTINCT FROM min(a.sum_len)) THEN (',"sum_len":'::text || max(a.sum_len))
            ELSE ''::text
        END,
        CASE
            WHEN (max(a.min_len) IS DISTINCT FROM min(a.min_len)) THEN (',"min_len":'::text || max(a.min_len))
            ELSE ''::text
        END,
        CASE
            WHEN (max(a.max_len) IS DISTINCT FROM min(a.max_len)) THEN (',"max_len":'::text || max(a.max_len))
            ELSE ''::text
        END, '}'))::json AS wf_data
   FROM ( SELECT b.wf_name,
            b.id,
            b.wf_key,
            ((b.wf_data ->> 'cnt'::text))::bigint AS cnt,
            ((b.wf_data ->> 'sum_len'::text))::bigint AS sum_len,
            ((b.wf_data ->> 'min_len'::text))::bigint AS min_len,
            ((b.wf_data ->> 'max_len'::text))::bigint AS max_len,
            ((b.wf_data ->> 'time'::text))::interval AS "time",
            (b.wf_data ->> 'type'::text) AS type,
            b.ts
           FROM s_grnplm_vd_hr_edp_srv_wf.tb_exchange_log b) a
  GROUP BY a.wf_name, a.id, a.wf_key, a.type;

comment on view s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log is '
Журнал операций обмена данными между системами. 
Содержит информацию о статусе, времени начала и завершения, источнике и получателе данных, 
а также результатах загрузки (успех/ошибка). 
Используется для мониторинга, диагностики и аудита процессов интеграции.
';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log.wf_name is 'Имя потока выгрузки';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log.id is 'ID пакета обмена';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log.wf_key is 'Ключ инкремента (дата/ID)';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log.cnt is 'Число записей в пакете';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log.sum_len is 'Общий размер данных (символы)';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log.min_len is 'Мин. длина записи';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log.max_len is 'Макс. длина записи';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log.time is 'Время обработки пакета';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log.ts is 'Время первой записи';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log.completed is 'Флаг завершения обмена';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log.wf_data is 'Диагностика в формате JSON';
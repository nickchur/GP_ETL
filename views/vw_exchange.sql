CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_exchange AS
 WITH log AS (
         SELECT a.wf_name,
            max(a.wf_key) AS max
           FROM s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log a
          WHERE ((((a.wf_data ->> 'type'::text) = 'OUT'::text) AND a.completed) AND (a.wf_name = 'vw_ztest'::text))
          GROUP BY a.wf_name
        ), retry AS (
         SELECT a.wf_name,
            a.wf_key
           FROM s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys a
          WHERE (a.retry AND (a.wf_name = 'vw_ztest'::text))
          GROUP BY a.wf_name, a.wf_key
        )
 SELECT a.wf_name,
    a.wf_key,
    a.wf_data
   FROM ( SELECT NULL::text AS wf_name,
            NULL::text AS wf_key,
            NULL::text AS wf_data
         LIMIT 0) a
UNION ALL
 SELECT 'vw_ztest'::text AS wf_name,
    ((a.ts)::date)::text AS wf_key,
    (row_to_json(a.*))::text AS wf_data
   FROM ( SELECT a_1.ts,
            a_1.object,
            a_1.ztest_ok,
            a_1.is_except,
            a_1.is_error,
            a_1.confidence,
            a_1.stable,
            a_1.zscore,
            a_1.key_date,
            a_1.rows_count,
            a_1.key_diff,
            a_1.value,
            a_1.avg,
            a_1.std,
            a_1.cnt,
            a_1.min,
            a_1.max,
            a_1.log_id,
            a_1.notes,
            a_1.error,
            a_1.z_cfg,
            a_1.z_except,
            a_1.z_error
           FROM (s_grnplm_vd_hr_edp_srv_dq.vw_ztest a_1
             JOIN ( SELECT (a_2.ts)::date AS ts
                   FROM s_grnplm_vd_hr_edp_srv_dq.vw_ztest a_2
                  WHERE ((a_2.ts > COALESCE(( SELECT (b_1.max)::date AS max
                           FROM log b_1
                          WHERE (b_1.wf_name = 'vw_ztest'::text)
                         LIMIT 1), '1900-01-01'::date)) OR (a_2.ts > (('now'::text)::date - 1)))
                  GROUP BY (a_2.ts)::date
                UNION
                 SELECT (f.wf_key)::date AS wf_key
                   FROM retry f
                  WHERE (f.wf_name = 'vw_ztest'::text)
                  GROUP BY (f.wf_key)::date
          ORDER BY 1
         LIMIT 100) b ON (((a_1.ts)::date = b.ts)))
          WHERE true) a;

comment on view s_grnplm_vd_hr_edp_srv_wf.vw_exchange is 'Источник данных для обмена данными между системами. 
Содержит данные в формате JSON.';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange.wf_name is 'Имя потока выгрузки (источник)';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange.wf_key is 'Ключ инкремента (например, дата)';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_exchange.wf_data is 'Данные строки в формате JSON';
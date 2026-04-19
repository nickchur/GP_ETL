CREATE VIEW s_grnplm_vd_hr_edp_srv_dq.vw_dq_log AS
 SELECT a.id,
    a.ts,
    a.dq_action AS metric,
    (a.dq_message ->> 'tbl'::text) AS tbl,
    (a.dq_message ->> 'params'::text) AS params,
    ((b.ts - a.ts))::interval(0) AS duration,
    b.dq_action AS res_msg,
    b.dq_message AS msg
   FROM (s_grnplm_vd_hr_edp_srv_dq.tb_dq_log a
     JOIN s_grnplm_vd_hr_edp_srv_dq.tb_dq_log b ON ((a.id = b.parent)))
  ORDER BY a.id DESC;


COMMENT ON VIEW s_grnplm_vd_hr_edp_srv_dq.vw_dq_log IS 'Журнал выполнения DQ-проверок с метриками, результатами и деталями ошибок';

CREATE VIEW s_grnplm_vd_hr_edp_srv_dq.vw_sberchat_dq AS
 SELECT (date_trunc('month'::text, tb_sberchat.log_time))::date AS dt,
    count(1) AS cnt,
    count(DISTINCT (tb_sberchat.log_time)::date) AS cnt_dt,
    ((((count(DISTINCT "left"((tb_sberchat.log_time)::text, 15)))::numeric / (count(DISTINCT (tb_sberchat.log_time)::date))::numeric) / (24)::numeric) / (6)::numeric) AS dq_dttm,
    count(DISTINCT tb_sberchat.snd_contacts) AS cnt_snd,
    count(DISTINCT tb_sberchat.snd_i_pernr) AS cnt_snd_i_pernr,
    ((count(DISTINCT tb_sberchat.snd_i_pernr))::numeric / (count(DISTINCT tb_sberchat.snd_contacts))::numeric) AS dq_snd,
    (count(DISTINCT tb_sberchat.snd_contacts) - count(DISTINCT tb_sberchat.snd_i_pernr)) AS cnt_snd_null,
    count(DISTINCT tb_sberchat.rcv_contacts) AS cnt_rcv,
    count(DISTINCT tb_sberchat.rcv_i_pernr) AS cnt_rcv_i_pernr,
    ((count(DISTINCT tb_sberchat.rcv_i_pernr))::numeric / (count(DISTINCT tb_sberchat.rcv_contacts))::numeric) AS dq_rcv,
    (count(DISTINCT tb_sberchat.rcv_contacts) - count(DISTINCT tb_sberchat.rcv_i_pernr)) AS cnt_rcv_null
   FROM s_grnplm_vd_hr_edp_stg.tb_sberchat
  WHERE ((tb_sberchat.log_time >= '2025-01-01 00:00:00'::timestamp without time zone) AND (tb_sberchat.msg_type = '1'::text))
  GROUP BY (date_trunc('month'::text, tb_sberchat.log_time))::date
  ORDER BY (date_trunc('month'::text, tb_sberchat.log_time))::date DESC;


COMMENT ON VIEW s_grnplm_vd_hr_edp_srv_dq.vw_sberchat_dq IS 'Ежемесячная статистика объёма сообщений SberChat для DQ-мониторинга';

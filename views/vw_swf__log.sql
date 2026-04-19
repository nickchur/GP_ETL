CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_swf__log AS
 SELECT s_grnplm_vd_hr_edp_srv_wf.try_cast2timestamp(a.wf_action) AS beg_action,
    a.id AS beg_id,
    a.ts AS beg_ts,
    (a.wf_message ->> 'swf'::text) AS swf,
    (a.wf_message ->> 'wf'::text) AS wf,
    ((a.wf_message ->> 'td'::text))::integer AS td,
    ((a.wf_message ->> 'ready'::text))::timestamp without time zone AS ready,
    (a.ts - ((a.wf_message ->> 'ready'::text))::timestamp without time zone) AS wait,
    ( SELECT json_object_agg(json_each.key, json_each.value) AS json_object_agg
           FROM json_each(a.wf_message) json_each(key, value)
          WHERE (json_each.key <> ALL (ARRAY['wf'::text, 'swf'::text, 'ready'::text, 'td'::text]))) AS beg_msg,
    (b.ts - a.ts) AS duration,
    b.id AS end_id,
    b.ts AS end_ts,
    b.wf_action AS end_action,
    (b.wf_message ->> 'reselt'::text) AS reselt,
    (b.wf_message ->> 'msg'::text) AS msg,
    ( SELECT json_object_agg(json_each.key, json_each.value) AS json_object_agg
           FROM json_each(b.wf_message) json_each(key, value)
          WHERE (json_each.key <> ALL (ARRAY['wf'::text, 'swf'::text, 'reselt'::text, 'msg'::text]))) AS end_msg
   FROM (s_grnplm_vd_hr_edp_srv_wf.tb_swf__log a
     LEFT JOIN s_grnplm_vd_hr_edp_srv_wf.tb_swf__log b ON ((a.id = b.parent)))
  WHERE (a.parent IS NULL)
  ORDER BY s_grnplm_vd_hr_edp_srv_wf.try_cast2timestamp(a.wf_action) DESC, a.ts DESC;


CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_swf_status AS
 SELECT s.rn_td,
    s.swf_log,
    s.swf_name,
    s.last_beg,
    s.last_end,
    s.last_duration,
    s.last_message,
    s.check_end,
    s.chk,
    n.ready,
    n.wf_name AS next_name,
    n.rn AS next_rn,
    n.wf_next AS next_td
   FROM (( SELECT row_number() OVER (ORDER BY t.last_end, t.swf_name) AS rn_td,
            format('tb_swf_%s_log'::text, t.swf_name) AS swf_log,
            t.swf_name,
            t.last_beg,
            t.last_end,
            (t.last_end - t.last_beg) AS last_duration,
            t.last_message,
            l.check_end,
            (((t.last_end - COALESCE(l.check_end, t.last_end)) < '00:00:05'::interval) OR ((t.last_message ->> 'reselt'::text) = '0'::text)) AS chk
           FROM (s_grnplm_vd_hr_edp_srv_wf.tb_swf_status t
             LEFT JOIN ( SELECT DISTINCT ON (tb_swf.wf_swf) tb_swf.wf_swf AS swf,
                    (tb_swf.wf_last + tb_swf.wf_duration) AS check_end
                   FROM s_grnplm_vd_hr_edp_srv_wf.tb_swf
                  ORDER BY tb_swf.wf_swf, tb_swf.wf_last DESC) l ON ((l.swf = t.swf_name)))) s
     LEFT JOIN s_grnplm_vd_hr_edp_srv_wf.vw_swf n ON (((s.rn_td = n.rn_td) AND (n.todo = true))))
  ORDER BY s.rn_td;


CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_swf_7_log (
	id integer not null DEFAULT nextval('s_grnplm_vd_hr_edp_srv_wf.tb_swf_7_log_id_seq'::regclass),
	ts timestamp without time zone not null DEFAULT clock_timestamp(),
	parent integer null,
	wf_action text null,
	wf_message json null
)
WITH (appendonly=false)
DISTRIBUTED BY (id);
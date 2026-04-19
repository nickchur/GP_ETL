CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.log_table (
	load_event_cd text null,
	load_event_ts timestamp(6) without time zone not null,
	workflow_run_id integer not null,
	load_event_text text null,
	tgr_entity_cd text not null
)
DISTRIBUTED BY (load_event_ts);
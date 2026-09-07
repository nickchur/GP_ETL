CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_ctl_alerts (
	ts timestamp without time zone not null DEFAULT clock_timestamp(),
	wf_id bigint null,
	wf_name text null,
	alert_grp text null,
	alert_key text null,
	period_ts timestamp without time zone null,
	reacted_ts timestamp without time zone null,
	res integer null,
	msg text null,
	jsn json null
)
WITH (appendonly=false)
DISTRIBUTED BY (wf_id);

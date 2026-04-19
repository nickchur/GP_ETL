CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_log_work (
	ts timestamp without time zone null,
	app text null,
	log_id integer null,
	action text null,
	msg text null,
	res text null
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (log_id);
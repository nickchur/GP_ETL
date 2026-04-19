CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_log (
	id integer not null DEFAULT nextval('s_grnplm_vd_hr_edp_srv_dq.tb_dq_log_id_seq'::regclass),
	ts timestamp without time zone not null DEFAULT clock_timestamp(),
	parent integer null,
	dq_action text null,
	dq_message json null
)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (id);
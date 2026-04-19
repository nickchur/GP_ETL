CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_checks (
	metric text null,
	active boolean null,
	tbl text null,
	params json null
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=3)
DISTRIBUTED RANDOMLY;
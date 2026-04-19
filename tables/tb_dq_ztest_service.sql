CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_service (
	view_name text null,
	scheme_name text null,
	object_name text null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (view_name);
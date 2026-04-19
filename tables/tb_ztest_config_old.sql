CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config_old (
	object text null,
	active boolean null,
	error boolean null,
	z_from double precision null DEFAULT '-3.0'::numeric,
	z_to double precision null DEFAULT 3.0,
	z_except date[] null
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=3)
DISTRIBUTED RANDOMLY;
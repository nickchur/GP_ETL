CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.bench_date (
	max date null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED RANDOMLY;
CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.rc_r (
	insert bigint null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (insert);
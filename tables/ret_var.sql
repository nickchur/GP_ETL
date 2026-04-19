CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.ret_var (
	fn_dq_calc_max_date double precision null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (fn_dq_calc_max_date);
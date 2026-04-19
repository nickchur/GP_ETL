CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_help (
	date_field_value date null,
	result_count integer null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (date_field_value);
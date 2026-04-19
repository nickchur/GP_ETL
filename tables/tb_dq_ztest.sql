CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest (
	rule_name character varying(100) null,
	stg_table character varying(100) null,
	stg_column character varying(100) null,
	load_dt date null,
	zscore character varying null,
	calc_dt date null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (rule_name);
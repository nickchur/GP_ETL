CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_nonnull_ratio (
	rule_name character varying(100) null,
	stg_table character varying(100) null,
	stg_column character varying(100) null,
	load_dt date null,
	nonnull_ratio numeric(20,3) null,
	calc_dt date null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (rule_name);
CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_aggregate_comp (
	rule_name character varying(100) null,
	stg_table character varying(100) null,
	stg_column character varying(100) null,
	sql_expression character varying(2000) null,
	load_dt date null,
	agg_pct numeric(10,5) null,
	calc_dt timestamp without time zone null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (rule_name);
CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_scd2_validity (
	rule_name character varying(100) null,
	stg_table character varying(100) null,
	pk_field_name character varying(100) null,
	invalid_records character varying null,
	calc_dt date null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (rule_name);
CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_check_status_hist (
	rule_name character varying(255) null,
	rule_table_name character varying(255) null,
	rule_field_name character varying(255) null,
	rule_type_cd character varying(255) null,
	control_value numeric(10,3) null,
	actual_value numeric null,
	is_passed integer null,
	load_dt date null,
	effective_from date null,
	effective_to date null,
	init_start_dttm timestamp(0) without time zone null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED RANDOMLY;
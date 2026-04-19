CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_etl_config (
	workflow name not null,
	source_tuncate boolean null,
	source_table name null,
	source_fields text null,
	proc_type s_grnplm_vd_hr_edp_srv_wf.etl_type null,
	source_distinct boolean null,
	target_table name null,
	target_fields text null,
	exc_fields text null,
	source_sql text null,
	key_field name null,
	key_type name null,
	dt_field name null,
	dt_period name null,
	proc_owner name null,
	mod_dt timestamp without time zone null DEFAULT now(),
	truncate_after boolean null,
	load_field text null
)
WITH (appendonly=false)
DISTRIBUTED BY (workflow);
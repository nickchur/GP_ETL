CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_rule (
	rule_id integer null,
	rule_type_cd character varying(255) null,
	rule_table_name character varying(255) null,
	rule_field_name character varying(255) null,
	rule_date_field_name character varying(255) null,
	rule_sql text null,
	rule_lower_value numeric(10,3) null,
	rule_upper_value numeric(10,3) null,
	rule_interval integer null,
	rule_on boolean null,
	rule_creator character varying(255) null,
	rule_name character varying(255) null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED RANDOMLY;
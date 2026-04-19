CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.s2t_2 (
	entity character varying(100) null,
	attribute_name character varying(512) null,
	SCHEMA character varying(100) null,
	table_name character varying(100) null,
	column_name character varying(100) null,
	data_type character varying(100) null,
	COMMENT character varying(200) null,
	schema_source_table character varying(100) null,
	source_table character varying(100) null,
	source_column_name character varying(100) null,
	source_data_type character varying(100) null
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED RANDOMLY;
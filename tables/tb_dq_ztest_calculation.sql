CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_ztest_calculation (
	calc_dt timestamp without time zone null,
	object_name character varying(100) null,
	column_name character varying(100) null,
	date_field_name character varying(100) null,
	date_field_value character varying(50) null,
	field_value integer null,
	date_from date null,
	date_to date null,
	avg_value numeric null,
	stddev_value numeric null,
	zscore character varying null,
	confidence_persentage numeric null
)
WITH (appendonly=false)
DISTRIBUTED BY (object_name, date_field_name, date_field_value, field_value);
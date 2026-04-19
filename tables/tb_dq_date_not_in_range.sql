CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_dq_date_not_in_range (
	table_name character varying(100) not null,
	field_name character varying(100) not null,
	cnt_not_in_range integer not null,
	calc_ts timestamp without time zone not null DEFAULT now()
)
WITH (appendonly=true, compresstype=zstd, compresslevel=3)
DISTRIBUTED RANDOMLY;